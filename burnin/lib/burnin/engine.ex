defmodule Burnin.Engine do
  @moduledoc """
  Burn-in engine managing workers and run lifecycle.
  Orchestrates start/stop of pattern workers and duration timer.
  """

  use GenServer
  require Logger

  alias Burnin.{Config, Tracker, Report}

  defstruct broker: "localhost:50000",
            tracker: Tracker.new(),
            config: nil,
            workers: [],
            timer_ref: nil,
            final_statuses: nil

  def start_link(opts) do
    broker = Keyword.get(opts, :broker, "localhost:50000")
    GenServer.start_link(__MODULE__, broker, name: __MODULE__)
  end

  def start_run(config_json) do
    GenServer.call(__MODULE__, {:start_run, config_json}, 30_000)
  end

  def stop_run do
    GenServer.call(__MODULE__, :stop_run, 30_000)
  end

  def status do
    GenServer.call(__MODULE__, :status)
  end

  def run_config do
    GenServer.call(__MODULE__, :run_config)
  end

  def run_report do
    GenServer.call(__MODULE__, :run_report)
  end

  def cleanup do
    GenServer.call(__MODULE__, :cleanup, 30_000)
  end

  def broker_status do
    GenServer.call(__MODULE__, :broker_status, 10_000)
  end

  def info do
    GenServer.call(__MODULE__, :info)
  end

  @impl true
  def init(broker) do
    Process.flag(:trap_exit, true)
    {:ok, %__MODULE__{broker: broker}}
  end

  @impl true
  def handle_call({:start_run, json}, _from, %{tracker: %{state: s}} = state)
      when s in [:idle, :completed, :failed] do
    case Config.from_json(json) do
      {:ok, config} ->
        case Config.validate(config) do
          :ok -> do_start(config, state)
          {:error, errs} -> {:reply, {:error, Enum.join(errs, "; ")}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_run, _}, _from, state) do
    {:reply, {:error, "Cannot start: current state is #{state.tracker.state}"}, state}
  end

  def handle_call(:stop_run, _from, %{tracker: %{state: s}} = state)
      when s in [:running, :starting] do
    try do
      new_state = do_stop(state)
      {:reply, :ok, new_state}
    catch
      kind, reason ->
        Logger.error("do_stop crashed on stop_run: #{kind}: #{inspect(reason)}")
        tracker = Tracker.fail(state.tracker, "do_stop crash")
        {:reply, :ok, %{state | tracker: tracker, timer_ref: nil}}
    end
  end

  def handle_call(:stop_run, _from, state) do
    {:reply, {:error, "Not running"}, state}
  end

  def handle_call(:status, _from, state) do
    statuses = get_statuses(state)
    {:reply, Tracker.to_status_map(state.tracker, statuses), state}
  end

  def handle_call(:run_config, _from, state) do
    {:reply, state.config, state}
  end

  def handle_call(:run_report, _from, state) do
    statuses = get_statuses(state)
    elapsed = Tracker.elapsed_seconds(state.tracker)
    report = Report.generate_report(statuses, state.config, elapsed)
    {:reply, report, state}
  end

  def handle_call(:cleanup, _from, state) do
    Enum.each(state.workers, fn {pid, _} ->
      try do
        GenServer.call(pid, :cleanup, 10_000)
      catch
        _, _ -> :ok
      end
    end)

    stop_workers(state.workers)
    {:reply, :ok, %{state | workers: [], config: nil, tracker: Tracker.new()}}
  end

  def handle_call(:broker_status, _from, state) do
    result = ping_broker(state.broker)
    {:reply, result, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      sdk: Burnin.sdk(),
      version: Burnin.version(),
      runtime: "Elixir #{System.version()} / OTP #{System.otp_release()}",
      state: Atom.to_string(state.tracker.state)
    }

    {:reply, info, state}
  end

  @impl true
  def handle_info(:duration_elapsed, state) do
    Logger.info("Duration elapsed, stopping run")

    try do
      new_state = do_stop(state)
      {:noreply, new_state}
    catch
      kind, reason ->
        Logger.error("do_stop crashed: #{kind}: #{inspect(reason)}")
        tracker = Tracker.fail(state.tracker, "do_stop crash: #{inspect(reason)}")
        {:noreply, %{state | tracker: tracker, timer_ref: nil}}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.warning("Worker #{inspect(pid)} down: #{inspect(reason)}")
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    Logger.warning("Linked process #{inspect(pid)} exited: #{inspect(reason)}")
    {:noreply, state}
  end

  defp do_start(config, state) do
    broker = Config.effective_broker(config, state.broker)
    run_id = if config.run_id != "", do: config.run_id, else: "run-#{System.system_time(:millisecond)}"

    case ping_broker(broker) do
      {:ok, _} -> :ok
      {:error, reason} -> throw({:broker_error, reason})
    end

    # Start workers first (opens connections in init), then start the timer
    workers = start_workers(broker, config)
    tracker = state.tracker |> Tracker.start(run_id) |> Tracker.running()

    duration_ms = Config.parse_duration(config.duration)

    timer_ref =
      if duration_ms > 0 do
        Process.send_after(self(), :duration_elapsed, duration_ms)
      end

    Logger.info("Started burn-in run #{run_id} targeting #{broker}")

    new_state = %{
      state
      | tracker: tracker,
        config: config,
        workers: workers,
        timer_ref: timer_ref,
        final_statuses: nil
    }

    {:reply, {:ok, run_id}, new_state}
  catch
    {:broker_error, reason} ->
      {:reply, {:error, "Cannot connect to broker: #{reason}"}, state}
  end

  defp do_stop(state) do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    # Collect statuses BEFORE stopping workers — stop may block on gRPC close
    statuses = collect_worker_statuses(state.workers)
    stop_workers(state.workers)

    verdict = Report.generate_verdict(statuses, state.config)
    tracker = Tracker.complete(state.tracker, verdict)

    Logger.info("Run #{tracker.run_id} completed")
    %{state | tracker: tracker, timer_ref: nil, final_statuses: statuses}
  end

  defp start_workers(broker, config) do
    worker_modules = [
      {"events", Burnin.Worker.Events},
      {"events_store", Burnin.Worker.EventsStore},
      {"queue_stream", Burnin.Worker.QueueStream},
      {"queue_simple", Burnin.Worker.QueueSimple},
      {"commands", Burnin.Worker.Commands},
      {"queries", Burnin.Worker.Queries}
    ]

    for {name, module} <- worker_modules,
        Config.pattern_enabled?(config, name) do
      pattern_cfg = Config.pattern_config(config, name)

      {:ok, pid} =
        module.start_link(
          broker: broker,
          config: pattern_cfg,
          message_size: config.message.size_bytes,
          rpc_timeout: config.rpc.timeout_ms,
          queue_config: config.queue
        )

      Process.monitor(pid)
      {pid, name}
    end
  end

  defp stop_workers(workers) do
    tasks =
      Enum.map(workers, fn {pid, _} ->
        Task.async(fn ->
          if Process.alive?(pid) do
            try do
              GenServer.call(pid, :stop, 8_000)
            catch
              _, _ -> :ok
            end
          end
        end)
      end)

    Task.yield_many(tasks, 10_000)
    |> Enum.each(fn {task, result} ->
      if result == nil, do: Task.shutdown(task, :brutal_kill)
    end)
  end

  defp get_statuses(%{final_statuses: statuses}) when is_list(statuses), do: statuses
  defp get_statuses(%{workers: workers}), do: collect_worker_statuses(workers)

  defp collect_worker_statuses(workers) do
    tasks =
      Enum.map(workers, fn {pid, name} ->
        {Task.async(fn ->
           if Process.alive?(pid) do
             try do
               GenServer.call(pid, :status, 2_000)
             catch
               _, _ -> nil
             end
           end
         end), name}
      end)

    task_list = Enum.map(tasks, fn {task, _} -> task end)
    results = Task.yield_many(task_list, 3_000)

    Enum.map(results, fn {task, result} ->
      case result do
        {:ok, status} when is_map(status) -> status
        _ -> Task.shutdown(task, :brutal_kill); nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp ping_broker(address) do
    try do
      {:ok, client} =
        KubeMQ.Client.start_link(address: address, client_id: "burnin-ping")

      result = KubeMQ.Client.ping(client)
      KubeMQ.Client.close(client)

      case result do
        {:ok, info} -> {:ok, info}
        {:error, err} -> {:error, inspect(err)}
      end
    catch
      kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
    end
  end
end
