defmodule Burnin.Worker.Commands do
  @moduledoc """
  Commands pattern burn-in worker.
  Subscribes as a responder and sends commands at a configured rate.
  Uses multiple sender clients and spawned processes for concurrent sends.
  """

  use GenServer
  require Logger

  @sender_pool_size 10

  defstruct broker: "",
            config: %{},
            message_size: 256,
            rpc_timeout: 10_000,
            channels: [],
            senders: {},
            sender_count: 0,
            responder: nil,
            running: false,
            sent: 0,
            received: 0,
            errors: 0,
            send_index: 0,
            tasks: []

  @pattern_name "commands"

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    broker = Keyword.fetch!(opts, :broker)
    config = Keyword.fetch!(opts, :config)
    msg_size = Keyword.get(opts, :message_size, 256)
    rpc_timeout = Keyword.get(opts, :rpc_timeout, 10_000)
    channels = generate_channels(config.channels)

    # Open all connections synchronously during init so they're ready before the run timer starts
    senders_list =
      for i <- 1..@sender_pool_size do
        case KubeMQ.Client.start_link(address: broker, client_id: "burnin-commands-send-#{i}") do
          {:ok, pid} -> pid
          {:error, _} -> nil
        end
      end
      |> Enum.reject(&is_nil/1)

    senders_tuple = List.to_tuple(senders_list)

    {:ok, responder} = KubeMQ.Client.start_link(address: broker, client_id: "burnin-commands-resp")
    ensure_channels(hd(senders_list), channels)

    state = %__MODULE__{
      broker: broker,
      config: config,
      message_size: msg_size,
      rpc_timeout: rpc_timeout,
      channels: channels,
      senders: senders_tuple,
      sender_count: tuple_size(senders_tuple),
      responder: responder
    }

    send(self(), :begin_loops)
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply,
     %{
       name: @pattern_name,
       running: state.running,
       sent: state.sent,
       received: state.received,
       errors: state.errors,
       channels: state.channels
     }, state}
  end

  def handle_call(:stop, _from, state) do
    {:reply, :ok, do_stop(state)}
  end

  def handle_call(:cleanup, _from, state) do
    cleanup_channels(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:begin_loops, state) do
    tasks = start_loops([], state.responder, state)
    {:noreply, %{state | running: true, tasks: tasks}}
  end

  def handle_info({:send_tick, channel, batch}, state) do
    if state.running && state.sender_count > 0 do
      worker = self()
      msg_size = state.message_size
      rpc_timeout = state.rpc_timeout

      for i <- 0..(batch - 1) do
        sender = elem(state.senders, rem(state.send_index + i, state.sender_count))

        spawn(fn ->
          cmd = %KubeMQ.Command{
            channel: channel,
            metadata: "burnin",
            body: :crypto.strong_rand_bytes(msg_size),
            timeout: rpc_timeout
          }

          case KubeMQ.Client.send_command(sender, cmd) do
            {:ok, %{executed: true}} ->
              send(worker, {:cmd_ok, channel})

            _ ->
              send(worker, {:cmd_error, channel})
          end
        end)
      end

      schedule_send(channel, state.config.rate)
      {:noreply, %{state | send_index: state.send_index + batch}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:cmd_ok, channel}, state) do
    Burnin.Metrics.sent(@pattern_name, channel)
    Burnin.Metrics.received(@pattern_name, channel)
    {:noreply, %{state | sent: state.sent + 1, received: state.received + 1}}
  end

  def handle_info({:cmd_error, channel}, state) do
    Burnin.Metrics.error(@pattern_name, channel)
    {:noreply, %{state | errors: state.errors + 1}}
  end

  def handle_info({:received, channel}, state) do
    Burnin.Metrics.received(@pattern_name, channel)
    {:noreply, %{state | received: state.received + 1}}
  end

  def handle_info({ref, _}, state) when is_reference(ref), do: {:noreply, state}
  def handle_info({:DOWN, _, :process, _, _}, state), do: {:noreply, state}
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  defp start_loops(_senders, responder, state) do
    sub_tasks =
      Enum.map(state.channels, fn ch ->
        Task.async(fn -> responder_loop(responder, ch) end)
      end)

    Process.sleep(500)
    Enum.each(state.channels, fn ch -> schedule_send(ch, state.config.rate) end)
    sub_tasks
  end

  defp responder_loop(responder, channel) do
    on_command = fn cmd_receive ->
      %KubeMQ.CommandReply{
        response_to: cmd_receive.reply_channel,
        request_id: cmd_receive.id,
        client_id: "burnin-commands-resp",
        executed: true,
        metadata: "burnin-ack"
      }
    end

    case KubeMQ.Client.subscribe_to_commands(responder, channel, on_command: on_command, group: "burnin") do
      {:ok, _sub} -> Process.sleep(:infinity)
      {:error, reason} -> Logger.debug("Commands subscribe error: #{inspect(reason)}")
    end
  end

  defp ensure_channels(client, channels) do
    Enum.each(channels, fn ch ->
      try do
        KubeMQ.Client.create_channel(client, ch, :commands)
      catch
        _, _ -> :ok
      end
    end)
  end

  @min_tick_ms 10

  defp schedule_send(channel, rate) when rate > 0 do
    natural = div(1_000, rate)

    {batch, interval} =
      if natural < @min_tick_ms do
        b = div(rate * @min_tick_ms + 999, 1_000)
        {b, max(@min_tick_ms, div(b * 1_000, rate))}
      else
        {1, natural}
      end

    Process.send_after(self(), {:send_tick, channel, batch}, interval)
  end

  defp schedule_send(_, _), do: :ok

  defp do_stop(state) do
    Enum.each(state.tasks, &Task.shutdown(&1, :brutal_kill))

    if state.sender_count > 0 do
      for i <- 0..(state.sender_count - 1) do
        s = elem(state.senders, i)
        if Process.alive?(s), do: KubeMQ.Client.close(s)
      end
    end

    if state.responder && Process.alive?(state.responder), do: KubeMQ.Client.close(state.responder)

    %{state | running: false, senders: {}, sender_count: 0, responder: nil, tasks: []}
  end

  defp cleanup_channels(state) do
    client = if state.sender_count > 0, do: elem(state.senders, 0), else: nil

    if client && Process.alive?(client) do
      Enum.each(state.channels, fn ch ->
        try do
          KubeMQ.Client.delete_channel(client, ch, :commands)
        catch
          _, _ -> :ok
        end
      end)
    end
  end

  defp generate_channels(count) do
    Enum.map(1..count, &"burnin-commands-#{&1}")
  end
end
