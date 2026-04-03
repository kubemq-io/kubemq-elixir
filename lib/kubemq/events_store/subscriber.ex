defmodule KubeMQ.EventsStore.Subscriber do
  @moduledoc false

  use GenServer
  require Logger

  alias KubeMQ.{Error, EventStoreReceive, Validation}

  defstruct [
    :channel,
    :group,
    :client_id,
    :transport,
    :grpc_channel,
    :stream,
    :on_event,
    :on_error,
    :notify,
    :conn,
    :start_at,
    :last_sequence,
    :recv_pid,
    :task_supervisor
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    channel = Keyword.fetch!(opts, :channel)
    client_id = Keyword.fetch!(opts, :client_id)
    start_at = Keyword.fetch!(opts, :start_at)
    transport = Keyword.get(opts, :transport, KubeMQ.Transport.GRPC)
    grpc_channel = Keyword.fetch!(opts, :grpc_channel)
    conn = Keyword.get(opts, :conn)
    max_callback_concurrency = Keyword.get(opts, :max_callback_concurrency, 100)

    case Validation.validate_start_position(start_at) do
      :ok ->
        {:ok, task_sup} = Task.Supervisor.start_link(max_children: max_callback_concurrency)

        state = %__MODULE__{
          channel: channel,
          group: Keyword.get(opts, :group, ""),
          client_id: client_id,
          transport: transport,
          grpc_channel: grpc_channel,
          on_event: Keyword.get(opts, :on_event),
          on_error: Keyword.get(opts, :on_error),
          notify: Keyword.get(opts, :notify),
          conn: conn,
          start_at: start_at,
          last_sequence: 0,
          recv_pid: nil,
          task_supervisor: task_sup
        }

        {:ok, state, {:continue, :subscribe}}

      {:error, error} ->
        {:stop, error}
    end
  end

  @impl GenServer
  def handle_continue(:subscribe, state) do
    if state.recv_pid && Process.alive?(state.recv_pid) do
      Process.exit(state.recv_pid, :shutdown)
    end

    parent = self()
    recv_pid = spawn_link(fn -> subscribe_and_recv(state, parent) end)
    {:noreply, %{state | recv_pid: recv_pid}}
  end

  def handle_continue(:resubscribe, state) do
    if state.conn && Process.alive?(state.conn) do
      case KubeMQ.Connection.get_channel(state.conn) do
        {:ok, grpc_channel} ->
          resume_state =
            if state.last_sequence > 0 do
              %{
                state
                | grpc_channel: grpc_channel,
                  start_at: {:start_at_sequence, state.last_sequence + 1}
              }
            else
              %{state | grpc_channel: grpc_channel}
            end

          if state.recv_pid && Process.alive?(state.recv_pid) do
            Process.exit(state.recv_pid, :shutdown)
          end

          parent = self()
          recv_pid = spawn_link(fn -> subscribe_and_recv(resume_state, parent) end)
          {:noreply, %{resume_state | recv_pid: recv_pid}}

        {:error, _} ->
          Process.send_after(self(), :retry_resubscribe, 5_000)
          {:noreply, state}
      end
    else
      {:stop, :connection_closed, state}
    end
  end

  @impl GenServer
  def handle_info({:subscribed, _pid}, state) do
    {:noreply, state}
  end

  def handle_info({:subscription_failed, reason}, state) do
    dispatch_error(
      state,
      Error.transient("subscription failed: #{inspect(reason)}",
        operation: "subscribe_to_events_store",
        channel: state.channel
      )
    )

    Process.send_after(self(), :retry_resubscribe, 5_000)
    {:noreply, state}
  end

  def handle_info({:stream_event, proto_event}, state) do
    event = EventStoreReceive.from_proto(proto_event)
    new_seq = max(state.last_sequence, event.sequence)
    dispatch_event(state, event)
    {:noreply, %{state | last_sequence: new_seq}}
  end

  def handle_info({:stream_error, reason}, state) do
    Logger.warning(
      "[EventsStore.Subscriber] Stream error on #{state.channel}: #{inspect(reason)}"
    )

    dispatch_error(
      state,
      Error.stream_broken("subscription stream error: #{inspect(reason)}",
        operation: "subscribe_to_events_store",
        channel: state.channel
      )
    )

    {:noreply, state, {:continue, :resubscribe}}
  end

  def handle_info({:stream_closed, _reason}, state) do
    Logger.info("[EventsStore.Subscriber] Stream closed on #{state.channel}")
    {:noreply, %{state | recv_pid: nil}, {:continue, :resubscribe}}
  end

  def handle_info(:retry_resubscribe, state) do
    {:noreply, state, {:continue, :resubscribe}}
  end

  def handle_info({:EXIT, pid, reason}, state) when pid == state.recv_pid do
    Logger.warning("[EventsStore.Subscriber] recv_loop exited: #{inspect(reason)}")
    {:noreply, %{state | recv_pid: nil}, {:continue, :resubscribe}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.recv_pid && Process.alive?(state.recv_pid) do
      Process.exit(state.recv_pid, :shutdown)
    end

    :ok
  end

  # --- Private ---

  defp do_subscribe(state) do
    {type_data, type_value} = start_position_to_proto(state.start_at)

    subscribe_request = %{
      subscribe_type: :events_store,
      client_id: state.client_id,
      channel: state.channel,
      group: state.group || "",
      events_store_type_data: type_data,
      events_store_type_value: type_value
    }

    state.transport.subscribe(state.grpc_channel, subscribe_request)
  end

  @spec start_position_to_proto(term()) :: {atom(), integer()}
  defp start_position_to_proto(:start_new_only), do: {:StartNewOnly, 0}
  defp start_position_to_proto(:start_from_new), do: {:StartNewOnly, 0}
  defp start_position_to_proto(:start_from_first), do: {:StartFromFirst, 0}
  defp start_position_to_proto(:start_from_last), do: {:StartFromLast, 0}
  defp start_position_to_proto({:start_at_sequence, n}), do: {:StartAtSequence, n}
  defp start_position_to_proto({:start_at_time, t}), do: {:StartAtTime, t}
  defp start_position_to_proto({:start_at_time_delta, ms}), do: {:StartAtTimeDelta, div(ms, 1000)}
  defp start_position_to_proto(_), do: {:EventsStoreTypeUndefined, 0}

  defp subscribe_and_recv(state, parent) do
    case do_subscribe(state) do
      {:ok, stream} ->
        Kernel.send(parent, {:subscribed, self()})
        recv_loop(stream, parent)

      {:error, reason} ->
        Kernel.send(parent, {:subscription_failed, reason})
    end
  end

  defp recv_loop(stream, parent) do
    Enum.each(stream, fn
      {:ok, event} ->
        Kernel.send(parent, {:stream_event, event})

      {:error, reason} ->
        Kernel.send(parent, {:stream_error, reason})
    end)

    # Stream ended normally (server closed)
    Kernel.send(parent, {:stream_closed, :eof})
  rescue
    e ->
      Kernel.send(parent, {:stream_closed, {:exception, Exception.message(e)}})
  end

  defp dispatch_event(state, event) do
    cond do
      is_function(state.on_event, 1) ->
        Task.Supervisor.start_child(state.task_supervisor, fn ->
          try do
            state.on_event.(event)
          rescue
            e ->
              Logger.error(
                "[EventsStore.Subscriber] on_event callback error: #{Exception.message(e)}"
              )
          end
        end)

      is_pid(state.notify) ->
        Kernel.send(state.notify, {:kubemq_event_store, event})

      true ->
        :ok
    end
  end

  defp dispatch_error(state, error) do
    if is_function(state.on_error, 1) do
      Task.Supervisor.start_child(state.task_supervisor, fn ->
        try do
          state.on_error.(error)
        rescue
          e ->
            Logger.error(
              "[EventsStore.Subscriber] on_error callback error: #{Exception.message(e)}"
            )
        end
      end)
    end
  end
end
