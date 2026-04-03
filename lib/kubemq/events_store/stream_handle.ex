defmodule KubeMQ.EventStoreStreamHandle do
  @moduledoc """
  Handle for streaming persistent events with awaitable confirmation.

  Unlike `KubeMQ.EventStreamHandle`, each `send/2` call waits for server
  confirmation and returns `{:ok, %EventStoreResult{}}`.

  ## Usage

      {:ok, handle} = KubeMQ.Client.send_event_store_stream(client)
      {:ok, result} = KubeMQ.EventStoreStreamHandle.send(handle, event_store)
      :ok = KubeMQ.EventStoreStreamHandle.close(handle)
  """

  use GenServer
  require Logger

  alias KubeMQ.{Error, EventStore, EventStoreResult, UUID, Validation}

  @type t :: GenServer.server()

  @pending_sweep_interval 30_000

  defstruct [:stream, :client_id, :transport, :recv_pid, :pending]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec send(t(), EventStore.t()) :: {:ok, EventStoreResult.t()} | {:error, Error.t()}
  def send(handle, %EventStore{} = event) do
    GenServer.call(handle, {:send, event}, 30_000)
  end

  @spec close(t()) :: :ok
  def close(handle) do
    GenServer.stop(handle, :normal)
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    client_id = Keyword.fetch!(opts, :client_id)
    transport = Keyword.get(opts, :transport, KubeMQ.Transport.GRPC)

    # Support both direct stream (for tests) and grpc_channel (for production)
    stream_result =
      case Keyword.get(opts, :stream) do
        nil ->
          grpc_channel = Keyword.fetch!(opts, :grpc_channel)
          transport.send_event_stream(grpc_channel)

        stream ->
          {:ok, stream}
      end

    case stream_result do
      {:ok, stream} ->
        schedule_pending_sweep()

        {:ok,
         %__MODULE__{
           stream: stream,
           client_id: client_id,
           transport: transport,
           recv_pid: nil,
           pending: %{}
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:send, %EventStore{} = event}, from, state) do
    with :ok <- Validation.validate_channel(event.channel),
         :ok <- Validation.validate_content(event.metadata, event.body) do
      event_id = UUID.ensure(event.id)

      pb_event =
        %KubeMQ.Proto.Event{
          event_id: event_id,
          client_id: event.client_id || state.client_id,
          channel: event.channel,
          metadata: event.metadata || "",
          body: event.body || "",
          store: true,
          tags: event.tags || %{}
        }

      try do
        _stream = GRPC.Stub.send_request(state.stream, pb_event)
        pending = Map.put(state.pending, event_id, {from, System.monotonic_time(:millisecond)})

        # Start recv after first send so server responds with headers
        if state.recv_pid == nil do
          {:noreply, %{state | pending: pending}, {:continue, :start_recv}}
        else
          {:noreply, %{state | pending: pending}}
        end
      rescue
        e ->
          {:reply,
           {:error,
            Error.stream_broken("event store stream send failed: #{inspect(e)}",
              operation: "event_store_stream_send",
              channel: event.channel
            )}, state}
      end
    else
      {:error, _} = err -> {:reply, err, state}
    end
  end

  @impl GenServer
  def handle_continue(:start_recv, state) do
    case GRPC.Stub.recv(state.stream, timeout: :infinity) do
      {:ok, enum} when is_function(enum) ->
        parent = self()

        recv_pid =
          spawn_link(fn ->
            Enum.each(enum, fn
              {:ok, result} ->
                Kernel.send(parent, {:stream_result, {:ok, result}})

              {:error, reason} ->
                Kernel.send(parent, {:stream_closed, reason})
            end)

            Kernel.send(parent, {:stream_closed, :eof})
          end)

        {:noreply, %{state | recv_pid: recv_pid}}

      {:ok, _} ->
        {:noreply, state, {:continue, :start_recv}}

      {:error, reason} ->
        Logger.warning("[#{__MODULE__}] recv failed: #{inspect(reason)}")

        Enum.each(state.pending, fn
          {_id, {from, _inserted_at}} ->
            GenServer.reply(
              from,
              {:error,
               Error.stream_broken("recv failed: #{inspect(reason)}",
                 operation: "event_store_stream_send"
               )}
            )
        end)

        {:stop, {:shutdown, :recv_failed}, %{state | pending: %{}}}
    end
  end

  def handle_info({:stream_result, {:ok, result}}, state) do
    event_id = result.event_id || ""
    store_result = EventStoreResult.from_proto(result)

    case Map.pop(state.pending, event_id) do
      {nil, _pending} ->
        {:noreply, state}

      {{from, _inserted_at}, pending} ->
        if store_result.sent do
          GenServer.reply(from, {:ok, store_result})
        else
          GenServer.reply(
            from,
            {:error,
             Error.transient(store_result.error || "event not sent",
               operation: "event_store_stream_send"
             )}
          )
        end

        {:noreply, %{state | pending: pending}}
    end
  end

  def handle_info({:stream_closed, reason}, state) do
    Logger.warning("[#{__MODULE__}] Stream closed: #{inspect(reason)}")

    Enum.each(state.pending, fn
      {_id, {from, _inserted_at}} ->
        GenServer.reply(
          from,
          {:error,
           Error.stream_broken("stream closed",
             operation: "event_store_stream_send"
           )}
        )
    end)

    {:stop, {:shutdown, :stream_closed}, %{state | pending: %{}}}
  end

  def handle_info(:sweep_pending, state) do
    now = System.monotonic_time(:millisecond)
    timeout = 30_000

    {expired, remaining} =
      Map.split_with(state.pending, fn {_id, {_from, inserted_at}} ->
        now - inserted_at > timeout
      end)

    Enum.each(expired, fn {_id, {from, _inserted_at}} ->
      GenServer.reply(
        from,
        {:error,
         Error.timeout("operation timed out waiting for response",
           operation: "stream_operation"
         )}
      )
    end)

    schedule_pending_sweep()
    {:noreply, %{state | pending: remaining}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.stream do
      try do
        GRPC.Stub.end_stream(state.stream)
      rescue
        _ -> :ok
      end
    end

    if state.recv_pid && Process.alive?(state.recv_pid) do
      Process.exit(state.recv_pid, :shutdown)
    end

    Enum.each(state.pending || %{}, fn
      {_id, {from, _inserted_at}} ->
        GenServer.reply(
          from,
          {:error,
           Error.stream_broken("stream terminated",
             operation: "stream_send"
           )}
        )
    end)

    :ok
  end

  # --- Private ---

  defp schedule_pending_sweep do
    Process.send_after(self(), :sweep_pending, @pending_sweep_interval)
  end
end
