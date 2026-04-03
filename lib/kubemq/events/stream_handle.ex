defmodule KubeMQ.EventStreamHandle do
  @moduledoc """
  Handle for streaming events via a bidirectional gRPC stream.

  Obtained via `KubeMQ.Client.send_event_stream/1`. The GenServer serializes
  writes to the underlying stream (mutex equivalent via message queue).

  ## Usage

      {:ok, handle} = KubeMQ.Client.send_event_stream(client)
      :ok = KubeMQ.EventStreamHandle.send(handle, event)
      :ok = KubeMQ.EventStreamHandle.close(handle)
  """

  use GenServer
  require Logger

  @type t :: GenServer.server()

  defstruct [:stream, :client_id, :transport, :recv_pid]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Send an event through the streaming handle.

  The event is written to the underlying bidirectional gRPC stream.
  Returns `:ok` on success or `{:error, %KubeMQ.Error{}}` on failure.
  """
  @spec send(t(), KubeMQ.Event.t()) :: :ok | {:error, KubeMQ.Error.t()}
  def send(handle, %KubeMQ.Event{} = event) do
    GenServer.call(handle, {:send, event})
  end

  @doc """
  Close the event stream handle, terminating the underlying gRPC stream.
  """
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
        {:ok,
         %__MODULE__{
           stream: stream,
           client_id: client_id,
           transport: transport,
           recv_pid: nil
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:send, %KubeMQ.Event{} = event}, _from, state) do
    with :ok <- KubeMQ.Validation.validate_channel(event.channel),
         :ok <- KubeMQ.Validation.validate_content(event.metadata, event.body) do
      pb_event =
        %KubeMQ.Proto.Event{
          event_id: KubeMQ.UUID.ensure(event.id),
          client_id: event.client_id || state.client_id,
          channel: event.channel,
          metadata: event.metadata || "",
          body: event.body || "",
          store: false,
          tags: event.tags || %{}
        }

      try do
        _stream = GRPC.Stub.send_request(state.stream, pb_event)

        # Start recv after first send so server responds with headers
        if state.recv_pid == nil do
          {:reply, :ok, state, {:continue, :start_recv}}
        else
          {:reply, :ok, state}
        end
      rescue
        e ->
          {:reply,
           {:error,
            KubeMQ.Error.stream_broken("event stream send failed: #{inspect(e)}",
              operation: "event_stream_send",
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
        {:stop, {:shutdown, :recv_failed}, state}
    end
  end

  def handle_info({:stream_result, {:ok, _result}}, state) do
    {:noreply, state}
  end

  def handle_info({:stream_result, {:error, reason}}, state) do
    Logger.warning("[EventStreamHandle] Stream recv error: #{inspect(reason)}")
    {:noreply, state}
  end

  def handle_info({:stream_closed, reason}, state) do
    Logger.warning("[#{__MODULE__}] Stream closed: #{inspect(reason)}")
    {:stop, {:shutdown, :stream_closed}, state}
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

    :ok
  end
end
