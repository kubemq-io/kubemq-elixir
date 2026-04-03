defmodule KubeMQ.QueueUpstreamHandle do
  @moduledoc """
  Handle for sending queue messages via a bidirectional upstream stream.

  Obtained via `KubeMQ.Client.queue_upstream/1`. Each `send/2` call writes
  a batch of messages and awaits the server response.

  ## Usage

      {:ok, handle} = KubeMQ.Client.queue_upstream(client)
      {:ok, results} = KubeMQ.QueueUpstreamHandle.send(handle, [msg1, msg2])
      :ok = KubeMQ.QueueUpstreamHandle.close(handle)
  """

  use GenServer
  require Logger

  alias KubeMQ.{Error, QueueMessage, QueueSendResult, UUID}

  @type t :: GenServer.server()

  @pending_sweep_interval 30_000

  defstruct [
    :stream,
    :client_id,
    :transport,
    :recv_pid,
    :pending,
    headers_received: false,
    buffer: <<>>
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec send(t(), [QueueMessage.t()]) ::
          {:ok, [QueueSendResult.t()]} | {:error, Error.t()}
  def send(handle, messages) when is_list(messages) do
    GenServer.call(handle, {:send, messages}, 30_000)
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
          transport.queue_upstream(grpc_channel)

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
           pending: %{},
           headers_received: false,
           buffer: <<>>
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:send, messages}, from, state) do
    ref_request_id = UUID.generate()

    pb_messages =
      Enum.map(messages, fn msg ->
        %KubeMQ.Proto.QueueMessage{
          message_id: UUID.ensure(msg.id),
          client_id: msg.client_id || state.client_id,
          channel: msg.channel || "",
          metadata: msg.metadata || "",
          body: msg.body || "",
          tags: msg.tags || %{},
          policy: build_policy_proto(msg.policy)
        }
      end)

    pb_request =
      %KubeMQ.Proto.QueuesUpstreamRequest{
        request_id: ref_request_id,
        messages: pb_messages
      }

    try do
      _stream = GRPC.Stub.send_request(state.stream, pb_request)

      pending =
        Map.put(state.pending, ref_request_id, {from, System.monotonic_time(:millisecond)})

      {:noreply, %{state | pending: pending}}
    rescue
      e ->
        {:reply,
         {:error,
          Error.stream_broken("upstream send failed: #{inspect(e)}",
            operation: "queue_upstream_send"
          )}, state}
    end
  end

  # Handle stream_result messages (from recv_loop in test mode, or forwarded)
  @impl GenServer
  def handle_info({:stream_result, {:ok, response}}, state) do
    new_state = handle_decoded_response(response, state)
    {:noreply, new_state}
  end

  def handle_info({:stream_closed, reason}, state) do
    Logger.warning("[#{__MODULE__}] Stream closed: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error, Error.stream_broken("upstream stream closed", operation: "queue_upstream_send")}
    )

    {:stop, {:shutdown, :stream_closed}, %{state | pending: %{}}}
  end

  # Handle gun response headers
  def handle_info({:gun_response, _conn_pid, _stream_ref, :nofin, 200, _headers}, state) do
    {:noreply, %{state | headers_received: true}}
  end

  def handle_info({:gun_response, _conn_pid, _stream_ref, :fin, _status, _headers}, state) do
    reply_all_pending(
      state,
      {:error, Error.stream_broken("stream closed by server", operation: "queue_upstream_send")}
    )

    {:stop, {:shutdown, :stream_closed}, %{state | pending: %{}}}
  end

  # Handle gun data
  def handle_info({:gun_data, _conn_pid, _stream_ref, is_fin, data}, state) do
    buffer = state.buffer <> data

    {messages, remaining} =
      decode_grpc_messages(buffer, state.stream.response_mod, state.stream.codec)

    state = %{state | buffer: remaining}

    state =
      Enum.reduce(messages, state, fn response, acc ->
        handle_decoded_response(response, acc)
      end)

    if is_fin == :fin do
      reply_all_pending(
        state,
        {:error, Error.stream_broken("stream ended", operation: "queue_upstream_send")}
      )

      {:stop, {:shutdown, :stream_ended}, %{state | pending: %{}}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:gun_trailers, _conn_pid, _stream_ref, _trailers}, state) do
    {:noreply, state}
  end

  def handle_info({:gun_error, _conn_pid, _stream_ref, reason}, state) do
    Logger.warning("[#{__MODULE__}] Gun stream error: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error,
       Error.stream_broken("stream error: #{inspect(reason)}", operation: "queue_upstream_send")}
    )

    {:stop, {:shutdown, :stream_error}, %{state | pending: %{}}}
  end

  def handle_info({:gun_error, _conn_pid, reason}, state) do
    Logger.warning("[#{__MODULE__}] Gun connection error: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error,
       Error.stream_broken("connection error: #{inspect(reason)}",
         operation: "queue_upstream_send"
       )}
    )

    {:stop, {:shutdown, :connection_error}, %{state | pending: %{}}}
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

  defp handle_decoded_response(response, state) do
    ref_id = response.ref_request_id || ""

    results =
      (response.results || [])
      |> Enum.map(fn r ->
        %QueueSendResult{
          message_id: r.message_id || "",
          sent_at: r.sent_at || 0,
          expiration_at: r.expiration_at || 0,
          delayed_to: r.delayed_to || 0,
          is_error: r.is_error || false,
          error: if(r.error in [nil, ""], do: nil, else: r.error)
        }
      end)

    case Map.pop(state.pending, ref_id) do
      {nil, _pending} ->
        state

      {{from, _inserted_at}, pending} ->
        GenServer.reply(from, {:ok, results})
        %{state | pending: pending}
    end
  end

  defp reply_all_pending(state, reply) do
    Enum.each(state.pending, fn
      {_id, {from, _inserted_at}} ->
        GenServer.reply(from, reply)
    end)
  end

  defp decode_grpc_messages(buffer, response_mod, codec) do
    decode_grpc_messages(buffer, response_mod, codec, [])
  end

  defp decode_grpc_messages(buffer, response_mod, codec, acc) when byte_size(buffer) >= 5 do
    <<_compressed::8, length::32, rest::binary>> = buffer

    if byte_size(rest) >= length do
      <<message_data::binary-size(length), remaining::binary>> = rest
      decoded = codec.decode(message_data, response_mod)
      decode_grpc_messages(remaining, response_mod, codec, [decoded | acc])
    else
      {Enum.reverse(acc), buffer}
    end
  end

  defp decode_grpc_messages(buffer, _response_mod, _codec, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp schedule_pending_sweep do
    Process.send_after(self(), :sweep_pending, @pending_sweep_interval)
  end

  defp build_policy_proto(nil), do: nil

  defp build_policy_proto(%KubeMQ.QueuePolicy{} = policy) do
    %KubeMQ.Proto.QueueMessagePolicy{
      expiration_seconds: policy.expiration_seconds,
      delay_seconds: policy.delay_seconds,
      max_receive_count: policy.max_receive_count,
      max_receive_queue: policy.max_receive_queue
    }
  end

  defp build_policy_proto(_), do: nil
end
