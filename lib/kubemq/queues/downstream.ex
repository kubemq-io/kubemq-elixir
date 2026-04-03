defmodule KubeMQ.Queues.Downstream do
  @moduledoc false

  use GenServer
  require Logger

  alias KubeMQ.{
    DownstreamRequestType,
    Error,
    PollResponse,
    QueueMessage,
    UUID,
    Validation
  }

  @pending_sweep_interval 30_000
  @transaction_timeout 300_000

  defstruct [
    :stream,
    :client_id,
    :transport,
    :recv_pid,
    :pending,
    :transactions,
    headers_received: false,
    buffer: <<>>
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec poll(GenServer.server(), keyword()) ::
          {:ok, PollResponse.t()} | {:error, Error.t()}
  def poll(downstream, opts) do
    channel = Keyword.fetch!(opts, :channel)
    max_items = Keyword.get(opts, :max_items, 1)
    wait_timeout = Keyword.get(opts, :wait_timeout, 5_000)
    auto_ack = Keyword.get(opts, :auto_ack, false)

    with :ok <- Validation.validate_channel(channel),
         :ok <- Validation.validate_max_messages(max_items),
         :ok <- Validation.validate_wait_timeout(wait_timeout) do
      call_timeout = wait_timeout + 10_000

      GenServer.call(
        downstream,
        {:poll, channel, max_items, wait_timeout, auto_ack},
        call_timeout
      )
    end
  end

  @spec close(GenServer.server()) :: :ok
  def close(downstream) do
    GenServer.stop(downstream, :normal)
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
          transport.queue_downstream(grpc_channel)

        stream ->
          {:ok, stream}
      end

    case stream_result do
      {:ok, stream} ->
        schedule_pending_sweep()
        schedule_transaction_timeout()

        {:ok,
         %__MODULE__{
           stream: stream,
           client_id: client_id,
           transport: transport,
           pending: %{},
           transactions: %{},
           headers_received: false,
           buffer: <<>>
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:poll, channel, max_items, wait_timeout, auto_ack}, from, state) do
    request_id = UUID.generate()

    pb_request =
      %KubeMQ.Proto.QueuesDownstreamRequest{
        request_id: request_id,
        client_id: state.client_id,
        channel: channel,
        max_items: max_items,
        wait_timeout: wait_timeout,
        auto_ack: auto_ack,
        request_type_data: DownstreamRequestType.get()
      }

    try do
      _stream = GRPC.Stub.send_request(state.stream, pb_request)
      pending = Map.put(state.pending, request_id, {from, System.monotonic_time(:millisecond)})
      {:noreply, %{state | pending: pending}}
    rescue
      e ->
        {:reply,
         {:error,
          Error.stream_broken("downstream poll failed: #{inspect(e)}",
            operation: "poll_queue"
          )}, state}
    end
  end

  def handle_call({:ack_all, transaction_id}, _from, state) do
    with_pending_transaction(state, transaction_id, :acked, fn ->
      send_transaction(state, transaction_id, DownstreamRequestType.ack_all(), [], "")
    end)
  end

  def handle_call({:nack_all, transaction_id}, _from, state) do
    with_pending_transaction(state, transaction_id, :nacked, fn ->
      send_transaction(state, transaction_id, DownstreamRequestType.nack_all(), [], "")
    end)
  end

  def handle_call({:requeue_all, transaction_id, requeue_channel}, _from, state) do
    with_pending_transaction(state, transaction_id, :acked, fn ->
      send_transaction(
        state,
        transaction_id,
        DownstreamRequestType.requeue_all(),
        [],
        requeue_channel
      )
    end)
  end

  def handle_call({:ack_range, transaction_id, sequences}, _from, state) do
    result =
      send_transaction(state, transaction_id, DownstreamRequestType.ack_range(), sequences, "")

    {:reply, result, state}
  end

  def handle_call({:nack_range, transaction_id, sequences}, _from, state) do
    result =
      send_transaction(state, transaction_id, DownstreamRequestType.nack_range(), sequences, "")

    {:reply, result, state}
  end

  def handle_call({:requeue_range, transaction_id, sequences, requeue_channel}, _from, state) do
    result =
      send_transaction(
        state,
        transaction_id,
        DownstreamRequestType.requeue_range(),
        sequences,
        requeue_channel
      )

    {:reply, result, state}
  end

  def handle_call({:active_offsets, transaction_id}, _from, state) do
    result =
      send_transaction(
        state,
        transaction_id,
        DownstreamRequestType.active_offsets(),
        [],
        ""
      )

    case result do
      :ok -> {:reply, {:ok, []}, state}
      other -> {:reply, other, state}
    end
  end

  def handle_call({:transaction_status, transaction_id}, _from, state) do
    result =
      send_transaction(
        state,
        transaction_id,
        DownstreamRequestType.transaction_status(),
        [],
        ""
      )

    case result do
      :ok -> {:reply, {:ok, true}, state}
      {:error, _} -> {:reply, {:ok, false}, state}
    end
  end

  # Handle stream_result messages (from recv_loop in test mode, or forwarded)
  @impl GenServer
  def handle_info({:stream_result, {:ok, response}}, state) do
    handle_decoded_response(response, state)
    |> case do
      %__MODULE__{} = new_state -> {:noreply, new_state}
    end
  end

  def handle_info({:stream_closed, reason}, state) do
    Logger.warning("[#{__MODULE__}] Stream closed: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error, Error.stream_broken("downstream stream closed", operation: "poll_queue")}
    )

    {:stop, {:shutdown, :stream_closed}, %{state | pending: %{}}}
  end

  # Handle gun response headers (HTTP/2 HEADERS frame from server)
  def handle_info({:gun_response, _conn_pid, _stream_ref, :nofin, 200, _headers}, state) do
    {:noreply, %{state | headers_received: true}}
  end

  def handle_info({:gun_response, _conn_pid, _stream_ref, :fin, _status, _headers}, state) do
    # Server closed the stream with headers only (no data)
    reply_all_pending(
      state,
      {:error, Error.stream_broken("stream closed by server", operation: "poll_queue")}
    )

    {:stop, {:shutdown, :stream_closed}, %{state | pending: %{}}}
  end

  # Handle gun data (HTTP/2 DATA frames from server)
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
        {:error, Error.stream_broken("stream ended", operation: "poll_queue")}
      )

      {:stop, {:shutdown, :stream_ended}, %{state | pending: %{}}}
    else
      {:noreply, state}
    end
  end

  # Handle gun trailers (HTTP/2 TRAILERS from server)
  def handle_info({:gun_trailers, _conn_pid, _stream_ref, _trailers}, state) do
    {:noreply, state}
  end

  # Handle gun errors
  def handle_info({:gun_error, _conn_pid, _stream_ref, reason}, state) do
    Logger.warning("[#{__MODULE__}] Gun stream error: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error, Error.stream_broken("stream error: #{inspect(reason)}", operation: "poll_queue")}
    )

    {:stop, {:shutdown, :stream_error}, %{state | pending: %{}}}
  end

  def handle_info({:gun_error, _conn_pid, reason}, state) do
    Logger.warning("[#{__MODULE__}] Gun connection error: #{inspect(reason)}")

    reply_all_pending(
      state,
      {:error,
       Error.stream_broken("connection error: #{inspect(reason)}", operation: "poll_queue")}
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

  def handle_info(:transaction_timeout, state) do
    Logger.warning("[#{__MODULE__}] Transaction timeout – stopping stale downstream process")
    {:stop, {:shutdown, :transaction_timeout}, state}
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
          {:error, Error.stream_broken("stream terminated", operation: "stream_send")}
        )
    end)

    :ok
  end

  # --- Private ---

  defp handle_decoded_response(response, state) do
    ref_id = response.ref_request_id || ""
    transaction_id = response.transaction_id || ""

    case Map.pop(state.pending, ref_id) do
      {nil, _pending} ->
        state

      {{from, _inserted_at}, pending} ->
        messages =
          (response.messages || [])
          |> Enum.map(fn msg ->
            KubeMQ.Transport.GRPC.queue_message_from_proto(msg)
            |> QueueMessage.from_transport()
          end)

        is_error = response.is_error || false
        error_str = response.error

        poll_response = %PollResponse{
          transaction_id: transaction_id,
          messages: messages,
          is_error: is_error,
          error: if(error_str in [nil, ""], do: nil, else: error_str),
          state: :pending,
          downstream_pid: self()
        }

        transactions = Map.put(state.transactions, transaction_id, :pending)
        GenServer.reply(from, {:ok, poll_response})
        %{state | pending: pending, transactions: transactions}
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

  defp with_pending_transaction(state, transaction_id, new_tx_state, fun) do
    case Map.get(state.transactions, transaction_id) do
      :pending ->
        result = fun.()
        {:reply, result, update_transaction_state(state, transaction_id, new_tx_state, result)}

      settled when settled in [:acked, :nacked] ->
        {:reply,
         {:error,
          %Error{
            message: "transaction already settled (state: #{settled})",
            code: :validation,
            retryable?: false
          }}, state}

      nil ->
        {:reply,
         {:error, %Error{message: "transaction not found", code: :not_found, retryable?: false}},
         state}
    end
  end

  defp schedule_pending_sweep do
    Process.send_after(self(), :sweep_pending, @pending_sweep_interval)
  end

  defp schedule_transaction_timeout do
    Process.send_after(self(), :transaction_timeout, @transaction_timeout)
  end

  defp send_transaction(state, transaction_id, request_type, sequences, requeue_channel) do
    pb_request =
      %KubeMQ.Proto.QueuesDownstreamRequest{
        request_id: UUID.generate(),
        client_id: state.client_id,
        request_type_data: request_type,
        ref_transaction_id: transaction_id,
        sequence_range: sequences,
        re_queue_channel: requeue_channel
      }

    try do
      _stream = GRPC.Stub.send_request(state.stream, pb_request)
      :ok
    rescue
      e ->
        {:error,
         Error.stream_broken("transaction operation failed: #{inspect(e)}",
           operation: "queue_transaction"
         )}
    end
  end

  defp update_transaction_state(state, transaction_id, new_state, :ok) do
    transactions = Map.put(state.transactions, transaction_id, new_state)
    %{state | transactions: transactions}
  end

  defp update_transaction_state(state, _transaction_id, _new_state, {:error, _}), do: state
end
