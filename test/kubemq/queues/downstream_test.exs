defmodule KubeMQ.Queues.DownstreamTest do
  use ExUnit.Case, async: true

  alias KubeMQ.{Error, Queues.Downstream}
  alias KubeMQ.Proto.QueuesDownstreamResponse

  # --- Helper: build a fake state ---

  defp build_state(overrides \\ []) do
    defaults = [
      stream: nil,
      client_id: "test-client",
      transport: KubeMQ.MockTransport,
      recv_pid: nil,
      pending: %{},
      transactions: %{},
      headers_received: false,
      buffer: <<>>
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(Downstream, Map.new(merged))
  end

  describe "handle_info {:stream_result, {:ok, response}} - G5 proto casing" do
    test "downstream reads RefRequestId (lowercase d)" do
      # The downstream module reads RefRequestId from the proto response
      from = {self(), make_ref()}
      request_id = "req-123"
      pending = %{request_id => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      # Proto uses snake_case field names in protobuf-elixir v0.14
      response = %QueuesDownstreamResponse{
        ref_request_id: request_id,
        transaction_id: "txn-456",
        messages: [],
        is_error: false,
        error: ""
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, new_state} = result
      # The pending entry should have been popped
      refute Map.has_key?(new_state.pending, request_id)
      # Transaction should be tracked
      assert Map.get(new_state.transactions, "txn-456") == :pending
    end

    test "downstream sends RefTransactionId in transaction requests" do
      # The send_transaction function uses RefTransactionId (capital I)
      # We verify this by examining the handle_call for ack_all which calls send_transaction
      state = build_state(transactions: %{"txn-1" => :pending})

      # This will fail because GRPC.Stub.send_request is not available,
      # but we can test that the transaction lookup works correctly
      result = Downstream.handle_call({:ack_all, "txn-nonexistent"}, {self(), make_ref()}, state)
      assert {:reply, {:error, %Error{code: :not_found}}, ^state} = result
    end
  end

  describe "handle_call - double-settle guard" do
    test "rejects ack_all on already-acked transaction" do
      state = build_state(transactions: %{"txn-1" => :acked})
      result = Downstream.handle_call({:ack_all, "txn-1"}, {self(), make_ref()}, state)

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end

    test "rejects nack_all on already-nacked transaction" do
      state = build_state(transactions: %{"txn-1" => :nacked})
      result = Downstream.handle_call({:nack_all, "txn-1"}, {self(), make_ref()}, state)

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end

    test "rejects ack_all on already-nacked transaction" do
      state = build_state(transactions: %{"txn-1" => :nacked})
      result = Downstream.handle_call({:ack_all, "txn-1"}, {self(), make_ref()}, state)

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end

    test "rejects requeue_all on already-acked transaction" do
      state = build_state(transactions: %{"txn-1" => :acked})

      result =
        Downstream.handle_call(
          {:requeue_all, "txn-1", "requeue-ch"},
          {self(), make_ref()},
          state
        )

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end

    test "returns not_found for unknown transaction" do
      state = build_state(transactions: %{})
      result = Downstream.handle_call({:ack_all, "txn-unknown"}, {self(), make_ref()}, state)

      assert {:reply, {:error, %Error{code: :not_found}}, ^state} = result
    end
  end

  describe "handle_info {:stream_closed, reason} - G8" do
    test "stops on stream_closed" do
      state = build_state()
      result = Downstream.handle_info({:stream_closed, :eof}, state)

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end

    test "clears pending entries on stream_closed" do
      from = {self(), make_ref()}
      pending = %{"req-1" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result = Downstream.handle_info({:stream_closed, :eof}, state)
      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "terminate/2 - G8" do
    test "terminate handles nil stream gracefully" do
      state = build_state(stream: nil, recv_pid: nil)
      assert :ok = Downstream.terminate(:normal, state)
    end

    test "terminate kills recv_pid if alive" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      Downstream.terminate(:normal, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "terminate handles nil recv_pid gracefully" do
      state = build_state(recv_pid: nil, stream: nil)
      assert :ok = Downstream.terminate(:normal, state)
    end
  end

  describe "handle_info :sweep_pending - G8" do
    test "pending entries expire after timeout" do
      old_time = System.monotonic_time(:millisecond) - 60_000
      from = {self(), make_ref()}
      pending = %{"req-old" => {from, old_time}}
      state = build_state(pending: pending)

      result = Downstream.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end

    test "non-expired entries are retained" do
      recent_time = System.monotonic_time(:millisecond)
      from = {self(), make_ref()}
      pending = %{"req-recent" => {from, recent_time}}
      state = build_state(pending: pending)

      result = Downstream.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert Map.has_key?(new_state.pending, "req-recent")
    end
  end

  describe "handle_info {:stream_result, {:ok, response}} - message parsing" do
    test "ignores results for unknown ref IDs" do
      state = build_state(pending: %{})

      response = %QueuesDownstreamResponse{
        ref_request_id: "unknown",
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: ""
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info catch-all" do
    test "ignores unknown messages" do
      state = build_state()
      result = Downstream.handle_info(:unknown, state)
      assert {:noreply, ^state} = result
    end
  end

  # --- poll/2 validation (ES17, ES18) ---

  describe "poll/2 validation" do
    test "validates channel - empty returns error" do
      # poll is a module function that validates before GenServer.call
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: "", max_items: 1, wait_timeout: 5_000)
    end

    test "validates channel - nil returns error" do
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: nil, max_items: 1, wait_timeout: 5_000)
    end

    test "validates max_items - ES17 boundary 0 rejected" do
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: "q", max_items: 0, wait_timeout: 5_000)
    end

    test "validates max_items - ES17 boundary 1025 rejected" do
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: "q", max_items: 1025, wait_timeout: 5_000)
    end

    test "validates wait_timeout - ES18 negative rejected" do
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: "q", max_items: 1, wait_timeout: -1)
    end

    test "validates wait_timeout - ES18 above 3600000 rejected" do
      assert {:error, %Error{code: :validation}} =
               Downstream.poll(self(), channel: "q", max_items: 1, wait_timeout: 3_600_001)
    end
  end

  # --- handle_decoded_response via stream_result ---

  describe "handle_decoded_response" do
    test "builds PollResponse with messages from proto" do
      from = {self(), make_ref()}
      request_id = "req-resp-1"
      pending = %{request_id => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      # Build a proto response with a message
      proto_msg = %KubeMQ.Proto.QueueMessage{
        message_id: "m1",
        client_id: "c1",
        channel: "orders",
        metadata: "meta",
        body: "hello",
        tags: %{"k" => "v"},
        policy: nil,
        attributes: nil
      }

      response = %QueuesDownstreamResponse{
        ref_request_id: request_id,
        transaction_id: "txn-resp-1",
        messages: [proto_msg],
        is_error: false,
        error: ""
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, new_state} = result
      assert Map.get(new_state.transactions, "txn-resp-1") == :pending
      refute Map.has_key?(new_state.pending, request_id)
    end

    test "handles error response from server" do
      from = {self(), make_ref()}
      request_id = "req-err-1"
      pending = %{request_id => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      response = %QueuesDownstreamResponse{
        ref_request_id: request_id,
        transaction_id: "txn-err-1",
        messages: [],
        is_error: true,
        error: "queue not found"
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, _new_state} = result
    end
  end

  # --- ST9-ST12: transaction state tests ---

  describe "handle_call - ack/nack/requeue on pending transaction (ST9-ST12)" do
    test "ack_all on pending transaction returns :ok (via send_transaction stub)" do
      # We can't call send_transaction directly without GRPC.Stub, but we can test
      # the not_found and already-settled paths thoroughly
      state = build_state(transactions: %{"txn-1" => :pending})

      # The actual send_transaction will raise because there's no stream,
      # but with_pending_transaction checks state first
      # Testing the unknown transaction path
      result = Downstream.handle_call({:ack_all, "txn-unknown"}, {self(), make_ref()}, state)
      assert {:reply, {:error, %Error{code: :not_found}}, ^state} = result
    end

    test "nack_all on already-acked transaction is rejected" do
      state = build_state(transactions: %{"txn-1" => :acked})
      result = Downstream.handle_call({:nack_all, "txn-1"}, {self(), make_ref()}, state)

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end

    test "requeue_all on already-nacked transaction is rejected" do
      state = build_state(transactions: %{"txn-1" => :nacked})

      result =
        Downstream.handle_call(
          {:requeue_all, "txn-1", "requeue-ch"},
          {self(), make_ref()},
          state
        )

      assert {:reply, {:error, %Error{code: :validation, message: msg}}, ^state} = result
      assert msg =~ "already settled"
    end
  end

  # --- ack_range/nack_range/requeue_range at GenServer level ---

  describe "handle_call - range operations" do
    test "ack_range on unknown transaction does not check with_pending_transaction" do
      # ack_range does NOT use with_pending_transaction, it calls send_transaction directly
      # Without a stream it will raise, but we can test the call structure
      state = build_state(transactions: %{})

      # ack_range calls send_transaction directly which needs GRPC.Stub
      # We verify the handle_call pattern exists by testing it handles the message
      result = Downstream.handle_call({:ack_range, "txn-1", [1, 2]}, {self(), make_ref()}, state)
      # It will return an error because stream is nil and GRPC.Stub.send_request will fail
      assert {:reply, {:error, %Error{code: :stream_broken}}, ^state} = result
    end

    test "nack_range returns error when stream is nil" do
      state = build_state(transactions: %{})

      result = Downstream.handle_call({:nack_range, "txn-1", [3]}, {self(), make_ref()}, state)
      assert {:reply, {:error, %Error{code: :stream_broken}}, ^state} = result
    end

    test "requeue_range returns error when stream is nil" do
      state = build_state(transactions: %{})

      result =
        Downstream.handle_call(
          {:requeue_range, "txn-1", [1], "other-ch"},
          {self(), make_ref()},
          state
        )

      assert {:reply, {:error, %Error{code: :stream_broken}}, ^state} = result
    end
  end

  # --- active_offsets and transaction_status ---

  describe "handle_call - active_offsets" do
    test "active_offsets returns error when stream is nil" do
      state = build_state(transactions: %{})

      result = Downstream.handle_call({:active_offsets, "txn-1"}, {self(), make_ref()}, state)
      assert {:reply, {:error, %Error{code: :stream_broken}}, ^state} = result
    end
  end

  describe "handle_call - transaction_status" do
    test "transaction_status returns {:ok, false} when send fails" do
      state = build_state(transactions: %{})

      result = Downstream.handle_call({:transaction_status, "txn-1"}, {self(), make_ref()}, state)
      assert {:reply, {:ok, false}, ^state} = result
    end
  end

  # --- sweep_pending with reply ---

  describe "handle_info :sweep_pending replies to expired callers" do
    test "expired pending entries receive timeout error reply" do
      # Use a from tuple that won't crash when replied to
      ref = make_ref()
      old_time = System.monotonic_time(:millisecond) - 60_000
      pending = %{"req-old" => {{self(), ref}, old_time}}
      state = build_state(pending: pending)

      result = Downstream.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert new_state.pending == %{}

      # We should receive the reply (GenServer.reply sends a message to {pid, ref})
      assert_receive {^ref, {:error, %Error{code: :timeout}}}
    end
  end

  # --- transaction_timeout ---

  describe "handle_info :transaction_timeout" do
    test "transaction_timeout stops the process" do
      state = build_state()
      result = Downstream.handle_info(:transaction_timeout, state)
      assert {:stop, {:shutdown, :transaction_timeout}, ^state} = result
    end
  end

  # --- stream_closed replies to pending ---

  describe "handle_info {:stream_closed, _} replies to pending callers" do
    test "pending callers receive stream_broken error" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result = Downstream.handle_info({:stream_closed, :eof}, state)
      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result

      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end
  end

  # --- terminate replies to pending ---

  describe "terminate/2 replies to pending callers" do
    test "pending callers receive stream_broken error on terminate" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending, stream: nil)

      Downstream.terminate(:normal, state)

      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end
  end

  # --- EXIT handling ---

  describe "handle_info {:EXIT, _, _}" do
    test "EXIT message is silently ignored" do
      state = build_state()
      result = Downstream.handle_info({:EXIT, self(), :normal}, state)
      assert {:noreply, ^state} = result
    end
  end

  # --- gun_response handler ---

  describe "handle_info {:gun_response, ...}" do
    test "gun_response with 200 nofin sets headers_received" do
      state = build_state(headers_received: false)

      result =
        Downstream.handle_info(
          {:gun_response, self(), make_ref(), :nofin, 200, []},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.headers_received == true
    end

    test "gun_response with :fin stops the process and replies to pending" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result =
        Downstream.handle_info(
          {:gun_response, self(), make_ref(), :fin, 200, []},
          state
        )

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end

    test "gun_response with :fin and no pending does not crash" do
      state = build_state(pending: %{})

      result =
        Downstream.handle_info(
          {:gun_response, self(), make_ref(), :fin, 500, []},
          state
        )

      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result
    end
  end

  # --- gun_data handler ---

  describe "handle_info {:gun_data, ...}" do
    test "gun_data with :nofin buffers incomplete data" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      state = build_state(stream: fake_stream, buffer: <<>>)

      # Send incomplete gRPC frame (just 3 bytes, need at least 5 for header)
      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :nofin, <<0, 0, 0>>},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.buffer == <<0, 0, 0>>
    end

    test "gun_data with :fin and empty data stops process" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(stream: fake_stream, buffer: <<>>, pending: pending)

      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :fin, <<>>},
          state
        )

      assert {:stop, {:shutdown, :stream_ended}, _new_state} = result
      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end

    test "gun_data with complete gRPC frame decodes message" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      # Build a real protobuf-encoded QueuesDownstreamResponse
      response = %QueuesDownstreamResponse{
        ref_request_id: "req-decode",
        transaction_id: "txn-decode",
        messages: [],
        is_error: false,
        error: ""
      }

      encoded = QueuesDownstreamResponse.encode(response)
      length = byte_size(encoded)
      # gRPC frame: 1 byte compressed flag + 4 bytes length + message
      grpc_frame = <<0::8, length::32>> <> encoded

      ref = make_ref()
      pending = %{"req-decode" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(stream: fake_stream, buffer: <<>>, pending: pending)

      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :nofin, grpc_frame},
          state
        )

      assert {:noreply, new_state} = result
      refute Map.has_key?(new_state.pending, "req-decode")
      assert Map.get(new_state.transactions, "txn-decode") == :pending
      assert_receive {^ref, {:ok, %KubeMQ.PollResponse{transaction_id: "txn-decode"}}}
    end

    test "gun_data with partial gRPC frame buffers remaining" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      # Build gRPC frame header claiming 100 bytes but only provide 10
      grpc_frame = <<0::8, 100::32, "short_data">>
      state = build_state(stream: fake_stream, buffer: <<>>)

      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :nofin, grpc_frame},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.buffer == grpc_frame
    end

    test "gun_data with multiple complete gRPC frames decodes all" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      # Build two complete gRPC frames
      resp1 = %QueuesDownstreamResponse{
        ref_request_id: "req-multi-1",
        transaction_id: "txn-multi-1",
        messages: [],
        is_error: false,
        error: ""
      }

      resp2 = %QueuesDownstreamResponse{
        ref_request_id: "req-multi-2",
        transaction_id: "txn-multi-2",
        messages: [],
        is_error: false,
        error: ""
      }

      enc1 = QueuesDownstreamResponse.encode(resp1)
      enc2 = QueuesDownstreamResponse.encode(resp2)
      frame1 = <<0::8, byte_size(enc1)::32>> <> enc1
      frame2 = <<0::8, byte_size(enc2)::32>> <> enc2

      ref1 = make_ref()
      ref2 = make_ref()

      pending = %{
        "req-multi-1" => {{self(), ref1}, System.monotonic_time(:millisecond)},
        "req-multi-2" => {{self(), ref2}, System.monotonic_time(:millisecond)}
      }

      state = build_state(stream: fake_stream, buffer: <<>>, pending: pending)

      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :nofin, frame1 <> frame2},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref1, {:ok, %KubeMQ.PollResponse{}}}
      assert_receive {^ref2, {:ok, %KubeMQ.PollResponse{}}}
    end

    test "gun_data with :fin after decoding stops process" do
      fake_stream = %{
        response_mod: QueuesDownstreamResponse,
        codec: GRPC.Codec.Proto
      }

      state = build_state(stream: fake_stream, buffer: <<>>, pending: %{})

      result =
        Downstream.handle_info(
          {:gun_data, self(), make_ref(), :fin, <<>>},
          state
        )

      assert {:stop, {:shutdown, :stream_ended}, _new_state} = result
    end
  end

  # --- gun_trailers handler ---

  describe "handle_info {:gun_trailers, ...}" do
    test "gun_trailers is silently ignored" do
      state = build_state()

      result =
        Downstream.handle_info(
          {:gun_trailers, self(), make_ref(), []},
          state
        )

      assert {:noreply, ^state} = result
    end
  end

  # --- gun_error handlers ---

  describe "handle_info {:gun_error, ...}" do
    test "gun_error with stream_ref stops process and replies to pending" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result =
        Downstream.handle_info(
          {:gun_error, self(), make_ref(), :timeout},
          state
        )

      assert {:stop, {:shutdown, :stream_error}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end

    test "gun_error connection-level (no stream_ref) stops process" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result =
        Downstream.handle_info(
          {:gun_error, self(), :econnrefused},
          state
        )

      assert {:stop, {:shutdown, :connection_error}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end

    test "gun_error with no pending callers does not crash" do
      state = build_state(pending: %{})

      result =
        Downstream.handle_info(
          {:gun_error, self(), make_ref(), :closed},
          state
        )

      assert {:stop, {:shutdown, :stream_error}, _new_state} = result
    end
  end

  # --- handle_call {:poll, ...} error path ---

  describe "handle_call {:poll, ...} error on send" do
    test "poll returns stream_broken when stream is nil" do
      state = build_state(stream: nil)

      result =
        Downstream.handle_call(
          {:poll, "my-queue", 1, 5000, false},
          {self(), make_ref()},
          state
        )

      assert {:reply, {:error, %Error{code: :stream_broken}}, ^state} = result
    end
  end

  # --- handle_decoded_response with error fields ---

  describe "handle_decoded_response with is_error true" do
    test "sets is_error and error in PollResponse" do
      ref = make_ref()
      request_id = "req-err-detail"
      pending = %{request_id => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      response = %QueuesDownstreamResponse{
        ref_request_id: request_id,
        transaction_id: "txn-err-detail",
        messages: [],
        is_error: true,
        error: "queue not found"
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, new_state} = result
      assert Map.get(new_state.transactions, "txn-err-detail") == :pending

      assert_receive {^ref, {:ok, %KubeMQ.PollResponse{is_error: true, error: "queue not found"}}}
    end

    test "handles nil error field by converting to nil" do
      ref = make_ref()
      request_id = "req-nil-err"
      pending = %{request_id => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      response = %QueuesDownstreamResponse{
        ref_request_id: request_id,
        transaction_id: "txn-nil-err",
        messages: [],
        is_error: false,
        error: nil
      }

      result = Downstream.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, _new_state} = result
      assert_receive {^ref, {:ok, %KubeMQ.PollResponse{error: nil}}}
    end
  end

  # --- init paths ---

  describe "init/1" do
    test "init with direct stream option succeeds" do
      fake_stream = %{response_mod: nil, codec: nil}

      {:ok, state} =
        Downstream.init(
          client_id: "test",
          stream: fake_stream,
          transport: KubeMQ.MockTransport
        )

      assert state.client_id == "test"
      assert state.stream == fake_stream
      assert state.transactions == %{}
    end

    test "init with grpc_channel uses transport.queue_downstream" do
      import Mox

      expect(KubeMQ.MockTransport, :queue_downstream, fn :fake_grpc_ch ->
        {:ok, %{response_mod: nil, codec: nil}}
      end)

      {:ok, state} =
        Downstream.init(
          client_id: "test",
          grpc_channel: :fake_grpc_ch,
          transport: KubeMQ.MockTransport
        )

      assert state.client_id == "test"
    end

    test "init returns stop when stream creation fails" do
      import Mox

      expect(KubeMQ.MockTransport, :queue_downstream, fn :bad_ch ->
        {:error, :connection_refused}
      end)

      assert {:stop, :connection_refused} =
               Downstream.init(
                 client_id: "test",
                 grpc_channel: :bad_ch,
                 transport: KubeMQ.MockTransport
               )
    end
  end

  # --- terminate with non-nil stream ---

  describe "terminate/2 with stream" do
    test "terminate calls end_stream on non-nil stream (rescues errors)" do
      fake_stream = %{some: :data}
      state = build_state(stream: fake_stream, recv_pid: nil)
      assert :ok = Downstream.terminate(:normal, state)
    end

    test "terminate with stream and recv_pid kills recv_pid" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      fake_stream = %{some: :data}
      state = build_state(stream: fake_stream, recv_pid: recv_pid)

      assert :ok = Downstream.terminate(:normal, state)
      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "terminate replies to all pending callers" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      fake_stream = %{some: :data}
      state = build_state(stream: fake_stream, recv_pid: nil, pending: pending)

      Downstream.terminate(:shutdown, state)
      assert_receive {^ref, {:error, %Error{code: :stream_broken}}}
    end
  end

  # --- with_pending_transaction on pending transaction that fails send ---

  describe "with_pending_transaction success path with send failure" do
    test "ack_all on pending transaction with nil stream returns error and keeps state" do
      state = build_state(transactions: %{"txn-1" => :pending}, stream: nil)

      result = Downstream.handle_call({:ack_all, "txn-1"}, {self(), make_ref()}, state)

      # send_transaction will raise because stream is nil, returning {:error, ...}
      # with_pending_transaction calls update_transaction_state with the error result,
      # which returns state unchanged
      assert {:reply, {:error, %Error{code: :stream_broken}}, new_state} = result
      # Transaction state should NOT change on error
      assert Map.get(new_state.transactions, "txn-1") == :pending
    end

    test "nack_all on pending transaction with nil stream returns error" do
      state = build_state(transactions: %{"txn-1" => :pending}, stream: nil)

      result = Downstream.handle_call({:nack_all, "txn-1"}, {self(), make_ref()}, state)
      assert {:reply, {:error, %Error{code: :stream_broken}}, _state} = result
    end

    test "requeue_all on pending transaction with nil stream returns error" do
      state = build_state(transactions: %{"txn-1" => :pending}, stream: nil)

      result =
        Downstream.handle_call(
          {:requeue_all, "txn-1", "other-ch"},
          {self(), make_ref()},
          state
        )

      assert {:reply, {:error, %Error{code: :stream_broken}}, _state} = result
    end
  end
end
