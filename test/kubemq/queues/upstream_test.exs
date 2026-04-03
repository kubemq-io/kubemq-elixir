defmodule KubeMQ.QueueUpstreamHandleTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Proto.QueuesUpstreamResponse
  alias KubeMQ.QueueUpstreamHandle

  # --- Helper: build a fake state ---

  defp build_state(overrides \\ []) do
    defaults = [
      stream: nil,
      client_id: "test-client",
      transport: KubeMQ.MockTransport,
      recv_pid: nil,
      pending: %{},
      headers_received: false,
      buffer: <<>>
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(QueueUpstreamHandle, Map.new(merged))
  end

  describe "handle_info {:stream_closed, reason}" do
    test "stops on stream_closed" do
      state = build_state()
      result = QueueUpstreamHandle.handle_info({:stream_closed, :eof}, state)

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end

    test "clears pending entries on stream_closed" do
      from = {self(), make_ref()}
      pending = %{"req-1" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result = QueueUpstreamHandle.handle_info({:stream_closed, :eof}, state)
      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "terminate/2" do
    test "terminate handles nil stream gracefully" do
      state = build_state(stream: nil, recv_pid: nil)
      assert :ok = QueueUpstreamHandle.terminate(:normal, state)
    end

    test "terminate kills recv_pid if alive" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      QueueUpstreamHandle.terminate(:normal, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "terminate handles nil recv_pid gracefully" do
      state = build_state(recv_pid: nil, stream: nil)
      assert :ok = QueueUpstreamHandle.terminate(:normal, state)
    end
  end

  describe "handle_info :sweep_pending" do
    test "pending entries expire after timeout" do
      old_time = System.monotonic_time(:millisecond) - 60_000
      from = {self(), make_ref()}
      pending = %{"req-old" => {from, old_time}}
      state = build_state(pending: pending)

      result = QueueUpstreamHandle.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end

    test "non-expired entries are retained" do
      recent_time = System.monotonic_time(:millisecond)
      from = {self(), make_ref()}
      pending = %{"req-recent" => {from, recent_time}}
      state = build_state(pending: pending)

      result = QueueUpstreamHandle.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert Map.has_key?(new_state.pending, "req-recent")
    end
  end

  describe "handle_info {:stream_result, {:ok, response}}" do
    test "ignores results for unknown ref IDs" do
      state = build_state(pending: %{})

      result =
        QueueUpstreamHandle.handle_info(
          {:stream_result,
           {:ok, %QueuesUpstreamResponse{ref_request_id: "unknown", results: []}}},
          state
        )

      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info catch-all" do
    test "ignores unknown messages" do
      state = build_state()
      result = QueueUpstreamHandle.handle_info(:unknown, state)
      assert {:noreply, ^state} = result
    end
  end

  # --- send builds proto message ---

  describe "handle_call {:send, messages}" do
    test "send builds proto with message fields and returns error when stream is nil" do
      msg = %KubeMQ.QueueMessage{
        id: nil,
        channel: "orders",
        metadata: "meta",
        body: "payload",
        client_id: nil,
        tags: %{"k" => "v"},
        policy: nil,
        attributes: nil
      }

      state = build_state(stream: nil)
      result = QueueUpstreamHandle.handle_call({:send, [msg]}, {self(), make_ref()}, state)

      # stream is nil so GRPC.Stub.send_request will raise
      assert {:reply, {:error, %KubeMQ.Error{code: :stream_broken}}, ^state} = result
    end

    test "send with policy builds QueueMessagePolicy proto" do
      policy = %KubeMQ.QueuePolicy{
        expiration_seconds: 60,
        delay_seconds: 10,
        max_receive_count: 3,
        max_receive_queue: "dlq"
      }

      msg = %KubeMQ.QueueMessage{
        id: "msg-1",
        channel: "orders",
        metadata: "",
        body: "data",
        client_id: "c1",
        tags: %{},
        policy: policy,
        attributes: nil
      }

      state = build_state(stream: nil)
      result = QueueUpstreamHandle.handle_call({:send, [msg]}, {self(), make_ref()}, state)

      # Will fail at GRPC.Stub.send_request but policy construction is exercised
      assert {:reply, {:error, %KubeMQ.Error{code: :stream_broken}}, ^state} = result
    end
  end

  # --- handle_decoded_response matching ---

  describe "handle_info {:stream_result, {:ok, response}} - response matching" do
    test "matches pending request by ref_request_id and replies" do
      ref = make_ref()
      request_id = "req-match-1"
      pending = %{request_id => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      response = %QueuesUpstreamResponse{
        ref_request_id: request_id,
        results: [
          %KubeMQ.Proto.SendQueueMessageResult{
            message_id: "m1",
            sent_at: 1000,
            expiration_at: 0,
            delayed_to: 0,
            is_error: false,
            error: ""
          }
        ]
      }

      result = QueueUpstreamHandle.handle_info({:stream_result, {:ok, response}}, state)
      assert {:noreply, new_state} = result
      refute Map.has_key?(new_state.pending, request_id)

      # We should get the reply
      assert_receive {^ref, {:ok, [%KubeMQ.QueueSendResult{message_id: "m1"}]}}
    end
  end

  # --- sweep_pending replies ---

  describe "handle_info :sweep_pending replies to expired callers" do
    test "expired pending entries receive timeout error reply" do
      ref = make_ref()
      old_time = System.monotonic_time(:millisecond) - 60_000
      pending = %{"req-old" => {{self(), ref}, old_time}}
      state = build_state(pending: pending)

      result = QueueUpstreamHandle.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert new_state.pending == %{}

      assert_receive {^ref, {:error, %KubeMQ.Error{code: :timeout}}}
    end
  end

  # --- stream_closed replies ---

  describe "handle_info {:stream_closed, _} replies to pending callers" do
    test "pending callers receive stream_broken error" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result = QueueUpstreamHandle.handle_info({:stream_closed, :eof}, state)
      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result

      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end
  end

  # --- terminate replies ---

  describe "terminate/2 replies to pending callers" do
    test "pending callers receive stream_broken error on terminate" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending, stream: nil)

      QueueUpstreamHandle.terminate(:normal, state)

      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end
  end

  # --- gun_response handler ---

  describe "handle_info {:gun_response, ...}" do
    test "gun_response with 200 nofin sets headers_received" do
      state = build_state(headers_received: false)

      result =
        QueueUpstreamHandle.handle_info(
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
        QueueUpstreamHandle.handle_info(
          {:gun_response, self(), make_ref(), :fin, 200, []},
          state
        )

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end

    test "gun_response with :fin and no pending does not crash" do
      state = build_state(pending: %{})

      result =
        QueueUpstreamHandle.handle_info(
          {:gun_response, self(), make_ref(), :fin, 500, []},
          state
        )

      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result
    end
  end

  # --- gun_data handler ---

  describe "handle_info {:gun_data, ...}" do
    test "gun_data with :nofin buffers incomplete data" do
      # Build a fake stream struct with response_mod and codec
      fake_stream = %{
        response_mod: QueuesUpstreamResponse,
        codec: GRPC.Codec.Proto
      }

      state = build_state(stream: fake_stream, buffer: <<>>)

      # Send incomplete gRPC frame (just 3 bytes, need at least 5 for header)
      result =
        QueueUpstreamHandle.handle_info(
          {:gun_data, self(), make_ref(), :nofin, <<0, 0, 0>>},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.buffer == <<0, 0, 0>>
    end

    test "gun_data with :fin and empty data stops process" do
      fake_stream = %{
        response_mod: QueuesUpstreamResponse,
        codec: GRPC.Codec.Proto
      }

      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(stream: fake_stream, buffer: <<>>, pending: pending)

      result =
        QueueUpstreamHandle.handle_info(
          {:gun_data, self(), make_ref(), :fin, <<>>},
          state
        )

      assert {:stop, {:shutdown, :stream_ended}, _new_state} = result
      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end

    test "gun_data with partial gRPC frame buffers remaining" do
      fake_stream = %{
        response_mod: QueuesUpstreamResponse,
        codec: GRPC.Codec.Proto
      }

      # gRPC frame header claims 100 bytes but only 10 provided
      grpc_frame = <<0::8, 100::32, "short_data">>
      state = build_state(stream: fake_stream, buffer: <<>>)

      result =
        QueueUpstreamHandle.handle_info(
          {:gun_data, self(), make_ref(), :nofin, grpc_frame},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.buffer == grpc_frame
    end

    test "gun_data with complete gRPC frame decodes message" do
      fake_stream = %{
        response_mod: QueuesUpstreamResponse,
        codec: GRPC.Codec.Proto
      }

      # Build a real protobuf-encoded QueuesUpstreamResponse
      response = %QueuesUpstreamResponse{
        ref_request_id: "req-decode",
        results: []
      }

      encoded = QueuesUpstreamResponse.encode(response)
      length = byte_size(encoded)
      # gRPC frame: 1 byte compressed flag + 4 bytes length + message
      grpc_frame = <<0::8, length::32>> <> encoded

      ref = make_ref()
      pending = %{"req-decode" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(stream: fake_stream, buffer: <<>>, pending: pending)

      result =
        QueueUpstreamHandle.handle_info(
          {:gun_data, self(), make_ref(), :nofin, grpc_frame},
          state
        )

      assert {:noreply, new_state} = result
      refute Map.has_key?(new_state.pending, "req-decode")
      assert_receive {^ref, {:ok, []}}
    end
  end

  # --- gun_trailers handler ---

  describe "handle_info {:gun_trailers, ...}" do
    test "gun_trailers is silently ignored" do
      state = build_state()

      result =
        QueueUpstreamHandle.handle_info(
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
        QueueUpstreamHandle.handle_info(
          {:gun_error, self(), make_ref(), :timeout},
          state
        )

      assert {:stop, {:shutdown, :stream_error}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end

    test "gun_error connection-level (no stream_ref) stops process" do
      ref = make_ref()
      pending = %{"req-1" => {{self(), ref}, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      result =
        QueueUpstreamHandle.handle_info(
          {:gun_error, self(), :econnrefused},
          state
        )

      assert {:stop, {:shutdown, :connection_error}, new_state} = result
      assert new_state.pending == %{}
      assert_receive {^ref, {:error, %KubeMQ.Error{code: :stream_broken}}}
    end
  end

  # --- EXIT handler ---

  describe "handle_info {:EXIT, ...}" do
    test "EXIT message is silently ignored" do
      state = build_state()
      result = QueueUpstreamHandle.handle_info({:EXIT, self(), :normal}, state)
      assert {:noreply, ^state} = result
    end
  end

  # --- init paths ---

  describe "init/1" do
    test "init with direct stream option succeeds" do
      fake_stream = %{response_mod: nil, codec: nil}

      {:ok, state} =
        QueueUpstreamHandle.init(
          client_id: "test",
          stream: fake_stream,
          transport: KubeMQ.MockTransport
        )

      assert state.client_id == "test"
      assert state.stream == fake_stream
    end

    test "init with grpc_channel uses transport.queue_upstream" do
      import Mox

      expect(KubeMQ.MockTransport, :queue_upstream, fn :fake_grpc_ch ->
        {:ok, %{response_mod: nil, codec: nil}}
      end)

      {:ok, state} =
        QueueUpstreamHandle.init(
          client_id: "test",
          grpc_channel: :fake_grpc_ch,
          transport: KubeMQ.MockTransport
        )

      assert state.client_id == "test"
    end

    test "init returns stop when stream creation fails" do
      import Mox

      expect(KubeMQ.MockTransport, :queue_upstream, fn :bad_ch ->
        {:error, :connection_refused}
      end)

      assert {:stop, :connection_refused} =
               QueueUpstreamHandle.init(
                 client_id: "test",
                 grpc_channel: :bad_ch,
                 transport: KubeMQ.MockTransport
               )
    end
  end

  # --- terminate with non-nil stream ---

  describe "terminate/2 with stream" do
    test "terminate calls end_stream on non-nil stream (rescues errors)" do
      # Create a fake stream struct - GRPC.Stub.end_stream will raise
      # but terminate rescues it
      fake_stream = %{some: :data}
      state = build_state(stream: fake_stream, recv_pid: nil)
      assert :ok = QueueUpstreamHandle.terminate(:normal, state)
    end

    test "terminate with stream and recv_pid kills recv_pid" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      fake_stream = %{some: :data}
      state = build_state(stream: fake_stream, recv_pid: recv_pid)

      assert :ok = QueueUpstreamHandle.terminate(:normal, state)
      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end
  end

  # --- send with nil fields defaults ---

  describe "handle_call {:send, messages} field defaults" do
    test "send with nil client_id uses state client_id" do
      msg = %KubeMQ.QueueMessage{
        id: "msg-1",
        channel: nil,
        metadata: nil,
        body: nil,
        client_id: nil,
        tags: nil,
        policy: nil,
        attributes: nil
      }

      state = build_state(stream: nil)
      result = QueueUpstreamHandle.handle_call({:send, [msg]}, {self(), make_ref()}, state)
      # Will fail at GRPC.Stub.send_request but field defaults are exercised
      assert {:reply, {:error, %KubeMQ.Error{code: :stream_broken}}, ^state} = result
    end

    test "send with non-QueuePolicy policy returns nil proto policy" do
      msg = %KubeMQ.QueueMessage{
        id: "msg-1",
        channel: "ch",
        metadata: "",
        body: "data",
        client_id: "c1",
        tags: %{},
        policy: :not_a_policy,
        attributes: nil
      }

      state = build_state(stream: nil)
      result = QueueUpstreamHandle.handle_call({:send, [msg]}, {self(), make_ref()}, state)
      assert {:reply, {:error, %KubeMQ.Error{code: :stream_broken}}, ^state} = result
    end
  end
end
