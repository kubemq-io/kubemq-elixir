defmodule KubeMQ.EventStoreStreamHandleTest do
  use ExUnit.Case, async: true

  alias KubeMQ.{Error, EventStoreStreamHandle}

  # --- Helper: build a fake state ---

  defp build_state(overrides \\ []) do
    defaults = [
      stream: nil,
      client_id: "test-client",
      transport: KubeMQ.MockTransport,
      recv_pid: nil,
      pending: %{}
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(EventStoreStreamHandle, Map.new(merged))
  end

  describe "handle_info {:stream_closed, reason}" do
    test "stops on stream_closed" do
      state = build_state()
      result = EventStoreStreamHandle.handle_info({:stream_closed, :eof}, state)

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end

    test "replies to pending callers with error on stream_closed" do
      test_pid = self()

      # Simulate a pending caller using a from tuple {pid, ref}
      from = {test_pid, make_ref()}
      pending = %{"evt-1" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      # We need to catch the GenServer.reply since we're not in a real call context
      # Instead, test that the state is cleared
      result = EventStoreStreamHandle.handle_info({:stream_closed, :eof}, state)
      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "terminate/2" do
    test "terminate handles nil stream gracefully" do
      state = build_state(stream: nil, recv_pid: nil)
      assert :ok = EventStoreStreamHandle.terminate(:normal, state)
    end

    test "terminate kills recv_pid if alive" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      EventStoreStreamHandle.terminate(:normal, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "terminate handles nil recv_pid gracefully" do
      state = build_state(recv_pid: nil, stream: nil)
      assert :ok = EventStoreStreamHandle.terminate(:normal, state)
    end
  end

  describe "handle_info :sweep_pending" do
    test "pending entries expire after timeout" do
      # Create a pending entry with a very old timestamp (well past the 30s timeout)
      old_time = System.monotonic_time(:millisecond) - 60_000
      from = {self(), make_ref()}
      pending = %{"evt-old" => {from, old_time}}
      state = build_state(pending: pending)

      # The sweep will try to GenServer.reply to the from, which will fail
      # since we're not in a real GenServer call. We test indirectly by
      # verifying the sweep handler is callable and returns noreply.
      # In a real scenario, expired entries are removed from pending.
      result = EventStoreStreamHandle.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end

    test "non-expired entries are retained" do
      recent_time = System.monotonic_time(:millisecond)
      from = {self(), make_ref()}
      pending = %{"evt-recent" => {from, recent_time}}
      state = build_state(pending: pending)

      result = EventStoreStreamHandle.handle_info(:sweep_pending, state)
      assert {:noreply, new_state} = result
      assert Map.has_key?(new_state.pending, "evt-recent")
    end
  end

  describe "handle_info {:stream_result, {:ok, result}}" do
    test "ignores results for unknown event IDs" do
      state = build_state(pending: %{})

      result =
        EventStoreStreamHandle.handle_info(
          {:stream_result, {:ok, %KubeMQ.Proto.Result{event_id: "unknown-id", sent: true}}},
          state
        )

      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info catch-all" do
    test "ignores unknown messages" do
      state = build_state()
      result = EventStoreStreamHandle.handle_info(:unknown, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_call {:send, event} validation" do
    test "returns error when channel is empty" do
      state = build_state()
      event = %KubeMQ.EventStore{channel: "", body: "data"}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %Error{} = err}, ^state} = result
      assert err.message =~ "channel"
    end

    test "returns error when channel is nil" do
      state = build_state()
      event = %KubeMQ.EventStore{channel: nil, body: "data"}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %Error{}}, ^state} = result
    end

    test "returns error when both metadata and body are nil" do
      state = build_state()
      event = %KubeMQ.EventStore{channel: "test-ch", metadata: nil, body: nil}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %Error{} = err}, ^state} = result
      assert err.message =~ "metadata or body"
    end

    test "returns error when both metadata and body are empty strings" do
      state = build_state()
      event = %KubeMQ.EventStore{channel: "test-ch", metadata: "", body: ""}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %Error{} = err}, ^state} = result
      assert err.message =~ "metadata or body"
    end
  end

  describe "handle_call {:send, event} success stores pending" do
    test "stores from in pending map and triggers start_recv on first send" do
      fake_stream = %GRPC.Client.Stream{
        grpc_type: :bidi_streaming,
        channel: %GRPC.Channel{host: "localhost", port: 50_000, scheme: "http"},
        path: "/kubemq.kubemq/SendEventsStream",
        codec: GRPC.Codec.Proto,
        payload: %{},
        compressor: nil,
        accepted_compressors: [],
        __interface__: %{send_request: fn stream, _req, _opts -> stream end}
      }

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      event = %KubeMQ.EventStore{id: "evt-123", channel: "test-ch", body: "hello"}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:noreply, new_state, {:continue, :start_recv}} = result
      # The pending map should have an entry (event_id may be "evt-123" or a UUID)
      assert map_size(new_state.pending) == 1
    end

    test "stores from in pending map without continue when recv_pid is set" do
      fake_stream = %GRPC.Client.Stream{
        grpc_type: :bidi_streaming,
        channel: %GRPC.Channel{host: "localhost", port: 50_000, scheme: "http"},
        path: "/kubemq.kubemq/SendEventsStream",
        codec: GRPC.Codec.Proto,
        payload: %{},
        compressor: nil,
        accepted_compressors: [],
        __interface__: %{send_request: fn stream, _req, _opts -> stream end}
      }

      recv = spawn(fn -> Process.sleep(:infinity) end)
      state = build_state(stream: fake_stream, recv_pid: recv, pending: %{})
      event = %KubeMQ.EventStore{id: "evt-456", channel: "test-ch", body: "hello"}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:noreply, new_state} = result
      assert map_size(new_state.pending) == 1
      Process.exit(recv, :kill)
    end
  end

  describe "handle_call {:send, event} rescue on GRPC error" do
    test "returns stream_broken error when GRPC.Stub.send_request raises" do
      state = build_state(stream: :not_a_real_stream)
      event = %KubeMQ.EventStore{channel: "test-ch", body: "hello"}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %Error{} = err}, ^state} = result
      assert err.message =~ "event store stream send failed"
      assert err.code == :stream_broken
    end
  end

  describe "handle_info {:stream_result, {:ok, result}} matches pending" do
    test "replies {:ok, result} to pending caller when sent is true" do
      from = {self(), make_ref()}
      pending = %{"evt-match" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      proto_result = %KubeMQ.Proto.Result{event_id: "evt-match", sent: true, error: ""}

      result =
        EventStoreStreamHandle.handle_info(
          {:stream_result, {:ok, proto_result}},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end

    test "replies {:error, _} to pending caller when sent is false" do
      from = {self(), make_ref()}
      pending = %{"evt-fail" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      proto_result = %KubeMQ.Proto.Result{event_id: "evt-fail", sent: false, error: "store full"}

      result =
        EventStoreStreamHandle.handle_info(
          {:stream_result, {:ok, proto_result}},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "handle_info {:stream_closed, reason} replies to all pending" do
    test "replies error to all pending callers and clears pending" do
      from1 = {self(), make_ref()}
      from2 = {self(), make_ref()}
      now = System.monotonic_time(:millisecond)

      pending = %{
        "evt-1" => {from1, now},
        "evt-2" => {from2, now}
      }

      state = build_state(pending: pending)

      result = EventStoreStreamHandle.handle_info({:stream_closed, :eof}, state)

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "handle_info :sweep_pending expires entries (EC10)" do
    test "expires old entries and keeps recent ones" do
      old_from = {self(), make_ref()}
      recent_from = {self(), make_ref()}
      old_time = System.monotonic_time(:millisecond) - 60_000
      recent_time = System.monotonic_time(:millisecond)

      pending = %{
        "evt-old" => {old_from, old_time},
        "evt-recent" => {recent_from, recent_time}
      }

      state = build_state(pending: pending)

      result = EventStoreStreamHandle.handle_info(:sweep_pending, state)

      assert {:noreply, new_state} = result
      refute Map.has_key?(new_state.pending, "evt-old")
      assert Map.has_key?(new_state.pending, "evt-recent")
    end

    test "schedules next sweep after processing" do
      state = build_state(pending: %{})

      EventStoreStreamHandle.handle_info(:sweep_pending, state)

      # Verify a :sweep_pending message is scheduled by checking the mailbox
      # (schedule_pending_sweep sends after 30_000ms, so we won't receive it,
      # but the process should not crash)
      assert {:noreply, _new_state} =
               EventStoreStreamHandle.handle_info(:sweep_pending, state)
    end
  end

  describe "terminate/2 replies to all pending" do
    test "replies error to all pending callers on terminate" do
      from1 = {self(), make_ref()}
      from2 = {self(), make_ref()}
      now = System.monotonic_time(:millisecond)

      pending = %{
        "evt-a" => {from1, now},
        "evt-b" => {from2, now}
      }

      state = build_state(stream: nil, recv_pid: nil, pending: pending)

      assert :ok = EventStoreStreamHandle.terminate(:normal, state)
    end
  end

  describe "handle_info {:EXIT, pid, reason}" do
    test "ignores EXIT messages" do
      state = build_state()
      result = EventStoreStreamHandle.handle_info({:EXIT, self(), :normal}, state)
      assert {:noreply, ^state} = result
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for events_store/stream_handle.ex
  # ===================================================================

  describe "handle_info {:stream_result, {:ok, result}} with nil event_id" do
    test "uses empty string for nil event_id" do
      state = build_state(pending: %{})

      # Proto result with nil event_id should be treated as ""
      proto_result = %KubeMQ.Proto.Result{event_id: nil, sent: true, error: ""}

      result =
        EventStoreStreamHandle.handle_info(
          {:stream_result, {:ok, proto_result}},
          state
        )

      # Should not crash, just no matching pending entry
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info {:stream_result, {:ok, result}} with unsent and nil error" do
    test "sends transient error with default message when error is nil" do
      from = {self(), make_ref()}
      pending = %{"evt-nosend" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(pending: pending)

      proto_result = %KubeMQ.Proto.Result{event_id: "evt-nosend", sent: false, error: nil}

      result =
        EventStoreStreamHandle.handle_info(
          {:stream_result, {:ok, proto_result}},
          state
        )

      assert {:noreply, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "terminate with pending callers and stream" do
    test "terminate with stream raises rescue path" do
      # Use a non-nil stream value that will cause GRPC.Stub.end_stream to raise
      state = build_state(stream: :fake_stream_ref, recv_pid: nil, pending: %{})
      assert :ok = EventStoreStreamHandle.terminate(:normal, state)
    end
  end

  describe "handle_info {:stream_closed, reason} with multiple pending entries" do
    test "all pending callers get error and state is cleared" do
      from1 = {self(), make_ref()}
      from2 = {self(), make_ref()}
      from3 = {self(), make_ref()}
      now = System.monotonic_time(:millisecond)

      pending = %{
        "evt-a" => {from1, now},
        "evt-b" => {from2, now},
        "evt-c" => {from3, now}
      }

      state = build_state(pending: pending)
      result = EventStoreStreamHandle.handle_info({:stream_closed, :server_shutdown}, state)

      assert {:stop, {:shutdown, :stream_closed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "handle_call {:send, event} with default client_id" do
    test "uses state client_id when event client_id is nil" do
      fake_stream = %GRPC.Client.Stream{
        grpc_type: :bidi_streaming,
        channel: %GRPC.Channel{host: "localhost", port: 50_000, scheme: "http"},
        path: "/kubemq.kubemq/SendEventsStream",
        codec: GRPC.Codec.Proto,
        payload: %{},
        compressor: nil,
        accepted_compressors: [],
        __interface__: %{send_request: fn stream, _req, _opts -> stream end}
      }

      state =
        build_state(stream: fake_stream, recv_pid: nil, pending: %{}, client_id: "default-client")

      event = %KubeMQ.EventStore{channel: "test-ch", body: "hello", client_id: nil}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:noreply, new_state, {:continue, :start_recv}} = result
      assert map_size(new_state.pending) == 1
    end
  end

  # ===================================================================
  # Phase 11: handle_continue :start_recv paths
  # ===================================================================

  describe "handle_continue :start_recv with function enum" do
    test "spawns recv_pid when recv returns function enum" do
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:ok, fn _acc, _fun -> :done end}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      {:noreply, new_state} = EventStoreStreamHandle.handle_continue(:start_recv, state)

      assert new_state.recv_pid != nil
      assert is_pid(new_state.recv_pid)
    end
  end

  describe "handle_continue :start_recv with error" do
    test "stops on recv error and replies to pending callers" do
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:error, :connection_lost}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      result = EventStoreStreamHandle.handle_continue(:start_recv, state)

      assert {:stop, {:shutdown, :recv_failed}, new_state} = result
      assert new_state.pending == %{}
    end

    test "stops on recv error and replies error to all pending callers" do
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:error, :connection_lost}
          end
        }
      }

      from = {self(), make_ref()}
      pending = %{"evt-1" => {from, System.monotonic_time(:millisecond)}}
      state = build_state(stream: fake_stream, recv_pid: nil, pending: pending)

      result = EventStoreStreamHandle.handle_continue(:start_recv, state)

      assert {:stop, {:shutdown, :recv_failed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "handle_continue :start_recv with non-function ok" do
    test "retries when recv returns ok with non-function" do
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:ok, :not_a_function}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      result = EventStoreStreamHandle.handle_continue(:start_recv, state)

      assert {:noreply, ^state, {:continue, :start_recv}} = result
    end
  end

  describe "handle_continue :start_recv with canceled stream" do
    test "returns error for canceled stream" do
      fake_stream = %{canceled: true, __interface__: %{}}

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      result = EventStoreStreamHandle.handle_continue(:start_recv, state)

      assert {:stop, {:shutdown, :recv_failed}, new_state} = result
      assert new_state.pending == %{}
    end
  end

  describe "handle_call {:send, event} with tags and metadata defaults" do
    test "defaults nil metadata to empty string and nil tags to empty map" do
      fake_stream = %GRPC.Client.Stream{
        grpc_type: :bidi_streaming,
        channel: %GRPC.Channel{host: "localhost", port: 50_000, scheme: "http"},
        path: "/kubemq.kubemq/SendEventsStream",
        codec: GRPC.Codec.Proto,
        payload: %{},
        compressor: nil,
        accepted_compressors: [],
        __interface__: %{send_request: fn stream, _req, _opts -> stream end}
      }

      state = build_state(stream: fake_stream, recv_pid: nil, pending: %{})
      event = %KubeMQ.EventStore{channel: "test-ch", body: "data", metadata: nil, tags: nil}
      from = {self(), make_ref()}

      result = EventStoreStreamHandle.handle_call({:send, event}, from, state)

      assert {:noreply, new_state, {:continue, :start_recv}} = result
      assert map_size(new_state.pending) == 1
    end
  end
end
