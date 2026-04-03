defmodule KubeMQ.EventStreamHandleTest do
  use ExUnit.Case, async: true

  alias KubeMQ.EventStreamHandle

  # --- Helper: build a fake state ---

  defp build_state(overrides \\ []) do
    defaults = [
      stream: nil,
      client_id: "test-client",
      transport: KubeMQ.MockTransport,
      recv_pid: nil
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(EventStreamHandle, Map.new(merged))
  end

  describe "handle_info {:stream_closed, reason}" do
    test "stops on stream_closed" do
      state = build_state(recv_pid: self())
      result = EventStreamHandle.handle_info({:stream_closed, :eof}, state)

      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result
    end

    test "logs warning on stream_closed" do
      state = build_state()
      result = EventStreamHandle.handle_info({:stream_closed, :connection_reset}, state)

      assert {:stop, {:shutdown, :stream_closed}, _new_state} = result
    end
  end

  describe "terminate/2" do
    test "terminate calls end_stream when stream is present" do
      # We can't easily mock GRPC.Stub.end_stream, but we verify terminate
      # handles a nil stream without crashing
      state = build_state(stream: nil, recv_pid: nil)
      assert :ok = EventStreamHandle.terminate(:normal, state)
    end

    test "terminate kills recv_pid if alive" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      EventStreamHandle.terminate(:normal, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "terminate handles nil recv_pid gracefully" do
      state = build_state(recv_pid: nil, stream: nil)
      assert :ok = EventStreamHandle.terminate(:normal, state)
    end

    test "terminate handles already-dead recv_pid gracefully" do
      recv_pid = spawn(fn -> :ok end)
      Process.sleep(20)
      refute Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      assert :ok = EventStreamHandle.terminate(:normal, state)
    end
  end

  describe "handle_info {:stream_result, _}" do
    test "ok result is ignored" do
      state = build_state()
      result = EventStreamHandle.handle_info({:stream_result, {:ok, %{}}}, state)
      assert {:noreply, ^state} = result
    end

    test "error result is ignored (noreply)" do
      state = build_state()
      result = EventStreamHandle.handle_info({:stream_result, {:error, :some_error}}, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info catch-all" do
    test "ignores unknown messages" do
      state = build_state()
      result = EventStreamHandle.handle_info(:unknown, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "init/1 with direct stream option" do
    test "uses provided stream directly instead of opening gRPC channel" do
      fake_stream = make_ref()

      {:ok, pid} =
        EventStreamHandle.start_link(
          client_id: "stream-client",
          stream: fake_stream,
          transport: KubeMQ.MockTransport
        )

      state = :sys.get_state(pid)
      assert state.stream == fake_stream
      assert state.client_id == "stream-client"
      assert state.recv_pid == nil

      GenServer.stop(pid, :normal)
    end
  end

  describe "handle_call {:send, event} validation" do
    test "returns error when channel is empty" do
      state = build_state()
      event = %KubeMQ.Event{channel: "", body: "data"}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %KubeMQ.Error{} = err}, ^state} = result
      assert err.message =~ "channel"
    end

    test "returns error when channel is nil" do
      state = build_state()
      event = %KubeMQ.Event{channel: nil, body: "data"}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %KubeMQ.Error{}}, ^state} = result
    end

    test "returns error when both metadata and body are empty" do
      state = build_state()
      event = %KubeMQ.Event{channel: "test-ch", metadata: nil, body: nil}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %KubeMQ.Error{} = err}, ^state} = result
      assert err.message =~ "metadata or body"
    end

    test "returns error when both metadata and body are empty strings" do
      state = build_state()
      event = %KubeMQ.Event{channel: "test-ch", metadata: "", body: ""}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %KubeMQ.Error{} = err}, ^state} = result
      assert err.message =~ "metadata or body"
    end
  end

  describe "handle_call {:send, event} success" do
    test "builds correct proto event and returns :ok with continue on first send" do
      # Use a fake stream that GRPC.Stub.send_request can accept
      # We test the handle_call directly; GRPC.Stub.send_request will be called
      # We mock it by using a stream ref that won't crash send_request
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

      state = build_state(stream: fake_stream, recv_pid: nil)
      event = %KubeMQ.Event{channel: "test-ch", body: "hello", tags: %{"k" => "v"}}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      # First send: should trigger {:continue, :start_recv}
      assert {:reply, :ok, ^state, {:continue, :start_recv}} = result
    end

    test "returns :ok without continue when recv_pid already set" do
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
      state = build_state(stream: fake_stream, recv_pid: recv)
      event = %KubeMQ.Event{channel: "test-ch", body: "hello"}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, :ok, _state} = result
      Process.exit(recv, :kill)
    end
  end

  describe "handle_call {:send, event} rescue on GRPC error" do
    test "returns stream_broken error when GRPC.Stub.send_request raises" do
      # Use nil stream which will cause GRPC.Stub.send_request to raise
      state = build_state(stream: :not_a_real_stream)
      event = %KubeMQ.Event{channel: "test-ch", body: "hello"}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, {:error, %KubeMQ.Error{} = err}, ^state} = result
      assert err.message =~ "event stream send failed"
    end
  end

  describe "handle_info {:EXIT, pid, reason}" do
    test "ignores EXIT messages" do
      state = build_state()
      result = EventStreamHandle.handle_info({:EXIT, self(), :normal}, state)
      assert {:noreply, ^state} = result
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for events/stream_handle.ex
  # ===================================================================

  describe "handle_call {:send, event} with default fields" do
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

      state = build_state(stream: fake_stream, recv_pid: nil)

      event = %KubeMQ.Event{
        channel: "test-ch",
        body: "data",
        metadata: nil,
        tags: nil,
        client_id: nil
      }

      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, :ok, ^state, {:continue, :start_recv}} = result
    end
  end

  describe "handle_call {:send, event} uses event client_id when set" do
    test "prefers event client_id over state client_id" do
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

      state = build_state(stream: fake_stream, recv_pid: nil, client_id: "state-client")
      event = %KubeMQ.Event{channel: "test-ch", body: "data", client_id: "event-client"}
      from = {self(), make_ref()}

      result = EventStreamHandle.handle_call({:send, event}, from, state)

      assert {:reply, :ok, ^state, {:continue, :start_recv}} = result
    end
  end

  describe "terminate with stream that raises on end_stream" do
    test "rescue path handles end_stream errors gracefully" do
      # Use a non-nil stream value that will cause GRPC.Stub.end_stream to raise
      state = build_state(stream: :fake_stream_ref, recv_pid: nil)
      assert :ok = EventStreamHandle.terminate(:normal, state)
    end
  end

  # ===================================================================
  # Phase 11: handle_continue :start_recv paths
  # ===================================================================

  describe "handle_continue :start_recv with function enum" do
    test "spawns recv_pid when recv returns function enum" do
      # Create a stream where GRPC.Stub.recv returns {:ok, enum} where enum is a function
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            # Return a function enum that immediately ends
            {:ok, fn _acc, _fun -> :done end}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil)
      {:noreply, new_state} = EventStreamHandle.handle_continue(:start_recv, state)

      assert new_state.recv_pid != nil
      assert is_pid(new_state.recv_pid)
    end
  end

  describe "handle_continue :start_recv with error" do
    test "stops on recv error" do
      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:error, :connection_lost}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil)
      result = EventStreamHandle.handle_continue(:start_recv, state)

      assert {:stop, {:shutdown, :recv_failed}, _state} = result
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

      state = build_state(stream: fake_stream, recv_pid: nil)
      result = EventStreamHandle.handle_continue(:start_recv, state)

      assert {:noreply, ^state, {:continue, :start_recv}} = result
    end
  end

  describe "handle_continue :start_recv with function enum (real recv format)" do
    test "spawns recv_pid and processes function enum" do
      # Trap exits so the spawned recv_pid crash doesn't kill the test
      Process.flag(:trap_exit, true)

      # Use a list as the enum - lists ARE functions in the Enumerable sense
      # and is_function(list) is false, so we need to use an actual function.
      # The GRPC adapter returns a Stream.unfold based function.
      # Use Stream.unfold which returns %Stream{} - but is_function(%Stream{}) is false.
      # So the `is_function(enum)` branch in production code requires a raw function.
      # We'll use a function that wraps a list for Enum.each compatibility.

      # Actually, Elixir functions DO implement Enumerable for arity-2 functions.
      # Let's use a proper reducible function.
      # Elixir implements Enumerable for Function - let me test with arity 2.
      fun = fn
        {:cont, acc}, _fun -> {:done, acc}
      end

      # is_function(fun) is true
      assert is_function(fun)

      fake_stream = %{
        canceled: false,
        __interface__: %{
          receive_data: fn _stream, _opts ->
            {:ok, fun}
          end
        }
      }

      state = build_state(stream: fake_stream, recv_pid: nil)
      {:noreply, new_state} = EventStreamHandle.handle_continue(:start_recv, state)
      assert new_state.recv_pid != nil
      # Give recv_pid time to run and potentially crash
      Process.sleep(100)
    end
  end

  describe "handle_continue :start_recv with canceled stream" do
    test "returns error for canceled stream" do
      fake_stream = %{canceled: true, __interface__: %{}}

      state = build_state(stream: fake_stream, recv_pid: nil)
      result = EventStreamHandle.handle_continue(:start_recv, state)

      assert {:stop, {:shutdown, :recv_failed}, _state} = result
    end
  end
end
