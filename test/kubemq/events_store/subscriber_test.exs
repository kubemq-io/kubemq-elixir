defmodule KubeMQ.EventsStore.SubscriberTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.EventsStore.Subscriber

  setup :set_mox_global
  setup :verify_on_exit!

  defp build_state(overrides \\ []) do
    defaults = [
      channel: "test-channel",
      group: "",
      client_id: "test-client",
      transport: KubeMQ.MockTransport,
      grpc_channel: :fake_grpc_channel,
      stream: nil,
      on_event: nil,
      on_error: nil,
      notify: nil,
      conn: nil,
      start_at: :start_from_first,
      last_sequence: 0,
      recv_pid: nil,
      task_supervisor: nil
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(Subscriber, Map.new(merged))
  end

  describe "init/1" do
    test "traps exits and validates start_at" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        send(test_pid, :subscribe_called)
        {:error, :unavailable}
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "test-ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          start_at: :start_from_first
        )

      assert_receive :subscribe_called, 1_000
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    end

    test "rejects invalid start_at" do
      Process.flag(:trap_exit, true)

      result =
        Subscriber.start_link(
          channel: "test-ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          start_at: :undefined
        )

      assert {:error, _} = result
    end

    test "rejects nil start_at" do
      Process.flag(:trap_exit, true)

      result =
        Subscriber.start_link(
          channel: "test-ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          start_at: nil
        )

      assert {:error, _} = result
    end
  end

  describe "handle_info :retry_resubscribe" do
    test "triggers resubscribe continue" do
      state = build_state()
      result = Subscriber.handle_info(:retry_resubscribe, state)
      assert {:noreply, ^state, {:continue, :resubscribe}} = result
    end
  end

  describe "handle_info {:EXIT, recv_pid, reason}" do
    test "clears recv_pid and triggers resubscribe" do
      fake_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      state = build_state(recv_pid: fake_pid)
      result = Subscriber.handle_info({:EXIT, fake_pid, :normal}, state)

      assert {:noreply, new_state, {:continue, :resubscribe}} = result
      assert new_state.recv_pid == nil
    end

    test "ignores EXIT from non-recv_pid" do
      other_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      state = build_state(recv_pid: nil)
      result = Subscriber.handle_info({:EXIT, other_pid, :normal}, state)

      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info {:stream_closed, reason}" do
    test "clears recv_pid and triggers resubscribe" do
      state = build_state(recv_pid: self())
      result = Subscriber.handle_info({:stream_closed, :eof}, state)

      assert {:noreply, new_state, {:continue, :resubscribe}} = result
      assert new_state.recv_pid == nil
    end
  end

  describe "handle_info {:stream_event, proto}" do
    test "dispatches event via notify pid and tracks sequence" do
      test_pid = self()
      state = build_state(notify: test_pid, last_sequence: 0)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e1",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        timestamp: 12_345,
        sequence: 42,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, new_state} = result
      assert new_state.last_sequence == 42

      assert_receive {:kubemq_event_store, event}
      assert event.id == "e1"
      assert event.sequence == 42
    end

    test "updates last_sequence only when higher" do
      test_pid = self()
      state = build_state(notify: test_pid, last_sequence: 100)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e2",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: 0,
        sequence: 50,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, new_state} = result
      # Should keep the higher sequence (100 > 50)
      assert new_state.last_sequence == 100
    end

    test "dispatches via on_event callback using Task.Supervisor" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_event = fn event ->
        send(test_pid, {:callback_event, event.sequence})
      end

      state = build_state(on_event: on_event, task_supervisor: task_sup, last_sequence: 0)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e3",
        channel: "ch",
        metadata: "",
        body: "data",
        timestamp: 0,
        sequence: 7,
        tags: %{}
      }

      Subscriber.handle_info({:stream_event, proto_event}, state)
      assert_receive {:callback_event, 7}, 1_000
    end
  end

  describe "terminate/2" do
    test "kills recv_pid if alive" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(recv_pid: recv_pid, stream: nil)
      Subscriber.terminate(:normal, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
    end

    test "handles nil recv_pid gracefully" do
      state = build_state(recv_pid: nil, stream: nil)
      assert :ok = Subscriber.terminate(:normal, state)
    end
  end

  describe "handle_info {:stream_error, reason}" do
    test "dispatches error and triggers resubscribe" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_error = fn error ->
        send(test_pid, {:error_callback, error.code})
      end

      state = build_state(on_error: on_error, task_supervisor: task_sup)
      result = Subscriber.handle_info({:stream_error, :test_error}, state)

      assert {:noreply, ^state, {:continue, :resubscribe}} = result
      assert_receive {:error_callback, :stream_broken}, 1_000
    end
  end

  describe "handle_info catch-all" do
    test "ignores unknown messages" do
      state = build_state()
      result = Subscriber.handle_info(:unknown_message, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_info {:subscription_failed, reason}" do
    test "dispatches error and schedules retry" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_error = fn error ->
        send(test_pid, {:error_callback, error.code})
      end

      state = build_state(on_error: on_error, task_supervisor: task_sup)
      result = Subscriber.handle_info({:subscription_failed, :unavailable}, state)

      assert {:noreply, ^state} = result
      assert_receive {:error_callback, :transient}, 1_000
    end
  end

  describe "handle_info {:subscribed, pid}" do
    test "returns noreply without state change" do
      state = build_state()
      result = Subscriber.handle_info({:subscribed, self()}, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "handle_continue :resubscribe" do
    test "stops when conn is nil (connection closed)" do
      state = build_state(conn: nil)
      result = Subscriber.handle_continue(:resubscribe, state)
      assert {:stop, :connection_closed, ^state} = result
    end

    test "stops when conn process is dead (connection closed)" do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(10)

      state = build_state(conn: dead_pid)
      result = Subscriber.handle_continue(:resubscribe, state)
      assert {:stop, :connection_closed, ^state} = result
    end

    test "resumes from last_sequence+1 when last_sequence > 0" do
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :new_grpc_channel})
          end
        end)

      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:error, :unavailable}
      end)

      state = build_state(conn: conn_pid, last_sequence: 42, start_at: :start_from_first)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      # The start_at should be updated to resume from last_sequence + 1
      assert new_state.start_at == {:start_at_sequence, 43}
      assert new_state.grpc_channel == :new_grpc_channel
    end

    test "keeps original start_at when last_sequence is 0" do
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :new_grpc_channel})
          end
        end)

      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:error, :unavailable}
      end)

      state = build_state(conn: conn_pid, last_sequence: 0, start_at: :start_from_first)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      assert new_state.start_at == :start_from_first
      assert new_state.grpc_channel == :new_grpc_channel
    end
  end

  describe "dispatch_event callback exception" do
    test "on_event callback exception is caught and logged" do
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_event = fn _event ->
        raise "callback exploded"
      end

      state = build_state(on_event: on_event, task_supervisor: task_sup, last_sequence: 0)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e-err",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: 0,
        sequence: 5,
        tags: %{}
      }

      # Should not raise -- the exception is caught inside the Task
      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, new_state} = result
      assert new_state.last_sequence == 5

      # Give the task time to execute and catch the error
      Process.sleep(100)
    end
  end

  describe "dispatch_event via notify PID" do
    test "sends :kubemq_event_store message to notify pid" do
      test_pid = self()
      state = build_state(notify: test_pid, last_sequence: 0)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e-notify",
        channel: "ch",
        metadata: "m",
        body: "b",
        timestamp: 100,
        sequence: 10,
        tags: %{"k" => "v"}
      }

      Subscriber.handle_info({:stream_event, proto_event}, state)

      assert_receive {:kubemq_event_store, event}
      assert event.id == "e-notify"
      assert event.sequence == 10
    end
  end

  describe "init validates start_at" do
    test "accepts all valid start_at atoms" do
      Process.flag(:trap_exit, true)

      for start_at <- [:start_new_only, :start_from_new, :start_from_first, :start_from_last] do
        expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
          {:error, :unavailable}
        end)

        {:ok, pid} =
          Subscriber.start_link(
            channel: "ch",
            client_id: "c1",
            transport: KubeMQ.MockTransport,
            grpc_channel: :fake,
            start_at: start_at
          )

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000
      end
    end

    test "accepts tuple start_at variants" do
      Process.flag(:trap_exit, true)

      for start_at <- [
            {:start_at_sequence, 5},
            {:start_at_time, 1000},
            {:start_at_time_delta, 5000}
          ] do
        expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
          {:error, :unavailable}
        end)

        {:ok, pid} =
          Subscriber.start_link(
            channel: "ch",
            client_id: "c1",
            transport: KubeMQ.MockTransport,
            grpc_channel: :fake,
            start_at: start_at
          )

        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000
      end
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for events_store/subscriber.ex
  # ===================================================================

  describe "dispatch_error with nil on_error" do
    test "stream_error without on_error does not crash" do
      state = build_state(on_error: nil)
      result = Subscriber.handle_info({:stream_error, :test_error}, state)

      assert {:noreply, ^state, {:continue, :resubscribe}} = result
    end
  end

  describe "dispatch_error with failing on_error callback" do
    test "on_error callback exception is caught and logged" do
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_error = fn _error ->
        raise "on_error callback exploded"
      end

      state = build_state(on_error: on_error, task_supervisor: task_sup)
      result = Subscriber.handle_info({:stream_error, :test_error}, state)

      assert {:noreply, ^state, {:continue, :resubscribe}} = result
      Process.sleep(100)
    end
  end

  describe "dispatch_event with no callback and no notify" do
    test "does nothing when neither on_event nor notify is set" do
      state = build_state(on_event: nil, notify: nil)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e-noop",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: 0,
        sequence: 1,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, new_state} = result
      assert new_state.last_sequence == 1
    end
  end

  describe "handle_info {:subscription_failed, reason} without on_error" do
    test "schedules retry even without on_error callback" do
      state = build_state(on_error: nil)
      result = Subscriber.handle_info({:subscription_failed, :unavailable}, state)

      assert {:noreply, ^state} = result
      assert_receive :retry_resubscribe, 6_000
    end
  end

  describe "handle_continue :resubscribe with get_channel failure" do
    test "schedules retry when get_channel returns error" do
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:error, :not_ready})
          end
        end)

      state = build_state(conn: conn_pid)
      {:noreply, ^state} = Subscriber.handle_continue(:resubscribe, state)

      assert_receive :retry_resubscribe, 6_000
    end
  end

  describe "subscribe_and_recv and recv_loop integration" do
    test "recv_loop dispatches stream events, errors, and handles stream close" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      # Create a stream (enumerable) that yields ok events, an error, then ends
      stream = [
        {:ok,
         %KubeMQ.Proto.EventReceive{
           event_id: "e-recv-1",
           channel: "ch",
           metadata: "",
           body: "data",
           timestamp: 0,
           sequence: 1,
           tags: %{}
         }},
        {:error, :test_stream_error}
      ]

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:ok, stream}
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          start_at: :start_from_first,
          notify: test_pid
        )

      # Should receive the event from recv_loop
      assert_receive {:kubemq_event_store, event}, 2_000
      assert event.id == "e-recv-1"

      # The stream_error and stream_closed triggers resubscribe which fails
      # because there's no conn, so it should stop
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    end

    test "recv_loop rescue path sends stream_closed with exception" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      # Create a Stream.resource that raises during enumeration
      raising_stream =
        Stream.resource(
          fn -> :init end,
          fn :init -> raise "stream exploded" end,
          fn _ -> :ok end
        )

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:ok, raising_stream}
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          start_at: :start_from_first,
          notify: test_pid
        )

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    end
  end

  describe "handle_continue :subscribe kills existing recv_pid" do
    test "kills old recv_pid on initial subscribe" do
      Process.flag(:trap_exit, true)

      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:error, :unavailable}
      end)

      state = build_state(recv_pid: recv_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:subscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
    end
  end

  describe "handle_continue :resubscribe kills existing recv_pid" do
    test "kills old recv_pid before resubscribing" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :new_grpc_channel})
          end
        end)

      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:error, :unavailable}
      end)

      state = build_state(conn: conn_pid, recv_pid: recv_pid, last_sequence: 10)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
      assert new_state.start_at == {:start_at_sequence, 11}
    end
  end
end
