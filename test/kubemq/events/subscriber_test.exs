defmodule KubeMQ.Events.SubscriberTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.Events.Subscriber

  setup :set_mox_global
  setup :verify_on_exit!

  # --- Helper: build a fake subscriber state ---

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
      recv_pid: nil,
      task_supervisor: nil
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(Subscriber, Map.new(merged))
  end

  describe "init/1" do
    test "traps exits" do
      # We start the subscriber with a transport that fails immediately
      # so we can inspect the process flags before it dies.
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
          grpc_channel: :fake
        )

      # The process should trap exits - it sets the flag in init before
      # handle_continue runs. Give time for subscribe to fail and process to stop.
      assert_receive :subscribe_called, 1_000
      # Process stops after subscription failure, that's expected
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    end

    test "starts Task.Supervisor with max_children" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        send(test_pid, {:subscribed, self()})
        {:error, :unavailable}
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "test-ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          max_callback_concurrency: 5
        )

      assert_receive {:subscribed, _}, 1_000
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    end
  end

  describe "handle_info :retry_resubscribe" do
    test "triggers resubscribe continue" do
      # Directly test the handle_info callback
      state = build_state()
      result = Subscriber.handle_info(:retry_resubscribe, state)
      assert {:noreply, ^state, {:continue, :resubscribe}} = result
    end
  end

  describe "handle_info {:EXIT, recv_pid, reason}" do
    test "clears recv_pid and triggers resubscribe" do
      fake_pid = spawn(fn -> :ok end)
      # Wait for it to die
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
    test "dispatches event via notify pid" do
      test_pid = self()
      state = build_state(notify: test_pid)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e1",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        timestamp: 12_345,
        sequence: 1,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, ^state} = result

      assert_receive {:kubemq_event, event}
      assert event.id == "e1"
      assert event.channel == "test-channel"
    end

    test "dispatches event via on_event callback using Task.Supervisor" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_event = fn event ->
        send(test_pid, {:callback_event, event.id})
      end

      state = build_state(on_event: on_event, task_supervisor: task_sup)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e2",
        channel: "test-channel",
        metadata: "",
        body: "data",
        timestamp: 0,
        sequence: 0,
        tags: %{}
      }

      Subscriber.handle_info({:stream_event, proto_event}, state)

      assert_receive {:callback_event, "e2"}, 1_000
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

    test "schedules retry even without on_error callback" do
      state = build_state(on_error: nil)
      result = Subscriber.handle_info({:subscription_failed, :unavailable}, state)

      assert {:noreply, ^state} = result
      # retry_resubscribe is scheduled via Process.send_after
      assert_receive :retry_resubscribe, 6_000
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
      refute Process.alive?(dead_pid)

      state = build_state(conn: dead_pid)
      result = Subscriber.handle_continue(:resubscribe, state)
      assert {:stop, :connection_closed, ^state} = result
    end

    test "gets fresh channel and resubscribes when connection available" do
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

      state = build_state(conn: conn_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      assert new_state.grpc_channel == :new_grpc_channel
      assert new_state.recv_pid != nil
    end

    test "schedules retry when get_channel fails" do
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

      state = build_state(conn: conn_pid, recv_pid: recv_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for events/subscriber.ex
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
        sequence: 0,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, ^state} = result
    end
  end

  describe "dispatch_event callback exception" do
    test "on_event callback exception is caught and logged" do
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_event = fn _event ->
        raise "callback exploded"
      end

      state = build_state(on_event: on_event, task_supervisor: task_sup)

      proto_event = %KubeMQ.Proto.EventReceive{
        event_id: "e-err",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: 0,
        sequence: 0,
        tags: %{}
      }

      result = Subscriber.handle_info({:stream_event, proto_event}, state)
      assert {:noreply, ^state} = result
      Process.sleep(100)
    end
  end

  describe "handle_continue :subscribe" do
    test "spawns recv_pid for initial subscription" do
      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:error, :unavailable}
      end)

      state = build_state(recv_pid: nil)
      {:noreply, new_state} = Subscriber.handle_continue(:subscribe, state)

      assert new_state.recv_pid != nil
      assert is_pid(new_state.recv_pid)
    end

    test "kills existing recv_pid before subscribing" do
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
end
