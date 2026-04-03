defmodule KubeMQ.Commands.SubscriberTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{CommandReceive, CommandReply}
  alias KubeMQ.Commands.Subscriber

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
      on_command: fn _cmd -> %CommandReply{executed: true} end,
      on_error: nil,
      conn: nil,
      recv_pid: nil,
      task_supervisor: nil
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(Subscriber, Map.new(merged))
  end

  describe "init/1" do
    test "traps exits" do
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
          on_command: fn _cmd -> %CommandReply{executed: true} end,
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake
        )

      assert_receive :subscribe_called, 1_000
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
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

  describe "handle_info {:stream_request, proto_request} - G13 sanitization" do
    test "exception in on_command sends sanitized error response" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      # Callback that raises an exception with sensitive info
      on_command = fn _cmd ->
        raise "secret database password: abc123"
      end

      # Mock get_channel and send_response
      # We use a conn that returns a channel
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn _ch, response_map ->
        send(test_pid, {:response_sent, response_map})
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "req-1",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, response_map}, 2_000

      # G13: The error in the response must be "internal callback error"
      # NOT the actual exception message which could leak secrets
      assert response_map.error == "internal callback error"
      assert response_map.executed == false
      assert response_map.request_id == "req-1"
      assert response_map.reply_channel == "reply-ch"
    end

    test "valid CommandReply is forwarded with correct fields" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn _cmd ->
        %CommandReply{executed: true}
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn _ch, response_map ->
        send(test_pid, {:response_sent, response_map})
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "req-2",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch-2",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, response_map}, 2_000
      assert response_map.executed == true
      assert response_map.request_id == "req-2"
      assert response_map.reply_channel == "reply-ch-2"
    end

    test "non-CommandReply return produces error reply" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn _cmd ->
        :not_a_reply
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn _ch, response_map ->
        send(test_pid, {:response_sent, response_map})
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "req-3",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch-3",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, response_map}, 2_000
      assert response_map.executed == false
      assert response_map.error == "invalid reply type from on_command callback"
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

      # retry_resubscribe is scheduled via Process.send_after
      assert_receive :retry_resubscribe, 6_000
    end
  end

  describe "handle_info {:stream_request, proto_request} - dispatches to on_command" do
    test "dispatches CommandReceive to on_command callback" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn cmd ->
        send(test_pid, {:command_received, cmd.id, cmd.channel})
        %CommandReply{executed: true}
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      stub(KubeMQ.MockTransport, :send_response, fn _ch, _response_map ->
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "cmd-1",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{"key" => "val"}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:command_received, "cmd-1", "test-channel"}, 2_000
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for commands/subscriber.ex
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

  describe "handle_info {:subscription_failed, reason} without on_error" do
    test "schedules retry even without on_error callback" do
      state = build_state(on_error: nil)
      result = Subscriber.handle_info({:subscription_failed, :unavailable}, state)

      assert {:noreply, ^state} = result
      assert_receive :retry_resubscribe, 6_000
    end
  end

  describe "handle_command_and_respond get_channel fallback" do
    test "falls back to state grpc_channel when get_channel fails" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn _cmd ->
        %CommandReply{executed: true, body: "ok"}
      end

      # Connection that returns error
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:error, :not_ready})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn ch, response_map ->
        send(test_pid, {:response_sent, ch, response_map})
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "cmd-fallback",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, channel, response_map}, 2_000
      assert channel == :fake_grpc_channel
      assert response_map.executed == true
    end
  end

  describe "handle_command_and_respond rescue with get_channel fallback" do
    test "rescue path falls back to state grpc_channel when get_channel fails" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn _cmd ->
        raise "intentional error"
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:error, :not_ready})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn ch, response_map ->
        send(test_pid, {:response_sent, ch, response_map})
        :ok
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "cmd-rescue-fallback",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, channel, response_map}, 2_000
      assert channel == :fake_grpc_channel
      assert response_map.error == "internal callback error"
      assert response_map.executed == false
    end
  end

  describe "handle_command_and_respond send_response failure" do
    test "send_response failure is logged but does not crash" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_command = fn _cmd ->
        %CommandReply{executed: true}
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      expect(KubeMQ.MockTransport, :send_response, fn _ch, _response_map ->
        send(test_pid, :send_response_attempted)
        {:error, :connection_lost}
      end)

      state =
        build_state(
          on_command: on_command,
          task_supervisor: task_sup,
          conn: conn_pid
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "cmd-send-fail",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive :send_response_attempted, 2_000
    end
  end

  describe "subscribe_and_recv and recv_loop integration" do
    test "recv_loop dispatches stream requests, errors, and handles stream close" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      stream = [
        {:ok,
         %KubeMQ.Proto.Request{
           request_id: "cmd-recv-1",
           channel: "ch",
           metadata: "",
           body: "data",
           reply_channel: "reply-ch",
           tags: %{}
         }},
        {:error, :test_stream_error}
      ]

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:ok, stream}
      end)

      # Need a conn for the response sending
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      stub(KubeMQ.MockTransport, :send_response, fn _ch, _resp ->
        send(test_pid, :response_sent)
        :ok
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          on_command: fn _cmd -> %CommandReply{executed: true} end,
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake,
          conn: conn_pid
        )

      # Should receive response_sent from the on_command handler
      assert_receive :response_sent, 3_000

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    end

    test "recv_loop rescue path sends stream_closed with exception" do
      Process.flag(:trap_exit, true)

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
          on_command: fn _cmd -> %CommandReply{executed: true} end,
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake
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

      state = build_state(conn: conn_pid, recv_pid: recv_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
    end
  end
end
