defmodule KubeMQ.Queries.SubscriberTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Queries.Subscriber
  alias KubeMQ.{QueryReceive, QueryReply}

  # A stub transport that captures send_response calls
  defmodule StubTransport do
    @behaviour KubeMQ.Transport

    def ping(_channel), do: {:ok, %{}}
    def send_event(_ch, _req), do: :ok
    def send_event_stream(_ch), do: {:error, :not_implemented}
    def send_request(_ch, _req), do: {:error, :not_implemented}

    def send_response(_ch, response) do
      # Send response to the test process via the process dictionary
      case Process.get(:test_pid) do
        nil -> :ok
        pid -> send(pid, {:response_sent, _ch, response})
      end

      :ok
    end

    def subscribe(_ch, _req), do: {:error, :not_implemented}
    def send_queue_message(_ch, _req), do: {:error, :not_implemented}
    def send_queue_messages_batch(_ch, _req), do: {:error, :not_implemented}
    def receive_queue_messages(_ch, _req), do: {:error, :not_implemented}
    def ack_all_queue_messages(_ch, _req), do: {:error, :not_implemented}
    def queue_upstream(_ch), do: {:error, :not_implemented}
    def queue_downstream(_ch), do: {:error, :not_implemented}
  end

  # A transport that captures calls and notifies the test process
  defmodule NotifyTransport do
    @behaviour KubeMQ.Transport

    def ping(_channel), do: {:ok, %{}}
    def send_event(_ch, _req), do: :ok
    def send_event_stream(_ch), do: {:error, :not_implemented}
    def send_request(_ch, _req), do: {:error, :not_implemented}

    def send_response(ch, response) do
      # Use a named ETS table to find the test pid
      case :persistent_term.get(:queries_sub_test_pid, nil) do
        nil -> :ok
        pid -> send(pid, {:response_sent, ch, response})
      end

      :ok
    end

    def subscribe(_ch, _req), do: {:error, :not_implemented}
    def send_queue_message(_ch, _req), do: {:error, :not_implemented}
    def send_queue_messages_batch(_ch, _req), do: {:error, :not_implemented}
    def receive_queue_messages(_ch, _req), do: {:error, :not_implemented}
    def ack_all_queue_messages(_ch, _req), do: {:error, :not_implemented}
    def queue_upstream(_ch), do: {:error, :not_implemented}
    def queue_downstream(_ch), do: {:error, :not_implemented}
  end

  defp build_state(overrides \\ []) do
    defaults = [
      channel: "test-channel",
      group: "",
      client_id: "test-client",
      transport: StubTransport,
      grpc_channel: :fake_grpc_channel,
      stream: nil,
      on_query: fn _q -> %QueryReply{executed: true} end,
      on_error: nil,
      conn: nil,
      recv_pid: nil,
      task_supervisor: nil
    ]

    merged = Keyword.merge(defaults, overrides)
    struct!(Subscriber, Map.new(merged))
  end

  describe "fresh channel for responses" do
    test "handle_query_and_respond gets fresh channel from connection" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        %QueryReply{executed: true, body: "result"}
      end

      # Simulate a connection process that responds to get_channel
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fresh_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-1",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, channel, response_map}, 2_000
      # The fresh channel should be used (from connection.get_channel)
      assert channel == :fresh_grpc_channel
      assert response_map.executed == true
      assert response_map.request_id == "q-1"
      assert response_map.reply_channel == "reply-ch"
    end
  end

  describe "sanitized error response" do
    test "exception in on_query sends sanitized error, not raw exception" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        raise "secret: password=abc123"
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-err",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, _ch, response_map}, 2_000
      # G13: Must be sanitized, NOT containing the actual exception message
      assert response_map.error == "internal callback error"
      assert response_map.executed == false
      refute String.contains?(response_map.error, "password")
    end
  end

  describe "terminate cleanup" do
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

  describe "handle_info {:stream_request, proto_request} - non-QueryReply" do
    test "non-QueryReply return produces error reply" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        :not_a_reply
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-bad",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, _ch, response_map}, 2_000
      assert response_map.executed == false
      assert response_map.error == "invalid reply type from on_query callback"
      assert response_map.request_id == "q-bad"
    end
  end

  describe "handle_info {:stream_request, proto_request} - dispatches to on_query" do
    test "dispatches QueryReceive to on_query and sends response" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn query ->
        send(test_pid, {:query_received, query.id, query.channel})
        %QueryReply{executed: true, body: "answer"}
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-dispatch",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{"k" => "v"}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:query_received, "q-dispatch", "test-channel"}, 2_000
      assert_receive {:response_sent, _ch, response_map}, 2_000
      assert response_map.executed == true
      assert response_map.request_id == "q-dispatch"
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

  describe "handle_info {:stream_closed, reason}" do
    test "clears recv_pid and triggers resubscribe" do
      state = build_state(recv_pid: self())
      result = Subscriber.handle_info({:stream_closed, :eof}, state)

      assert {:noreply, new_state, {:continue, :resubscribe}} = result
      assert new_state.recv_pid == nil
    end
  end

  describe "handle_continue :resubscribe with live connection" do
    test "gets fresh channel and resubscribes when connection available" do
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :new_grpc_channel})
          end
        end)

      state = build_state(conn: conn_pid, transport: StubTransport)
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

    test "kills existing recv_pid before resubscribing" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :new_grpc_channel})
          end
        end)

      state = build_state(conn: conn_pid, recv_pid: recv_pid, transport: StubTransport)
      {:noreply, new_state} = Subscriber.handle_continue(:resubscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
    end
  end

  describe "handle_continue :subscribe" do
    test "spawns recv_pid for initial subscription" do
      state = build_state(transport: StubTransport, recv_pid: nil)
      {:noreply, new_state} = Subscriber.handle_continue(:subscribe, state)

      assert new_state.recv_pid != nil
      assert is_pid(new_state.recv_pid)
    end

    test "kills existing recv_pid before subscribing" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(transport: StubTransport, recv_pid: recv_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:subscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
    end
  end

  describe "dispatch_error with nil on_error" do
    test "stream_error without on_error does not crash" do
      state = build_state(on_error: nil)
      result = Subscriber.handle_info({:stream_error, :test_error}, state)

      assert {:noreply, ^state, {:continue, :resubscribe}} = result
    end
  end

  describe "handle_query_and_respond with conn get_channel failure" do
    test "falls back to state grpc_channel when get_channel fails" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        %QueryReply{executed: true, body: "result"}
      end

      # Connection that returns error
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:error, :not_ready})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-fallback",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, channel, response_map}, 2_000
      # Should fallback to state.grpc_channel
      assert channel == :fake_grpc_channel
      assert response_map.executed == true
    end
  end

  describe "handle_query_and_respond with send_response failure" do
    defmodule FailSendTransport do
      @behaviour KubeMQ.Transport

      def ping(_channel), do: {:ok, %{}}
      def send_event(_ch, _req), do: :ok
      def send_event_stream(_ch), do: {:error, :not_implemented}
      def send_request(_ch, _req), do: {:error, :not_implemented}

      def send_response(_ch, _response) do
        case :persistent_term.get(:queries_sub_test_pid, nil) do
          nil -> :ok
          pid -> send(pid, :send_response_attempted)
        end

        {:error, :connection_lost}
      end

      def subscribe(_ch, _req), do: {:error, :not_implemented}
      def send_queue_message(_ch, _req), do: {:error, :not_implemented}
      def send_queue_messages_batch(_ch, _req), do: {:error, :not_implemented}
      def receive_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def ack_all_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def queue_upstream(_ch), do: {:error, :not_implemented}
      def queue_downstream(_ch), do: {:error, :not_implemented}
    end

    test "send_response failure is logged but does not crash" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        %QueryReply{executed: true}
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: FailSendTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-send-fail",
        channel: "test-channel",
        metadata: "",
        body: "body",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive :send_response_attempted, 2_000
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests
  # ===================================================================

  describe "handle_query_and_respond exception with conn get_channel failure" do
    test "rescue path falls back to state grpc_channel when get_channel fails" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      on_query = fn _q ->
        raise "intentional error"
      end

      # Connection that returns error for the rescue-path get_channel
      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:error, :not_ready})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-rescue-fallback",
        channel: "test-channel",
        metadata: "",
        body: "",
        reply_channel: "reply-ch",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, channel, response_map}, 2_000
      # Should fallback to state.grpc_channel in rescue path
      assert channel == :fake_grpc_channel
      assert response_map.error == "internal callback error"
      assert response_map.executed == false
    end
  end

  describe "dispatch_error with on_error callback that raises" do
    test "on_error callback exception is caught and logged" do
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      on_error = fn _error ->
        raise "on_error callback exploded"
      end

      state = build_state(on_error: on_error, task_supervisor: task_sup)
      # This exercises dispatch_error's rescue path
      result = Subscriber.handle_info({:stream_error, :test_error}, state)

      assert {:noreply, ^state, {:continue, :resubscribe}} = result
      # Give the task time to execute and catch the error
      Process.sleep(100)
    end
  end

  describe "handle_continue :subscribe with existing alive recv_pid" do
    defmodule SubscribeOkTransport do
      @behaviour KubeMQ.Transport

      def ping(_channel), do: {:ok, %{}}
      def send_event(_ch, _req), do: :ok
      def send_event_stream(_ch), do: {:error, :not_implemented}
      def send_request(_ch, _req), do: {:error, :not_implemented}
      def send_response(_ch, _resp), do: :ok

      def subscribe(_ch, _req) do
        # Return error so recv loop ends quickly
        {:error, :unavailable}
      end

      def send_queue_message(_ch, _req), do: {:error, :not_implemented}
      def send_queue_messages_batch(_ch, _req), do: {:error, :not_implemented}
      def receive_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def ack_all_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def queue_upstream(_ch), do: {:error, :not_implemented}
      def queue_downstream(_ch), do: {:error, :not_implemented}
    end

    test "kills old recv_pid and spawns new one when subscribing with alive recv_pid" do
      recv_pid = spawn(fn -> Process.sleep(:infinity) end)
      assert Process.alive?(recv_pid)

      state = build_state(transport: SubscribeOkTransport, recv_pid: recv_pid)
      {:noreply, new_state} = Subscriber.handle_continue(:subscribe, state)

      Process.sleep(50)
      refute Process.alive?(recv_pid)
      assert new_state.recv_pid != recv_pid
      assert new_state.recv_pid != nil
    end
  end

  describe "subscribe_and_recv and recv_loop integration" do
    defmodule StreamTransport do
      @behaviour KubeMQ.Transport

      def ping(_channel), do: {:ok, %{}}
      def send_event(_ch, _req), do: :ok
      def send_event_stream(_ch), do: {:error, :not_implemented}
      def send_request(_ch, _req), do: {:error, :not_implemented}

      def send_response(_ch, response) do
        case :persistent_term.get(:queries_sub_test_pid, nil) do
          nil -> :ok
          pid -> send(pid, {:response_sent, _ch, response})
        end

        :ok
      end

      def subscribe(_ch, _req) do
        stream = :persistent_term.get(:queries_sub_test_stream, {:error, :not_configured})
        stream
      end

      def send_queue_message(_ch, _req), do: {:error, :not_implemented}
      def send_queue_messages_batch(_ch, _req), do: {:error, :not_implemented}
      def receive_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def ack_all_queue_messages(_ch, _req), do: {:error, :not_implemented}
      def queue_upstream(_ch), do: {:error, :not_implemented}
      def queue_downstream(_ch), do: {:error, :not_implemented}
    end

    test "recv_loop dispatches stream requests and handles close" do
      Process.flag(:trap_exit, true)
      test_pid = self()

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      stream = [
        {:ok,
         %KubeMQ.Proto.Request{
           request_id: "q-recv-1",
           channel: "ch",
           metadata: "",
           body: "data",
           reply_channel: "reply-ch",
           tags: %{}
         }},
        {:error, :test_stream_error}
      ]

      :persistent_term.put(:queries_sub_test_stream, {:ok, stream})

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
        :persistent_term.erase(:queries_sub_test_stream)
      end)

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          on_query: fn _q -> %QueryReply{executed: true} end,
          transport: StreamTransport,
          grpc_channel: :fake,
          conn: conn_pid
        )

      assert_receive {:response_sent, _ch, response_map}, 3_000
      assert response_map.executed == true

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

      :persistent_term.put(:queries_sub_test_stream, {:ok, raising_stream})

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_stream)
      end)

      {:ok, pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          on_query: fn _q -> %QueryReply{executed: true} end,
          transport: StreamTransport,
          grpc_channel: :fake
        )

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    end
  end

  describe "handle_query_and_respond fills in default reply fields" do
    test "sets request_id, response_to, client_id from query when nil" do
      test_pid = self()
      {:ok, task_sup} = Task.Supervisor.start_link(max_children: 10)

      :persistent_term.put(:queries_sub_test_pid, test_pid)

      on_exit(fn ->
        :persistent_term.erase(:queries_sub_test_pid)
      end)

      # Return a QueryReply with nil request_id, response_to, client_id
      on_query = fn _q ->
        %QueryReply{
          executed: true,
          body: "answer",
          request_id: nil,
          response_to: nil,
          client_id: nil
        }
      end

      conn_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_channel} ->
              GenServer.reply(from, {:ok, :fake_grpc_channel})
          end
        end)

      state =
        build_state(
          on_query: on_query,
          task_supervisor: task_sup,
          conn: conn_pid,
          transport: NotifyTransport
        )

      proto_request = %KubeMQ.Proto.Request{
        request_id: "q-defaults",
        channel: "test-channel",
        metadata: "meta",
        body: "body",
        reply_channel: "reply-ch-defaults",
        tags: %{}
      }

      Subscriber.handle_info({:stream_request, proto_request}, state)

      assert_receive {:response_sent, _ch, response_map}, 2_000
      assert response_map.executed == true
      assert response_map.request_id == "q-defaults"
      assert response_map.reply_channel == "reply-ch-defaults"
      assert response_map.client_id == "test-client"
    end
  end
end
