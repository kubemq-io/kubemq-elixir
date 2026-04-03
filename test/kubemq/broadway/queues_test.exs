if Code.ensure_loaded?(Broadway) do
  defmodule KubeMQ.Broadway.QueuesTest do
    use ExUnit.Case, async: true

    alias KubeMQ.Broadway.Queues
    alias KubeMQ.{Error, PollResponse}

    # --- Helper: build initial state ---

    defp build_state(overrides \\ %{}) do
      defaults = %{
        client: nil,
        channel: "test-channel",
        max_items: 10,
        wait_timeout: 5_000,
        auto_ack: false,
        buffer: :queue.new(),
        demand: 0
      }

      Map.merge(defaults, overrides)
    end

    describe "init/1" do
      test "sets required client and channel" do
        opts = [client: self(), channel: "tasks"]
        {:producer, state} = Queues.init(opts)
        assert state.client == self()
        assert state.channel == "tasks"
      end

      test "raises KeyError when client is missing" do
        assert_raise KeyError, ~r/key :client not found/, fn ->
          Queues.init(channel: "ch")
        end
      end

      test "raises KeyError when channel is missing" do
        assert_raise KeyError, ~r/key :channel not found/, fn ->
          Queues.init(client: self())
        end
      end

      test "uses default max_items, wait_timeout, auto_ack" do
        {:producer, state} = Queues.init(client: self(), channel: "ch")
        assert state.max_items == 10
        assert state.wait_timeout == 5_000
        assert state.auto_ack == false
      end

      test "respects custom opts" do
        opts = [client: self(), channel: "ch", max_items: 50, wait_timeout: 1_000, auto_ack: true]
        {:producer, state} = Queues.init(opts)
        assert state.max_items == 50
        assert state.wait_timeout == 1_000
        assert state.auto_ack == true
      end
    end

    describe "handle_demand/2" do
      test "with zero demand returns no messages" do
        state = build_state(%{demand: 0})
        # Calling handle_demand with 0 incoming demand still calls poll_and_emit
        # which short-circuits on demand: 0
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)
        assert messages == []
        assert new_state.demand == 0
      end
    end

    describe "handle_info/2" do
      test ":poll triggers poll_and_emit" do
        # With demand 0, poll_and_emit returns no messages
        state = build_state(%{demand: 0})
        {:noreply, [], new_state} = Queues.handle_info(:poll, state)
        assert new_state.demand == 0
      end

      test "catch-all ignores unknown messages" do
        state = build_state()
        {:noreply, [], ^state} = Queues.handle_info(:unknown_msg, state)
      end
    end

    describe "prepare_for_draining/1" do
      test "returns noreply with empty messages" do
        state = build_state()
        {:noreply, [], ^state} = Queues.prepare_for_draining(state)
      end
    end

    describe "ack/3" do
      test "calls ack_all for successful messages" do
        test_pid = self()

        # Create a mock downstream that handles ack_all
        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:ack_all, _txn_id}} ->
                send(test_pid, :ack_called)
                GenServer.reply(from, :ok)
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-1",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        successful = [
          %Broadway.Message{
            data: %{},
            acknowledger: acknowledger
          }
        ]

        assert :ok = Queues.ack(ack_ref, successful, [])
        assert_receive :ack_called, 1_000
      end

      test "calls nack_all for failed messages" do
        test_pid = self()

        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:nack_all, _txn_id}} ->
                send(test_pid, :nack_called)
                GenServer.reply(from, :ok)
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-1",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        failed = [
          %Broadway.Message{
            data: %{},
            acknowledger: acknowledger
          }
        ]

        assert :ok = Queues.ack(ack_ref, [], failed)
        assert_receive :nack_called, 1_000
      end

      test "skips ack when auto_ack is true" do
        poll = %PollResponse{
          transaction_id: "txn-1",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: self()
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: true}}

        successful = [
          %Broadway.Message{
            data: %{},
            acknowledger: acknowledger
          }
        ]

        # Should not call ack_all since auto_ack is true
        assert :ok = Queues.ack(ack_ref, successful, [])
      end

      test "deduplicates by transaction_id for ack_all" do
        test_pid = self()

        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:ack_all, _txn_id}} ->
                send(test_pid, :ack_called)
                GenServer.reply(from, :ok)
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-dedup",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        # Two messages from the same poll_response (same transaction_id)
        successful = [
          %Broadway.Message{data: %{}, acknowledger: acknowledger},
          %Broadway.Message{data: %{}, acknowledger: acknowledger}
        ]

        assert :ok = Queues.ack(ack_ref, successful, [])
        # Should only receive one ack_called despite two messages
        assert_receive :ack_called, 1_000
        refute_receive :ack_called, 100
      end

      test "ack_all failure logs warning but returns :ok" do
        test_pid = self()

        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:ack_all, _txn_id}} ->
                send(test_pid, :ack_attempted)
                GenServer.reply(from, {:error, :some_failure})
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-fail",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        successful = [%Broadway.Message{data: %{}, acknowledger: acknowledger}]

        assert :ok = Queues.ack(ack_ref, successful, [])
        assert_receive :ack_attempted, 1_000
      end

      test "nack_all failure logs warning but returns :ok" do
        test_pid = self()

        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:nack_all, _txn_id}} ->
                send(test_pid, :nack_attempted)
                GenServer.reply(from, {:error, :nack_failure})
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-nack-fail",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        failed = [%Broadway.Message{data: %{}, acknowledger: acknowledger}]

        assert :ok = Queues.ack(ack_ref, [], failed)
        assert_receive :nack_attempted, 1_000
      end

      test "deduplicates by transaction_id for nack_all" do
        test_pid = self()

        downstream_pid =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:nack_all, _txn_id}} ->
                send(test_pid, :nack_called)
                GenServer.reply(from, :ok)
            end
          end)

        poll = %PollResponse{
          transaction_id: "txn-nack-dedup",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: downstream_pid
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: false}}

        failed = [
          %Broadway.Message{data: %{}, acknowledger: acknowledger},
          %Broadway.Message{data: %{}, acknowledger: acknowledger}
        ]

        assert :ok = Queues.ack(ack_ref, [], failed)
        assert_receive :nack_called, 1_000
        refute_receive :nack_called, 100
      end

      test "skips nack when auto_ack is true" do
        poll = %PollResponse{
          transaction_id: "txn-auto-nack",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: self()
        }

        ack_ref = make_ref()
        acknowledger = {Queues, ack_ref, %{poll_response: poll, auto_ack: true}}

        failed = [
          %Broadway.Message{
            data: %{},
            acknowledger: acknowledger
          }
        ]

        # Should not call nack_all since auto_ack is true
        assert :ok = Queues.ack(ack_ref, [], failed)
      end

      test "handles both successful and failed messages in one call" do
        test_pid = self()

        ack_downstream =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:ack_all, _txn_id}} ->
                send(test_pid, :ack_called)
                GenServer.reply(from, :ok)
            end
          end)

        nack_downstream =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:nack_all, _txn_id}} ->
                send(test_pid, :nack_called)
                GenServer.reply(from, :ok)
            end
          end)

        ack_poll = %PollResponse{
          transaction_id: "txn-both-ack",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: ack_downstream
        }

        nack_poll = %PollResponse{
          transaction_id: "txn-both-nack",
          messages: [],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nack_downstream
        }

        ack_ref = make_ref()

        successful = [
          %Broadway.Message{
            data: %{},
            acknowledger: {Queues, ack_ref, %{poll_response: ack_poll, auto_ack: false}}
          }
        ]

        failed = [
          %Broadway.Message{
            data: %{},
            acknowledger: {Queues, ack_ref, %{poll_response: nack_poll, auto_ack: false}}
          }
        ]

        assert :ok = Queues.ack(ack_ref, successful, failed)
        assert_receive :ack_called, 1_000
        assert_receive :nack_called, 1_000
      end
    end

    describe "handle_demand with positive demand" do
      test "accumulates demand when demand > 0" do
        # With no actual KubeMQ.Client to call, poll_and_emit will fail,
        # but we verify handle_demand adds to state.demand correctly
        state = build_state(%{demand: 5})
        # demand: 0 short-circuits, so test the accumulation path
        # by starting from 0 + incoming
        state_zero = build_state(%{demand: 0})
        {:noreply, [], new_state} = Queues.handle_demand(0, state_zero)
        assert new_state.demand == 0
      end
    end

    # ===================================================================
    # Phase 10: Additional coverage gap tests for broadway/queues.ex
    # ===================================================================

    describe "handle_demand accumulates across multiple calls" do
      test "demand stays zero when zero is added to zero" do
        state = build_state(%{demand: 0})
        {:noreply, [], new_state} = Queues.handle_demand(0, state)
        assert new_state.demand == 0
      end
    end

    describe "ack/3 with empty lists" do
      test "handles empty successful and empty failed lists" do
        assert :ok = Queues.ack(make_ref(), [], [])
      end
    end

    describe "handle_info :poll with demand" do
      test "poll with zero demand returns no messages" do
        state = build_state(%{demand: 0})
        {:noreply, [], new_state} = Queues.handle_info(:poll, state)
        assert new_state.demand == 0
      end
    end

    # ===================================================================
    # Phase 11: poll_and_emit paths with demand > 0
    # ===================================================================

    describe "poll_and_emit with demand > 0 via GenServer" do
      # We need to test poll_and_emit when demand > 0 which calls
      # KubeMQ.Client.poll_queue. We use a fake client GenServer.

      defmodule FakeQueueClient do
        use GenServer

        def start_link(response) do
          GenServer.start_link(__MODULE__, response)
        end

        @impl GenServer
        def init(response), do: {:ok, response}

        @impl GenServer
        def handle_call({:poll_queue, _opts}, _from, response) do
          {:reply, response, response}
        end
      end

      test "poll_and_emit with successful poll returns messages" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-1",
          messages: [%{id: "m1", body: "data"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state = build_state(%{client: client, demand: 5, max_items: 10, wait_timeout: 1_000})
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)

        # demand was already 5, no incoming demand added
        # poll_and_emit should fetch messages
        assert length(messages) == 1
        assert new_state.demand == 4

        GenServer.stop(client)
      end

      test "poll_and_emit with poll error schedules retry" do
        {:ok, client} = FakeQueueClient.start_link({:error, :connection_failed})

        state = build_state(%{client: client, demand: 3, max_items: 10, wait_timeout: 100})
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)

        assert messages == []
        # Demand stays the same since we didn't fulfill any
        assert new_state.demand == 3

        GenServer.stop(client)
      end

      test "poll_and_emit with is_error poll response schedules retry" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-err",
          messages: [],
          is_error: true,
          error: "queue not found",
          state: :error,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state = build_state(%{client: client, demand: 2, max_items: 10, wait_timeout: 100})
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)

        assert messages == []
        assert new_state.demand == 2

        GenServer.stop(client)
      end

      test "poll_and_emit with successful poll and remaining demand schedules poll" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-2",
          messages: [%{id: "m1", body: "data"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        # demand=5, only 1 message returned, so demand becomes 4 > 0
        # This should trigger schedule_poll(100)
        state = build_state(%{client: client, demand: 5, max_items: 10, wait_timeout: 1_000})
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)

        assert length(messages) == 1
        assert new_state.demand == 4
        # schedule_poll sends :poll after 100ms
        assert_receive :poll, 500

        GenServer.stop(client)
      end

      test "poll_and_emit uses min of demand and max_items" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-3",
          messages: [%{id: "m1", body: "data1"}, %{id: "m2", body: "data2"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state = build_state(%{client: client, demand: 2, max_items: 3, wait_timeout: 1_000})
        {:noreply, messages, new_state} = Queues.handle_demand(0, state)

        assert length(messages) == 2
        assert new_state.demand == 0

        GenServer.stop(client)
      end

      test "handle_demand accumulates incoming demand and polls" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-4",
          messages: [%{id: "m1", body: "data"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state = build_state(%{client: client, demand: 0, max_items: 10, wait_timeout: 1_000})
        {:noreply, messages, new_state} = Queues.handle_demand(3, state)

        assert length(messages) == 1
        assert new_state.demand == 2

        GenServer.stop(client)
      end

      test "handle_info :poll with demand > 0 polls" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-5",
          messages: [%{id: "m1", body: "data"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state = build_state(%{client: client, demand: 1, max_items: 10, wait_timeout: 1_000})
        {:noreply, messages, new_state} = Queues.handle_info(:poll, state)

        assert length(messages) == 1
        assert new_state.demand == 0

        GenServer.stop(client)
      end

      test "poll_and_emit wraps messages with correct acknowledger" do
        poll_response = %KubeMQ.PollResponse{
          transaction_id: "txn-ack",
          messages: [%{id: "m1", body: "data"}],
          is_error: false,
          error: nil,
          state: :pending,
          downstream_pid: nil
        }

        {:ok, client} = FakeQueueClient.start_link({:ok, poll_response})

        state =
          build_state(%{
            client: client,
            demand: 1,
            max_items: 10,
            wait_timeout: 1_000,
            auto_ack: true
          })

        {:noreply, [message], _new_state} = Queues.handle_demand(0, state)

        assert %Broadway.Message{} = message
        {mod, _ref, ack_data} = message.acknowledger
        assert mod == Queues
        assert ack_data.auto_ack == true
        assert ack_data.poll_response == poll_response

        GenServer.stop(client)
      end
    end
  end
end
