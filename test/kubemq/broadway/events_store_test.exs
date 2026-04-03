if Code.ensure_loaded?(Broadway) do
  defmodule KubeMQ.Broadway.EventsStoreTest do
    use ExUnit.Case, async: true

    alias KubeMQ.Broadway.EventsStore

    # --- Helper: build initial state ---

    defp build_state(overrides \\ %{}) do
      defaults = %{
        client: nil,
        channel: "test-channel",
        group: "",
        start_at: :start_from_first,
        subscription: nil,
        sub_ref: nil,
        buffer: :queue.new(),
        buffer_size: 0,
        max_buffer_size: 10_000,
        demand: 0
      }

      Map.merge(defaults, overrides)
    end

    describe "init/1 deferred subscribe" do
      test "deferred init subscribes via handle_continue" do
        opts = [client: self(), channel: "test-ch", start_at: :start_from_first]
        assert {:producer, state, {:continue, :subscribe}} = EventsStore.init(opts)
        assert state.client == self()
        assert state.channel == "test-ch"
        assert state.start_at == :start_from_first
        assert state.subscription == nil
      end

      test "default max_buffer_size is 10_000" do
        opts = [client: self(), channel: "ch", start_at: :start_from_first]
        {:producer, state, _} = EventsStore.init(opts)
        assert state.max_buffer_size == 10_000
      end

      test "custom max_buffer_size is respected" do
        opts = [client: self(), channel: "ch", start_at: :start_from_first, max_buffer_size: 500]
        {:producer, state, _} = EventsStore.init(opts)
        assert state.max_buffer_size == 500
      end
    end

    describe "handle_info {:kubemq_event_store, event} respects max_buffer_size" do
      test "accepts event when buffer not full" do
        state = build_state(%{buffer_size: 0, max_buffer_size: 100, demand: 0})

        result = EventsStore.handle_info({:kubemq_event_store, :some_event}, state)
        assert {:noreply, [], new_state} = result
        assert new_state.buffer_size == 1
      end

      test "drops event when buffer is full" do
        state = build_state(%{buffer_size: 10, max_buffer_size: 10, demand: 0})

        result = EventsStore.handle_info({:kubemq_event_store, :some_event}, state)
        assert {:noreply, [], new_state} = result
        # Buffer size should remain the same (event dropped)
        assert new_state.buffer_size == 10
      end
    end

    describe "handle_demand drain_buffer" do
      test "drain_buffer delivers events when demand exists" do
        buffer = :queue.in(:event1, :queue.in(:event2, :queue.new()))
        state = build_state(%{buffer: buffer, buffer_size: 2, demand: 0})

        result = EventsStore.handle_demand(2, state)
        assert {:noreply, messages, new_state} = result
        assert length(messages) == 2
        assert new_state.buffer_size == 0
        assert new_state.demand == 0
      end

      test "empty buffer returns no messages" do
        state = build_state(%{buffer: :queue.new(), buffer_size: 0, demand: 0})

        result = EventsStore.handle_demand(5, state)
        assert {:noreply, [], new_state} = result
        assert new_state.demand == 5
      end
    end

    describe "handle_info {:DOWN, ...} monitors subscription" do
      test "monitors subscription and resubscribes on death" do
        ref = make_ref()
        state = build_state(%{sub_ref: ref, subscription: %{pid: self()}})

        result = EventsStore.handle_info({:DOWN, ref, :process, self(), :normal}, state)

        assert {:noreply, [], new_state} = result
        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
      end
    end

    describe "handle_info :retry_subscribe" do
      test "triggers subscribe continue" do
        state = build_state()
        result = EventsStore.handle_info(:retry_subscribe, state)
        assert {:noreply, [], ^state, {:continue, :subscribe}} = result
      end
    end

    describe "handle_info catch-all" do
      test "ignores unknown messages" do
        state = build_state()
        result = EventsStore.handle_info(:unknown, state)
        assert {:noreply, [], ^state} = result
      end
    end

    describe "ack/3" do
      test "is a no-op (events store are fire-and-forget for consumer)" do
        assert :ok = EventsStore.ack(:ref, [], [])
      end
    end

    describe "init/1 missing required opts" do
      test "raises KeyError when start_at is missing" do
        assert_raise KeyError, ~r/key :start_at not found/, fn ->
          EventsStore.init(client: self(), channel: "ch")
        end
      end

      test "default group is empty string" do
        opts = [client: self(), channel: "ch", start_at: :start_from_first]
        {:producer, state, _} = EventsStore.init(opts)
        assert state.group == ""
      end
    end

    describe "handle_info {:kubemq_event_store, event} drains when demand exists" do
      test "wraps event into Broadway.Message with correct acknowledger" do
        state = build_state(%{buffer_size: 0, max_buffer_size: 100, demand: 2})

        {:noreply, messages, new_state} =
          EventsStore.handle_info({:kubemq_event_store, :store_event_a}, state)

        assert length(messages) == 1
        msg = hd(messages)
        assert msg.data == :store_event_a
        assert {KubeMQ.Broadway.EventsStore, :ack_id, :ack_data} = msg.acknowledger
        assert new_state.demand == 1
        assert new_state.buffer_size == 0
      end
    end

    describe "prepare_for_draining/1" do
      test "cancels active subscription" do
        sub_pid = spawn(fn -> Process.sleep(10_000) end)
        sub = %KubeMQ.Subscription{pid: sub_pid, ref: make_ref()}
        state = build_state(%{subscription: sub, sub_ref: make_ref()})

        {:noreply, [], new_state} = EventsStore.prepare_for_draining(state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
        refute Process.alive?(sub_pid)
      end

      test "handles nil subscription gracefully" do
        state = build_state(%{subscription: nil, sub_ref: nil})

        {:noreply, [], new_state} = EventsStore.prepare_for_draining(state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
      end
    end

    describe "handle_info {:DOWN, ...} ignores non-matching ref" do
      test "catch-all handles non-subscription DOWN" do
        state = build_state(%{sub_ref: make_ref()})
        other_ref = make_ref()

        result = EventsStore.handle_info({:DOWN, other_ref, :process, self(), :normal}, state)
        assert {:noreply, [], ^state} = result
      end
    end

    describe "handle_demand accumulates demand" do
      test "partial drain when demand < buffer_size" do
        buffer =
          :queue.in(:event3, :queue.in(:event2, :queue.in(:event1, :queue.new())))

        state = build_state(%{buffer: buffer, buffer_size: 3, demand: 0})

        {:noreply, messages, new_state} = EventsStore.handle_demand(1, state)
        assert length(messages) == 1
        assert new_state.buffer_size == 2
        assert new_state.demand == 0
      end
    end

    # ===================================================================
    # Phase 10: Additional coverage gap tests for broadway/events_store.ex
    # ===================================================================

    describe "drain_buffer edge cases" do
      test "drain_buffer with zero demand returns nothing" do
        buffer = :queue.in(:event1, :queue.new())
        state = build_state(%{buffer: buffer, buffer_size: 1, demand: 0})

        {:noreply, messages, new_state} = EventsStore.handle_demand(0, state)
        assert messages == []
        assert new_state.demand == 0
        assert new_state.buffer_size == 1
      end
    end

    describe "handle_info {:kubemq_event_store, event} buffer full with demand" do
      test "drops event even when demand exists if buffer is full" do
        state = build_state(%{buffer_size: 5, max_buffer_size: 5, demand: 10})

        result = EventsStore.handle_info({:kubemq_event_store, :overflow_event}, state)
        assert {:noreply, [], new_state} = result
        assert new_state.buffer_size == 5
      end
    end

    describe "ack/3 with messages" do
      test "is a no-op even with successful and failed messages" do
        msg = %Broadway.Message{
          data: :some_event,
          acknowledger: {EventsStore, :ack_id, :ack_data}
        }

        assert :ok = EventsStore.ack(:ref, [msg], [msg])
      end
    end

    describe "handle_demand accumulates across multiple calls" do
      test "accumulates demand correctly" do
        state = build_state(%{demand: 0})

        {:noreply, [], state1} = EventsStore.handle_demand(3, state)
        assert state1.demand == 3

        {:noreply, [], state2} = EventsStore.handle_demand(2, state1)
        assert state2.demand == 5
      end
    end

    # ===================================================================
    # Phase 11: handle_continue :subscribe paths for broadway/events_store.ex
    # ===================================================================

    describe "handle_continue :subscribe success" do
      defmodule FakeEventsStoreClient do
        use GenServer

        def start_link(response) do
          GenServer.start_link(__MODULE__, response)
        end

        @impl GenServer
        def init(response), do: {:ok, response}

        @impl GenServer
        def handle_call({:subscribe_to_events_store, _channel, _opts}, _from, response) do
          {:reply, response, response}
        end
      end

      test "successful subscribe sets subscription and monitors" do
        sub_pid = spawn(fn -> Process.sleep(:infinity) end)
        sub = %KubeMQ.Subscription{pid: sub_pid, ref: make_ref()}
        {:ok, client} = FakeEventsStoreClient.start_link({:ok, sub})

        state =
          build_state(%{
            client: client,
            subscription: nil,
            sub_ref: nil,
            start_at: :start_from_first
          })

        {:noreply, [], new_state} = EventsStore.handle_continue(:subscribe, state)

        assert new_state.subscription == sub
        assert new_state.sub_ref != nil

        Process.exit(sub_pid, :kill)
        GenServer.stop(client)
      end

      test "failed subscribe schedules retry" do
        {:ok, client} =
          FakeEventsStoreClient.start_link(
            {:error, %KubeMQ.Error{code: :unavailable, message: "not ready"}}
          )

        state = build_state(%{client: client, subscription: nil, start_at: :start_from_first})
        {:noreply, [], new_state} = EventsStore.handle_continue(:subscribe, state)

        assert new_state.subscription == nil
        assert_receive :retry_subscribe, 6_000

        GenServer.stop(client)
      end
    end

    describe "handle_info {:DOWN, ref, ...} matching subscription triggers resubscribe" do
      test "matching DOWN clears subscription and schedules retry" do
        ref = make_ref()
        state = build_state(%{sub_ref: ref, subscription: %{pid: self()}})

        {:noreply, [], new_state} =
          EventsStore.handle_info({:DOWN, ref, :process, self(), :normal}, state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
        assert_receive :retry_subscribe, 6_000
      end
    end
  end
end
