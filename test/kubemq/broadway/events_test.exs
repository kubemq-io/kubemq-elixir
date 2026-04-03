if Code.ensure_loaded?(Broadway) do
  defmodule KubeMQ.Broadway.EventsTest do
    use ExUnit.Case, async: true

    alias KubeMQ.Broadway.Events

    # --- Helper: build initial state ---

    defp build_state(overrides \\ %{}) do
      defaults = %{
        client: nil,
        channel: "test-channel",
        group: "",
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
        # init returns {:producer, state, {:continue, :subscribe}}
        opts = [client: self(), channel: "test-ch"]
        assert {:producer, state, {:continue, :subscribe}} = Events.init(opts)
        assert state.client == self()
        assert state.channel == "test-ch"
        assert state.subscription == nil
      end

      test "default max_buffer_size is 10_000" do
        opts = [client: self(), channel: "ch"]
        {:producer, state, _} = Events.init(opts)
        assert state.max_buffer_size == 10_000
      end

      test "custom max_buffer_size is respected" do
        opts = [client: self(), channel: "ch", max_buffer_size: 500]
        {:producer, state, _} = Events.init(opts)
        assert state.max_buffer_size == 500
      end
    end

    describe "handle_info {:DOWN, ...} monitors subscription" do
      test "monitors subscription and resubscribes on death" do
        ref = make_ref()
        state = build_state(%{sub_ref: ref, subscription: %{pid: self()}})

        result = Events.handle_info({:DOWN, ref, :process, self(), :normal}, state)

        assert {:noreply, [], new_state} = result
        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
      end

      test "ignores DOWN for non-subscription refs" do
        state = build_state(%{sub_ref: make_ref()})
        other_ref = make_ref()

        result = Events.handle_info({:DOWN, other_ref, :process, self(), :normal}, state)

        # Should hit the catch-all handler
        assert {:noreply, [], ^state} = result
      end
    end

    describe "handle_info {:kubemq_event, event} respects max_buffer_size" do
      test "accepts event when buffer not full" do
        state = build_state(%{buffer_size: 0, max_buffer_size: 100, demand: 0})

        result = Events.handle_info({:kubemq_event, :some_event}, state)
        assert {:noreply, [], new_state} = result
        assert new_state.buffer_size == 1
      end

      test "drops event when buffer is full" do
        state = build_state(%{buffer_size: 10, max_buffer_size: 10, demand: 0})

        result = Events.handle_info({:kubemq_event, :some_event}, state)
        assert {:noreply, [], new_state} = result
        # Buffer size should remain the same (event dropped)
        assert new_state.buffer_size == 10
      end
    end

    describe "handle_demand drain_buffer" do
      test "drain_buffer delivers events when demand exists" do
        buffer = :queue.in(:event1, :queue.in(:event2, :queue.new()))
        state = build_state(%{buffer: buffer, buffer_size: 2, demand: 0})

        # Request 2 items
        result = Events.handle_demand(2, state)
        assert {:noreply, messages, new_state} = result
        assert length(messages) == 2
        assert new_state.buffer_size == 0
        assert new_state.demand == 0
      end

      test "partial drain when demand < buffer_size" do
        buffer =
          :queue.in(:event3, :queue.in(:event2, :queue.in(:event1, :queue.new())))

        state = build_state(%{buffer: buffer, buffer_size: 3, demand: 0})

        result = Events.handle_demand(1, state)
        assert {:noreply, messages, new_state} = result
        assert length(messages) == 1
        assert new_state.buffer_size == 2
        assert new_state.demand == 0
      end

      test "empty buffer returns no messages" do
        state = build_state(%{buffer: :queue.new(), buffer_size: 0, demand: 0})

        result = Events.handle_demand(5, state)
        assert {:noreply, [], new_state} = result
        assert new_state.demand == 5
      end
    end

    describe "handle_info :retry_subscribe" do
      test "triggers subscribe continue" do
        state = build_state()
        result = Events.handle_info(:retry_subscribe, state)
        assert {:noreply, [], ^state, {:continue, :subscribe}} = result
      end
    end

    describe "handle_info catch-all" do
      test "ignores unknown messages" do
        state = build_state()
        result = Events.handle_info(:unknown, state)
        assert {:noreply, [], ^state} = result
      end
    end

    describe "ack/3" do
      test "is a no-op (events are fire-and-forget)" do
        assert :ok = Events.ack(:ref, [], [])
      end
    end

    describe "init/1 missing required opts" do
      test "raises KeyError when client is missing" do
        assert_raise KeyError, ~r/key :client not found/, fn ->
          Events.init(channel: "ch")
        end
      end

      test "raises KeyError when channel is missing" do
        assert_raise KeyError, ~r/key :channel not found/, fn ->
          Events.init(client: self())
        end
      end

      test "default group is empty string" do
        opts = [client: self(), channel: "ch"]
        {:producer, state, _} = Events.init(opts)
        assert state.group == ""
      end
    end

    describe "handle_info {:kubemq_event, event} drains when demand exists" do
      test "wraps event into Broadway.Message and fulfills demand" do
        state = build_state(%{buffer_size: 0, max_buffer_size: 100, demand: 2})

        {:noreply, messages, new_state} =
          Events.handle_info({:kubemq_event, :event_a}, state)

        assert length(messages) == 1
        assert hd(messages).data == :event_a
        assert new_state.demand == 1
        assert new_state.buffer_size == 0
      end
    end

    describe "prepare_for_draining/1" do
      test "cancels active subscription" do
        # Spawn a process that stays alive long enough to be cancelled
        sub_pid = spawn(fn -> Process.sleep(10_000) end)
        sub = %KubeMQ.Subscription{pid: sub_pid, ref: make_ref()}
        state = build_state(%{subscription: sub, sub_ref: make_ref()})

        {:noreply, [], new_state} = Events.prepare_for_draining(state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
        refute Process.alive?(sub_pid)
      end

      test "handles nil subscription gracefully" do
        state = build_state(%{subscription: nil, sub_ref: nil})

        {:noreply, [], new_state} = Events.prepare_for_draining(state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
      end
    end

    describe "handle_demand accumulates demand" do
      test "accumulates demand across multiple calls" do
        state = build_state(%{demand: 0})

        {:noreply, [], state1} = Events.handle_demand(3, state)
        assert state1.demand == 3

        {:noreply, [], state2} = Events.handle_demand(2, state1)
        assert state2.demand == 5
      end
    end

    # ===================================================================
    # Phase 10: Additional coverage gap tests for broadway/events.ex
    # ===================================================================

    describe "drain_buffer edge cases" do
      test "drain_buffer with zero demand returns nothing" do
        buffer = :queue.in(:event1, :queue.new())
        state = build_state(%{buffer: buffer, buffer_size: 1, demand: 0})

        {:noreply, messages, new_state} = Events.handle_demand(0, state)
        assert messages == []
        assert new_state.demand == 0
        assert new_state.buffer_size == 1
      end
    end

    describe "handle_info {:kubemq_event, event} buffer full with demand" do
      test "drops event even when demand exists if buffer is full" do
        state = build_state(%{buffer_size: 5, max_buffer_size: 5, demand: 10})

        result = Events.handle_info({:kubemq_event, :overflow_event}, state)
        assert {:noreply, [], new_state} = result
        assert new_state.buffer_size == 5
      end
    end

    describe "ack/3 with messages" do
      test "is a no-op even with successful and failed messages" do
        msg = %Broadway.Message{
          data: :some_event,
          acknowledger: {Events, :ack_id, :ack_data}
        }

        assert :ok = Events.ack(:ref, [msg], [msg])
      end
    end

    # ===================================================================
    # Phase 11: handle_continue :subscribe paths for broadway/events.ex
    # ===================================================================

    describe "handle_continue :subscribe success" do
      defmodule FakeEventsClient do
        use GenServer

        def start_link(response) do
          GenServer.start_link(__MODULE__, response)
        end

        @impl GenServer
        def init(response), do: {:ok, response}

        @impl GenServer
        def handle_call({:subscribe_to_events, _channel, _opts}, _from, response) do
          {:reply, response, response}
        end
      end

      test "successful subscribe sets subscription and monitors" do
        sub_pid = spawn(fn -> Process.sleep(:infinity) end)
        sub = %KubeMQ.Subscription{pid: sub_pid, ref: make_ref()}
        {:ok, client} = FakeEventsClient.start_link({:ok, sub})

        state = build_state(%{client: client, subscription: nil, sub_ref: nil})
        {:noreply, [], new_state} = Events.handle_continue(:subscribe, state)

        assert new_state.subscription == sub
        assert new_state.sub_ref != nil

        Process.exit(sub_pid, :kill)
        GenServer.stop(client)
      end

      test "failed subscribe schedules retry" do
        {:ok, client} =
          FakeEventsClient.start_link(
            {:error, %KubeMQ.Error{code: :unavailable, message: "not ready"}}
          )

        state = build_state(%{client: client, subscription: nil})
        {:noreply, [], new_state} = Events.handle_continue(:subscribe, state)

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
          Events.handle_info({:DOWN, ref, :process, self(), :normal}, state)

        assert new_state.subscription == nil
        assert new_state.sub_ref == nil
        assert_receive :retry_subscribe, 6_000
      end
    end
  end
end
