defmodule KubeMQ.ConcurrencyTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{Connection, Event, Events.Publisher, Events.Subscriber}

  setup :set_mox_global
  setup :verify_on_exit!

  describe "multiple subscribers on same channel" do
    @tag :integration
    test "three subscribers all receive events on the same channel" do
      test_pid = self()

      # Each subscriber gets its own stream that yields one event then closes
      proto_event = %{
        event_id: "e1",
        channel: "shared-ch",
        metadata: "",
        body: "hello",
        timestamp: 0,
        sequence: 0,
        tags: %{}
      }

      # We need 3 subscribe calls (one per subscriber)
      expect(KubeMQ.MockTransport, :subscribe, 3, fn _ch, _req ->
        # Return a stream that emits one event then ends
        {:ok, [{:ok, proto_event}]}
      end)

      Process.flag(:trap_exit, true)

      subscribers =
        for i <- 1..3 do
          {:ok, pid} =
            Subscriber.start_link(
              channel: "shared-ch",
              client_id: "client-#{i}",
              transport: KubeMQ.MockTransport,
              grpc_channel: :fake_grpc,
              notify: test_pid
            )

          pid
        end

      # Each subscriber should forward the event
      for _ <- 1..3 do
        assert_receive {:kubemq_event, event}, 2_000
        assert event.id == "e1"
        assert event.channel == "shared-ch"
      end

      # Cleanup
      for pid <- subscribers do
        if Process.alive?(pid), do: Process.exit(pid, :shutdown)
      end
    end
  end

  describe "rapid subscribe/unsubscribe cycles" do
    test "50 rapid start/stop cycles do not crash" do
      Process.flag(:trap_exit, true)

      # Each cycle starts a subscriber then immediately stops it.
      # The subscribe call may or may not happen before shutdown.
      stub(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        # Return a stream that blocks briefly then ends
        {:ok, []}
      end)

      for _i <- 1..50 do
        {:ok, pid} =
          Subscriber.start_link(
            channel: "cycle-ch",
            client_id: "cycle-client",
            transport: KubeMQ.MockTransport,
            grpc_channel: :fake_grpc
          )

        # Immediately stop
        Process.exit(pid, :shutdown)
      end

      # Allow exits to propagate
      Process.sleep(200)

      # If we got here without a crash, the test passes
      assert true
    end
  end

  describe "concurrent send_event calls" do
    test "100 concurrent sends all succeed" do
      # Use expect with count=100 to verify exactly 100 calls are made
      expect(KubeMQ.MockTransport, :send_event, 100, fn _ch, _req -> :ok end)

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            event = Event.new(channel: "test-ch", body: "msg-#{i}")
            Publisher.send_event(:fake, KubeMQ.MockTransport, event, "c1")
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert length(results) == 100
      assert Enum.all?(results, &(&1 == :ok))
    end
  end

  describe "subscriber survives callback exception" do
    test "on_event raising does not kill the subscriber" do
      test_pid = self()
      Process.flag(:trap_exit, true)

      call_count = :counters.new(1, [:atomics])

      on_event = fn _event ->
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)

        if count == 1 do
          # First call raises
          raise "callback exploded"
        else
          # Second call succeeds -- notify test
          send(test_pid, :second_event_received)
        end
      end

      # Subscribe returns a stream with two events
      proto_event_1 = %{
        event_id: "e1",
        channel: "ch",
        metadata: "",
        body: "first",
        timestamp: 0,
        sequence: 1,
        tags: %{}
      }

      proto_event_2 = %{
        event_id: "e2",
        channel: "ch",
        metadata: "",
        body: "second",
        timestamp: 0,
        sequence: 2,
        tags: %{}
      }

      expect(KubeMQ.MockTransport, :subscribe, fn _ch, _req ->
        {:ok, [{:ok, proto_event_1}, {:ok, proto_event_2}]}
      end)

      {:ok, sub_pid} =
        Subscriber.start_link(
          channel: "ch",
          client_id: "c1",
          transport: KubeMQ.MockTransport,
          grpc_channel: :fake_grpc,
          on_event: on_event
        )

      # The subscriber should still be alive after the first callback raised
      assert_receive :second_event_received, 2_000
      assert Process.alive?(sub_pid)

      # Cleanup
      Process.exit(sub_pid, :shutdown)
    end
  end

  describe "connection buffer under concurrent load" do
    test "multiple callers get buffered during reconnecting state" do
      # Build a connection state struct in :reconnecting with buffer
      # Simulate multiple concurrent get_channel calls that get buffered

      # Start a real Connection process that will be stuck reconnecting
      # because GRPC.Stub.connect will fail
      Process.flag(:trap_exit, true)

      {:ok, conn_pid} =
        Connection.start_link(
          address: "localhost:99999",
          client_id: "test-buffer",
          reconnect_policy: [
            enabled: true,
            initial_delay: 5_000,
            max_delay: 10_000,
            max_attempts: 0
          ],
          reconnect_buffer_size: 100
        )

      # Wait briefly for the connection to fail and enter reconnecting
      Process.sleep(500)

      # Launch multiple concurrent get_channel calls.
      # They should be buffered (not crash) while reconnecting.
      tasks =
        for _i <- 1..10 do
          Task.async(fn ->
            try do
              Connection.get_channel(conn_pid, 1_000)
            catch
              :exit, _ -> {:error, :timeout}
            end
          end)
        end

      # All tasks should complete (either buffered and timed out, or got an error)
      # The important thing is no crashes
      results = Task.await_many(tasks, 5_000)
      assert length(results) == 10

      # Each result should be either {:ok, _} or {:error, _}
      for result <- results do
        assert match?({:ok, _}, result) or match?({:error, _}, result)
      end

      # Cleanup
      Connection.close(conn_pid)
    end
  end
end
