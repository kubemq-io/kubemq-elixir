defmodule KubeMQ.Integration.EventsTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-events-#{System.unique_integer([:positive])}"

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-events-test"
      )

    on_exit(fn ->
      if Process.alive?(client), do: Client.close(client)
    end)

    %{client: client}
  end

  describe "events pub/sub" do
    test "publish -> subscribe -> receive", %{client: client} do
      channel = unique_channel()
      test_pid = self()

      # Subscribe first
      {:ok, _sub} =
        Client.subscribe_to_events(client, channel,
          on_event: fn event ->
            send(test_pid, {:received_event, event})
          end
        )

      # Give the subscription time to establish
      Process.sleep(500)

      # Publish an event
      event = %KubeMQ.Event{
        channel: channel,
        body: "hello events",
        metadata: "test-metadata",
        client_id: "integration-events-test"
      }

      assert :ok = Client.send_event(client, event)

      # Wait for the event to arrive
      assert_receive {:received_event, received}, 5_000
      assert received.channel == channel
      assert received.body == "hello events"
      assert received.metadata == "test-metadata"
    end

    test "publish -> subscribe with notify -> receive via message", %{client: client} do
      channel = unique_channel()

      {:ok, _sub} =
        Client.subscribe_to_events(client, channel, notify: self())

      Process.sleep(500)

      event = %KubeMQ.Event{
        channel: channel,
        body: "notify test",
        client_id: "integration-events-test"
      }

      assert :ok = Client.send_event(client, event)

      assert_receive {:kubemq_event, received}, 5_000
      assert received.channel == channel
      assert received.body == "notify test"
    end

    test "subscribe with group distributes events", %{client: client} do
      channel = unique_channel()
      test_pid = self()
      group = "test-group"
      received_by = :ets.new(:received_by, [:set, :public])

      {:ok, _sub1} =
        Client.subscribe_to_events(client, channel,
          group: group,
          on_event: fn event ->
            :ets.insert(received_by, {:sub1, event})
            send(test_pid, {:received, :sub1})
          end
        )

      {:ok, _sub2} =
        Client.subscribe_to_events(client, channel,
          group: group,
          on_event: fn event ->
            :ets.insert(received_by, {:sub2, event})
            send(test_pid, {:received, :sub2})
          end
        )

      Process.sleep(500)

      event = %KubeMQ.Event{
        channel: channel,
        body: "group test",
        client_id: "integration-events-test"
      }

      assert :ok = Client.send_event(client, event)

      # At least one subscriber should receive it
      assert_receive {:received, _subscriber}, 5_000

      :ets.delete(received_by)
    end

    test "multiple events received in order", %{client: client} do
      channel = unique_channel()
      test_pid = self()

      {:ok, _sub} =
        Client.subscribe_to_events(client, channel,
          on_event: fn event ->
            send(test_pid, {:received_event, event.body})
          end
        )

      Process.sleep(500)

      for i <- 1..5 do
        event = %KubeMQ.Event{
          channel: channel,
          body: "msg-#{i}",
          client_id: "integration-events-test"
        }

        assert :ok = Client.send_event(client, event)
      end

      for i <- 1..5 do
        assert_receive {:received_event, body}, 5_000
        assert body == "msg-#{i}"
      end
    end
  end
end
