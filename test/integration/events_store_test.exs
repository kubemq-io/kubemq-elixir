defmodule KubeMQ.Integration.EventsStoreTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-events-store-#{System.unique_integer([:positive])}"

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-events-store-test"
      )

    on_exit(fn ->
      if Process.alive?(client), do: Client.close(client)
    end)

    %{client: client}
  end

  describe "events store pub/sub" do
    test "publish -> subscribe with start_from_first -> receive", %{client: client} do
      channel = unique_channel()
      test_pid = self()

      # Publish first (persistent, so subscriber can replay)
      event = %KubeMQ.EventStore{
        channel: channel,
        body: "persistent event",
        metadata: "store-metadata",
        client_id: "integration-events-store-test"
      }

      {:ok, result} = Client.send_event_store(client, event)
      assert result.sent == true

      # Subscribe with start_from_first to replay
      {:ok, _sub} =
        Client.subscribe_to_events_store(client, channel,
          start_at: :start_from_first,
          on_event: fn event ->
            send(test_pid, {:received_store_event, event})
          end
        )

      # Wait for the replayed event
      assert_receive {:received_store_event, received}, 5_000
      assert received.channel == channel
      assert received.body == "persistent event"
      assert received.metadata == "store-metadata"
    end

    test "subscribe receives new events after subscription", %{client: client} do
      channel = unique_channel()
      test_pid = self()

      {:ok, _sub} =
        Client.subscribe_to_events_store(client, channel,
          start_at: :start_from_new,
          on_event: fn event ->
            send(test_pid, {:received_store_event, event})
          end
        )

      Process.sleep(500)

      event = %KubeMQ.EventStore{
        channel: channel,
        body: "new event after sub",
        client_id: "integration-events-store-test"
      }

      {:ok, result} = Client.send_event_store(client, event)
      assert result.sent == true

      assert_receive {:received_store_event, received}, 5_000
      assert received.body == "new event after sub"
    end

    test "multiple persistent events replayed in sequence order", %{client: client} do
      channel = unique_channel()
      test_pid = self()

      # Publish multiple events
      for i <- 1..3 do
        event = %KubeMQ.EventStore{
          channel: channel,
          body: "event-#{i}",
          client_id: "integration-events-store-test"
        }

        {:ok, _result} = Client.send_event_store(client, event)
      end

      # Subscribe and replay from first
      {:ok, _sub} =
        Client.subscribe_to_events_store(client, channel,
          start_at: :start_from_first,
          on_event: fn event ->
            send(test_pid, {:received_store_event, event.body})
          end
        )

      for i <- 1..3 do
        assert_receive {:received_store_event, body}, 5_000
        assert body == "event-#{i}"
      end
    end
  end
end
