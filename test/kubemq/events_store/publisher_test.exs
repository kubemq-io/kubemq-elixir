defmodule KubeMQ.EventsStore.PublisherTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{Error, EventStore, EventStoreResult}
  alias KubeMQ.EventsStore.Publisher

  setup :set_mox_global
  setup :verify_on_exit!

  describe "send_event_store/4" do
    test "success with correct request construction" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, request ->
        assert request.channel == "store-ch"
        assert request.body == "persistent-data"
        assert request.client_id == "client-1"
        assert request.store == true
        assert request.metadata == ""
        assert request.tags == %{}
        assert is_binary(request.id) and byte_size(request.id) > 0
        :ok
      end)

      event = EventStore.new(channel: "store-ch", body: "persistent-data")

      assert {:ok, %EventStoreResult{sent: true, error: nil} = result} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")

      assert is_binary(result.id) and byte_size(result.id) > 0
    end

    test "validates channel - empty" do
      event = EventStore.new(channel: "", body: "data")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "validates channel - nil" do
      event = EventStore.new(body: "data")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "validates content - both nil" do
      event = EventStore.new(channel: "store-ch")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "gRPC error tuple" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      event = EventStore.new(channel: "store-ch", body: "data")

      assert {:error, %Error{}} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "generic error" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, _request ->
        {:error, :connection_refused}
      end)

      event = EventStore.new(channel: "store-ch", body: "data")

      assert {:error, %Error{code: :transient}} =
               Publisher.send_event_store(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end
  end
end
