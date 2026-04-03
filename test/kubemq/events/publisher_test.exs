defmodule KubeMQ.Events.PublisherTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{Error, Event}
  alias KubeMQ.Events.Publisher

  setup :set_mox_global
  setup :verify_on_exit!

  describe "send_event/4" do
    test "success with correct request construction" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, request ->
        assert request.channel == "test-ch"
        assert request.body == "data"
        assert request.client_id == "client-1"
        assert request.store == false
        assert request.metadata == ""
        assert request.tags == %{}
        assert is_binary(request.id) and byte_size(request.id) > 0
        :ok
      end)

      event = Event.new(channel: "test-ch", body: "data")
      assert :ok = Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "validates channel - empty" do
      event = Event.new(channel: "", body: "data")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "validates channel - nil" do
      event = Event.new(body: "data")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "validates content - both nil" do
      event = Event.new(channel: "test-ch")

      assert {:error, %Error{code: :validation}} =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "gRPC error tuple" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      event = Event.new(channel: "test-ch", body: "data")

      assert {:error, %Error{}} =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "generic error" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, _request ->
        {:error, :connection_refused}
      end)

      event = Event.new(channel: "test-ch", body: "data")

      assert {:error, %Error{code: :transient}} =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end

    test "client_id override from event" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, request ->
        assert request.client_id == "event-client"
        :ok
      end)

      event = Event.new(channel: "test-ch", body: "data", client_id: "event-client")

      assert :ok =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "fallback-client")
    end

    test "client_id fallback when event client_id is nil" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, request ->
        assert request.client_id == "fallback-client"
        :ok
      end)

      event = Event.new(channel: "test-ch", body: "data")

      assert :ok =
               Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "fallback-client")
    end

    test "UUID generation when id is nil" do
      expect(KubeMQ.MockTransport, :send_event, fn _channel, request ->
        assert is_binary(request.id)
        assert byte_size(request.id) > 0
        :ok
      end)

      event = Event.new(channel: "test-ch", body: "data")
      assert event.id == nil
      assert :ok = Publisher.send_event(:fake_channel, KubeMQ.MockTransport, event, "client-1")
    end
  end
end
