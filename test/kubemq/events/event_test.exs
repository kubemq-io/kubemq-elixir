defmodule KubeMQ.EventTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.Event

  alias KubeMQ.Event

  describe "new/0" do
    test "returns struct with default values" do
      event = Event.new()
      assert %Event{} = event
      assert event.id == nil
      assert event.channel == nil
      assert event.metadata == nil
      assert event.body == nil
      assert event.client_id == nil
      assert event.tags == %{}
      assert event.store == false
    end
  end

  describe "new/1" do
    test "sets all fields from keyword options" do
      event =
        Event.new(
          id: "evt-1",
          channel: "notifications",
          metadata: "meta",
          body: "hello",
          client_id: "client-1",
          tags: %{"key" => "value"}
        )

      assert event.id == "evt-1"
      assert event.channel == "notifications"
      assert event.metadata == "meta"
      assert event.body == "hello"
      assert event.client_id == "client-1"
      assert event.tags == %{"key" => "value"}
    end
  end

  describe "store enforcement" do
    test "store is always false regardless of input" do
      event = Event.new(store: true)
      assert event.store == false
    end
  end

  describe "struct defaults" do
    test "bare struct has expected defaults" do
      event = %Event{}
      assert event.id == nil
      assert event.channel == nil
      assert event.metadata == nil
      assert event.body == nil
      assert event.client_id == nil
      assert event.tags == %{}
      assert event.store == false
    end
  end
end
