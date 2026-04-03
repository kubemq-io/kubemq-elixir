defmodule KubeMQ.EventStoreTest do
  use ExUnit.Case, async: true

  alias KubeMQ.EventStore

  describe "new/0" do
    test "returns struct with default values" do
      event = EventStore.new()
      assert %EventStore{} = event
      assert event.id == nil
      assert event.channel == nil
      assert event.metadata == nil
      assert event.body == nil
      assert event.client_id == nil
      assert event.tags == %{}
    end
  end

  describe "new/1" do
    test "sets all fields from keyword options" do
      event =
        EventStore.new(
          id: "es-1",
          channel: "audit-log",
          metadata: "meta",
          body: "user login",
          client_id: "client-1",
          tags: %{"level" => "info"}
        )

      assert event.id == "es-1"
      assert event.channel == "audit-log"
      assert event.metadata == "meta"
      assert event.body == "user login"
      assert event.client_id == "client-1"
      assert event.tags == %{"level" => "info"}
    end
  end
end
