defmodule KubeMQ.EventStoreReceiveTest do
  use ExUnit.Case, async: true

  alias KubeMQ.EventStoreReceive

  describe "from_proto/1" do
    test "maps all fields from proto map" do
      proto = %{
        event_id: "esr-123",
        channel: "audit-log",
        metadata: "meta",
        body: "payload",
        timestamp: 1_700_000_000,
        sequence: 99,
        tags: %{"source" => "api"}
      }

      result = EventStoreReceive.from_proto(proto)

      assert %EventStoreReceive{} = result
      assert result.id == "esr-123"
      assert result.channel == "audit-log"
      assert result.metadata == "meta"
      assert result.body == "payload"
      assert result.timestamp == 1_700_000_000
      assert result.sequence == 99
      assert result.tags == %{"source" => "api"}
    end

    test "nil fields default correctly" do
      proto = %{
        event_id: "esr-1",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: nil,
        sequence: nil,
        tags: nil
      }

      result = EventStoreReceive.from_proto(proto)
      assert result.timestamp == 0
      assert result.sequence == 0
      assert result.tags == %{}
    end
  end
end
