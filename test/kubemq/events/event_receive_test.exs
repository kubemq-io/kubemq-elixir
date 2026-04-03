defmodule KubeMQ.EventReceiveTest do
  use ExUnit.Case, async: true

  alias KubeMQ.EventReceive

  describe "from_proto/1" do
    test "maps all fields from proto map" do
      proto = %{
        event_id: "evt-123",
        channel: "notifications",
        metadata: "meta-data",
        body: "payload",
        timestamp: 1_700_000_000,
        sequence: 42,
        tags: %{"env" => "prod"}
      }

      result = EventReceive.from_proto(proto)

      assert %EventReceive{} = result
      assert result.id == "evt-123"
      assert result.channel == "notifications"
      assert result.metadata == "meta-data"
      assert result.body == "payload"
      assert result.timestamp == 1_700_000_000
      assert result.sequence == 42
      assert result.tags == %{"env" => "prod"}
    end

    test "nil tags defaults to empty map" do
      proto = %{
        event_id: "evt-1",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: 0,
        sequence: 0,
        tags: nil
      }

      result = EventReceive.from_proto(proto)
      assert result.tags == %{}
    end

    test "nil timestamp and sequence default to 0" do
      proto = %{
        event_id: "evt-2",
        channel: "ch",
        metadata: "",
        body: "",
        timestamp: nil,
        sequence: nil,
        tags: %{}
      }

      result = EventReceive.from_proto(proto)
      assert result.timestamp == 0
      assert result.sequence == 0
    end
  end
end
