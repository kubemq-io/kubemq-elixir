defmodule KubeMQ.EventStoreResultTest do
  use ExUnit.Case, async: true

  alias KubeMQ.EventStoreResult

  describe "new/0" do
    test "returns struct with default values" do
      result = EventStoreResult.new()
      assert %EventStoreResult{} = result
      assert result.id == ""
      assert result.sent == false
      assert result.error == nil
    end
  end

  describe "new/1" do
    test "sets all fields from keyword options" do
      result = EventStoreResult.new(id: "res-1", sent: true, error: "something failed")

      assert result.id == "res-1"
      assert result.sent == true
      assert result.error == "something failed"
    end
  end

  describe "from_proto/1" do
    test "maps all fields from proto map" do
      proto = %{
        event_id: "res-123",
        sent: true,
        error: "broker error"
      }

      result = EventStoreResult.from_proto(proto)

      assert %EventStoreResult{} = result
      assert result.id == "res-123"
      assert result.sent == true
      assert result.error == "broker error"
    end

    test "nil and empty error are normalized to nil" do
      proto_nil = %{event_id: "res-1", sent: true, error: nil}
      proto_empty = %{event_id: "res-2", sent: true, error: ""}

      result_nil = EventStoreResult.from_proto(proto_nil)
      result_empty = EventStoreResult.from_proto(proto_empty)

      assert result_nil.error == nil
      assert result_empty.error == nil
    end
  end
end
