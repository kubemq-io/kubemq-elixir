defmodule KubeMQ.QueueAckAllResultTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueAckAllResult

  describe "new/0" do
    test "returns struct with defaults" do
      result = QueueAckAllResult.new()
      assert %QueueAckAllResult{} = result
      assert result.request_id == ""
      assert result.affected_messages == 0
      assert result.is_error == false
      assert result.error == nil
    end
  end

  describe "from_transport/1" do
    test "maps all fields and normalizes empty error to nil" do
      transport = %{
        request_id: "req-ack",
        affected_messages: 5,
        is_error: false,
        error: ""
      }

      result = QueueAckAllResult.from_transport(transport)
      assert result.request_id == "req-ack"
      assert result.affected_messages == 5
      assert result.is_error == false
      assert result.error == nil
    end
  end
end
