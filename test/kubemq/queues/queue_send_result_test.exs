defmodule KubeMQ.QueueSendResultTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueSendResult

  describe "new/0" do
    test "returns struct with defaults" do
      result = QueueSendResult.new()
      assert %QueueSendResult{} = result
      assert result.message_id == ""
      assert result.sent_at == 0
      assert result.expiration_at == 0
      assert result.delayed_to == 0
      assert result.is_error == false
      assert result.error == nil
    end
  end

  describe "from_transport/1" do
    test "maps all fields from transport" do
      transport = %{
        message_id: "msg-42",
        sent_at: 1_700_000,
        expiration_at: 1_800_000,
        delayed_to: 1_750_000,
        is_error: true,
        error: "queue full"
      }

      result = QueueSendResult.from_transport(transport)
      assert result.message_id == "msg-42"
      assert result.sent_at == 1_700_000
      assert result.expiration_at == 1_800_000
      assert result.delayed_to == 1_750_000
      assert result.is_error == true
      assert result.error == "queue full"
    end

    test "empty string error becomes nil" do
      transport = %{
        message_id: "msg-1",
        sent_at: 0,
        expiration_at: 0,
        delayed_to: 0,
        is_error: false,
        error: ""
      }

      result = QueueSendResult.from_transport(transport)
      assert result.error == nil
    end
  end
end
