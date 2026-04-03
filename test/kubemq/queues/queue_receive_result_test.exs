defmodule KubeMQ.QueueReceiveResultTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueMessage
  alias KubeMQ.QueueReceiveResult

  describe "new/0" do
    test "returns struct with defaults" do
      result = QueueReceiveResult.new()
      assert %QueueReceiveResult{} = result
      assert result.request_id == ""
      assert result.messages == []
      assert result.messages_received == 0
      assert result.messages_expired == 0
      assert result.is_peek == false
      assert result.is_error == false
      assert result.error == nil
    end
  end

  describe "from_transport/1" do
    test "maps all fields and converts nested messages" do
      transport = %{
        request_id: "req-1",
        messages: [
          %{
            id: "msg-1",
            channel: "q1",
            metadata: "",
            body: "data",
            client_id: "c1",
            tags: %{"k" => "v"},
            policy: nil,
            attributes: nil
          }
        ],
        messages_received: 1,
        messages_expired: 0,
        is_peek: true,
        is_error: false,
        error: nil
      }

      result = QueueReceiveResult.from_transport(transport)
      assert result.request_id == "req-1"
      assert result.messages_received == 1
      assert result.is_peek == true
      assert result.is_error == false
      assert result.error == nil
      assert length(result.messages) == 1

      [msg] = result.messages
      assert %QueueMessage{} = msg
      assert msg.id == "msg-1"
      assert msg.channel == "q1"
      assert msg.tags == %{"k" => "v"}
    end

    test "empty string error becomes nil" do
      transport = %{
        request_id: "req-2",
        messages: [],
        messages_received: 0,
        messages_expired: 0,
        is_peek: false,
        is_error: false,
        error: ""
      }

      result = QueueReceiveResult.from_transport(transport)
      assert result.error == nil
    end
  end
end
