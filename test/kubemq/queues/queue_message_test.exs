defmodule KubeMQ.QueueMessageTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueAttributes
  alias KubeMQ.QueueMessage
  alias KubeMQ.QueuePolicy

  describe "new/0" do
    test "returns struct with default field values" do
      msg = QueueMessage.new()
      assert %QueueMessage{} = msg
      assert msg.id == nil
      assert msg.channel == nil
      assert msg.metadata == nil
      assert msg.body == nil
      assert msg.client_id == nil
      assert msg.tags == %{}
      assert msg.policy == nil
      assert msg.attributes == nil
    end
  end

  describe "new/1" do
    test "with policy and all fields" do
      policy = QueuePolicy.new(expiration_seconds: 60, delay_seconds: 10)

      msg =
        QueueMessage.new(
          id: "msg-1",
          channel: "orders",
          metadata: "meta",
          body: "payload",
          client_id: "client-1",
          tags: %{"key" => "val"},
          policy: policy
        )

      assert msg.id == "msg-1"
      assert msg.channel == "orders"
      assert msg.metadata == "meta"
      assert msg.body == "payload"
      assert msg.client_id == "client-1"
      assert msg.tags == %{"key" => "val"}
      assert %QueuePolicy{expiration_seconds: 60, delay_seconds: 10} = msg.policy
      assert msg.attributes == nil
    end
  end

  describe "from_transport/1" do
    test "nil tags become empty map, nil policy/attributes stay nil" do
      transport_msg = %{
        id: "id-1",
        channel: "ch",
        metadata: "md",
        body: "b",
        client_id: "c",
        tags: nil,
        policy: nil,
        attributes: nil
      }

      result = QueueMessage.from_transport(transport_msg)
      assert %QueueMessage{} = result
      assert result.id == "id-1"
      assert result.channel == "ch"
      assert result.tags == %{}
      assert result.policy == nil
      assert result.attributes == nil
    end

    test "full transport message with policy and attributes" do
      transport_msg = %{
        id: "id-2",
        channel: "orders",
        metadata: "meta",
        body: "data",
        client_id: "client-2",
        tags: %{"env" => "prod"},
        policy: %{
          expiration_seconds: 120,
          delay_seconds: 5,
          max_receive_count: 3,
          max_receive_queue: "dlq"
        },
        attributes: %{
          timestamp: 1_000_000,
          sequence: 42,
          md5_of_body: "abc123",
          receive_count: 2,
          re_routed: true,
          re_routed_from_queue: "original",
          expiration_at: 2_000_000,
          delayed_to: 1_500_000
        }
      }

      result = QueueMessage.from_transport(transport_msg)
      assert result.id == "id-2"
      assert result.channel == "orders"
      assert result.tags == %{"env" => "prod"}

      assert %QueuePolicy{} = result.policy
      assert result.policy.expiration_seconds == 120
      assert result.policy.delay_seconds == 5
      assert result.policy.max_receive_count == 3
      assert result.policy.max_receive_queue == "dlq"

      assert %QueueAttributes{} = result.attributes
      assert result.attributes.timestamp == 1_000_000
      assert result.attributes.sequence == 42
      assert result.attributes.md5_of_body == "abc123"
      assert result.attributes.receive_count == 2
      assert result.attributes.re_routed == true
      assert result.attributes.re_routed_from_queue == "original"
      assert result.attributes.expiration_at == 2_000_000
      assert result.attributes.delayed_to == 1_500_000
    end
  end
end
