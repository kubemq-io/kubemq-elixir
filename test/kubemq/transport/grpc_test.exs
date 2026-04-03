defmodule KubeMQ.Transport.GRPCTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Transport.GRPC

  describe "send_timeout/1" do
    test "returns default 5000 when no option provided" do
      assert GRPC.send_timeout([]) == 5_000
    end

    test "returns custom value from opts" do
      assert GRPC.send_timeout(send_timeout: 3_000) == 3_000
    end
  end

  describe "rpc_timeout/1" do
    test "returns default 10000 when no option provided" do
      assert GRPC.rpc_timeout([]) == 10_000
    end

    test "returns custom value from opts" do
      assert GRPC.rpc_timeout(rpc_timeout: 8_000) == 8_000
    end
  end

  describe "queue_message_from_proto/1" do
    test "converts a proto queue message with nil attributes and nil policy" do
      proto_msg = %KubeMQ.Proto.QueueMessage{
        message_id: "msg-1",
        client_id: "client-1",
        channel: "test-queue",
        metadata: "meta",
        body: "body-data",
        tags: %{"key" => "val"},
        policy: nil,
        attributes: nil
      }

      result = GRPC.queue_message_from_proto(proto_msg)

      assert result.id == "msg-1"
      assert result.channel == "test-queue"
      assert result.metadata == "meta"
      assert result.body == "body-data"
      assert result.client_id == "client-1"
      assert result.tags == %{"key" => "val"}
      assert result.policy == nil
      assert result.attributes == nil
    end

    test "converts a proto queue message with attributes" do
      proto_msg = %KubeMQ.Proto.QueueMessage{
        message_id: "msg-2",
        client_id: "client-2",
        channel: "queue-ch",
        metadata: "",
        body: "data",
        tags: %{},
        policy: nil,
        attributes: %KubeMQ.Proto.QueueMessageAttributes{
          timestamp: 1_700_000_000,
          sequence: 42,
          md5_of_body: "abc123",
          receive_count: 3,
          re_routed: true,
          re_routed_from_queue: "original-queue",
          expiration_at: 1_700_001_000,
          delayed_to: 0
        }
      }

      result = GRPC.queue_message_from_proto(proto_msg)

      assert result.attributes.timestamp == 1_700_000_000
      assert result.attributes.sequence == 42
      assert result.attributes.md5_of_body == "abc123"
      assert result.attributes.receive_count == 3
      assert result.attributes.re_routed == true
      assert result.attributes.re_routed_from_queue == "original-queue"
      assert result.attributes.expiration_at == 1_700_001_000
      assert result.attributes.delayed_to == 0
    end

    test "converts a proto queue message with policy" do
      proto_msg = %KubeMQ.Proto.QueueMessage{
        message_id: "msg-3",
        client_id: "client-3",
        channel: "queue-ch",
        metadata: "",
        body: "data",
        tags: %{},
        attributes: nil,
        policy: %KubeMQ.Proto.QueueMessagePolicy{
          expiration_seconds: 300,
          delay_seconds: 10,
          max_receive_count: 5,
          max_receive_queue: "dlq"
        }
      }

      result = GRPC.queue_message_from_proto(proto_msg)

      assert result.policy.expiration_seconds == 300
      assert result.policy.delay_seconds == 10
      assert result.policy.max_receive_count == 5
      assert result.policy.max_receive_queue == "dlq"
    end

    test "handles nil tags by defaulting to empty map" do
      proto_msg = %KubeMQ.Proto.QueueMessage{
        message_id: "msg-4",
        client_id: "",
        channel: "ch",
        metadata: "",
        body: "",
        tags: nil,
        policy: nil,
        attributes: nil
      }

      result = GRPC.queue_message_from_proto(proto_msg)
      assert result.tags == %{}
    end
  end
end
