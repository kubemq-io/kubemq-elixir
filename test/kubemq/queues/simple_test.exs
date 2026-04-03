defmodule KubeMQ.Queues.SimpleTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.Queues.Simple

  alias KubeMQ.{
    Error,
    QueueAckAllResult,
    QueueBatchResult,
    QueueMessage,
    QueuePolicy,
    QueueReceiveResult,
    QueueSendResult
  }

  setup :set_mox_global
  setup :verify_on_exit!

  # ---------- send_queue_message ----------

  describe "send_queue_message/4" do
    test "success - builds transport message and returns QueueSendResult" do
      expect(KubeMQ.MockTransport, :send_queue_message, fn _channel, request ->
        assert request.channel == "orders"
        assert request.body == "payload"
        assert request.metadata == "meta"
        assert request.client_id == "c1"
        assert request.tags == %{"k" => "v"}
        assert is_binary(request.id) and byte_size(request.id) > 0
        assert request.policy == nil

        {:ok,
         %{
           message_id: "msg-1",
           sent_at: 1_000,
           expiration_at: 0,
           delayed_to: 0,
           is_error: false,
           error: ""
         }}
      end)

      msg =
        QueueMessage.new(
          channel: "orders",
          body: "payload",
          metadata: "meta",
          tags: %{"k" => "v"}
        )

      assert {:ok, %QueueSendResult{message_id: "msg-1"}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end

    test "validates channel - empty returns validation error" do
      msg = QueueMessage.new(channel: "", body: "data")

      assert {:error, %Error{code: :validation}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end

    test "validates content - both nil returns validation error" do
      msg = QueueMessage.new(channel: "orders")

      assert {:error, %Error{code: :validation}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end

    test "gRPC error tuple is wrapped" do
      expect(KubeMQ.MockTransport, :send_queue_message, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      msg = QueueMessage.new(channel: "orders", body: "data")

      assert {:error, %Error{}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :send_queue_message, fn _channel, _request ->
        {:error, :timeout}
      end)

      msg = QueueMessage.new(channel: "orders", body: "data")

      assert {:error, %Error{retryable?: true}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end

    test "with policy - policy fields are passed through" do
      expect(KubeMQ.MockTransport, :send_queue_message, fn _channel, request ->
        assert request.policy.expiration_seconds == 60
        assert request.policy.delay_seconds == 10
        assert request.policy.max_receive_count == 3
        assert request.policy.max_receive_queue == "dlq"

        {:ok,
         %{
           message_id: "msg-2",
           sent_at: 2_000,
           expiration_at: 0,
           delayed_to: 0,
           is_error: false,
           error: nil
         }}
      end)

      policy =
        QueuePolicy.new(
          expiration_seconds: 60,
          delay_seconds: 10,
          max_receive_count: 3,
          max_receive_queue: "dlq"
        )

      msg = QueueMessage.new(channel: "orders", body: "data", policy: policy)

      assert {:ok, %QueueSendResult{}} =
               Simple.send_queue_message(:fake, KubeMQ.MockTransport, msg, "c1")
    end
  end

  # ---------- send_queue_messages ----------

  describe "send_queue_messages/4" do
    test "batch success - sends all messages and returns QueueBatchResult" do
      expect(KubeMQ.MockTransport, :send_queue_messages_batch, fn _channel, batch ->
        assert is_binary(batch.batch_id) and byte_size(batch.batch_id) > 0
        assert length(batch.messages) == 2

        {:ok,
         %{
           batch_id: batch.batch_id,
           results: [
             %{
               message_id: "m1",
               sent_at: 1,
               expiration_at: 0,
               delayed_to: 0,
               is_error: false,
               error: ""
             },
             %{
               message_id: "m2",
               sent_at: 2,
               expiration_at: 0,
               delayed_to: 0,
               is_error: false,
               error: ""
             }
           ],
           have_errors: false
         }}
      end)

      msgs = [
        QueueMessage.new(channel: "q1", body: "a"),
        QueueMessage.new(channel: "q1", body: "b")
      ]

      assert {:ok, %QueueBatchResult{results: results}} =
               Simple.send_queue_messages(:fake, KubeMQ.MockTransport, msgs, "c1")

      assert length(results) == 2
    end

    test "gRPC error tuple is wrapped" do
      expect(KubeMQ.MockTransport, :send_queue_messages_batch, fn _channel, _batch ->
        {:error, {13, "internal"}}
      end)

      msgs = [QueueMessage.new(channel: "q1", body: "a")]

      assert {:error, %Error{}} =
               Simple.send_queue_messages(:fake, KubeMQ.MockTransport, msgs, "c1")
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :send_queue_messages_batch, fn _channel, _batch ->
        {:error, :timeout}
      end)

      msgs = [QueueMessage.new(channel: "q1", body: "a")]

      assert {:error, %Error{retryable?: true}} =
               Simple.send_queue_messages(:fake, KubeMQ.MockTransport, msgs, "c1")
    end
  end

  # ---------- receive_queue_messages ----------

  describe "receive_queue_messages/5" do
    test "success - returns QueueReceiveResult with messages" do
      expect(KubeMQ.MockTransport, :receive_queue_messages, fn _channel, request ->
        assert request.channel == "orders"
        assert request.max_messages == 5
        assert request.wait_time_seconds >= 1
        assert request.is_peek == false
        assert is_binary(request.client_id)

        {:ok,
         %{
           request_id: "req-1",
           messages: [
             %{
               id: "m1",
               channel: "orders",
               metadata: "",
               body: "hello",
               client_id: "c1",
               tags: %{},
               policy: nil,
               attributes: nil
             }
           ],
           messages_received: 1,
           messages_expired: 0,
           is_peek: false,
           is_error: false,
           error: ""
         }}
      end)

      assert {:ok, %QueueReceiveResult{messages_received: 1}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 max_messages: 5,
                 wait_timeout: 10_000
               )
    end

    test "validates channel - nil returns validation error" do
      assert {:error, %Error{code: :validation}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, nil, "c1")
    end

    test "validates max_messages - ES17 boundary 0 rejected" do
      assert {:error, %Error{code: :validation}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 max_messages: 0
               )
    end

    test "validates max_messages - ES17 boundary 1025 rejected" do
      assert {:error, %Error{code: :validation}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 max_messages: 1025
               )
    end

    test "validates wait_timeout - ES18 negative rejected" do
      assert {:error, %Error{code: :validation}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 wait_timeout: -1
               )
    end

    test "validates wait_timeout - ES18 above 3600000 rejected" do
      assert {:error, %Error{code: :validation}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 wait_timeout: 3_600_001
               )
    end

    test "gRPC error tuple is wrapped" do
      expect(KubeMQ.MockTransport, :receive_queue_messages, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      assert {:error, %Error{}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1")
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :receive_queue_messages, fn _channel, _request ->
        {:error, :timeout}
      end)

      assert {:error, %Error{retryable?: true}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1")
    end

    test "peek mode sets is_peek true" do
      expect(KubeMQ.MockTransport, :receive_queue_messages, fn _channel, request ->
        assert request.is_peek == true

        {:ok,
         %{
           request_id: "req-2",
           messages: [],
           messages_received: 0,
           messages_expired: 0,
           is_peek: true,
           is_error: false,
           error: ""
         }}
      end)

      assert {:ok, %QueueReceiveResult{}} =
               Simple.receive_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1",
                 is_peek: true
               )
    end
  end

  # ---------- ack_all_queue_messages ----------

  describe "ack_all_queue_messages/5" do
    test "success - returns QueueAckAllResult" do
      expect(KubeMQ.MockTransport, :ack_all_queue_messages, fn _channel, request ->
        assert request.channel == "orders"
        assert request.wait_time_seconds >= 1
        assert is_binary(request.client_id)

        {:ok,
         %{
           request_id: "req-1",
           affected_messages: 5,
           is_error: false,
           error: ""
         }}
      end)

      assert {:ok, %QueueAckAllResult{affected_messages: 5}} =
               Simple.ack_all_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1")
    end

    test "validates channel - empty returns validation error" do
      assert {:error, %Error{code: :validation}} =
               Simple.ack_all_queue_messages(:fake, KubeMQ.MockTransport, "", "c1")
    end

    test "gRPC error tuple is wrapped" do
      expect(KubeMQ.MockTransport, :ack_all_queue_messages, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      assert {:error, %Error{}} =
               Simple.ack_all_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1")
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :ack_all_queue_messages, fn _channel, _request ->
        {:error, :timeout}
      end)

      assert {:error, %Error{retryable?: true}} =
               Simple.ack_all_queue_messages(:fake, KubeMQ.MockTransport, "orders", "c1")
    end
  end
end
