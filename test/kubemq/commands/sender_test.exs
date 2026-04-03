defmodule KubeMQ.Commands.SenderTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{Command, CommandReply, CommandResponse, Error}
  alias KubeMQ.Commands.Sender

  setup :set_mox_global
  setup :verify_on_exit!

  describe "send_command/4" do
    test "success with correct request construction" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, request ->
        assert request.channel == "cmd-ch"
        assert request.body == "cmd-data"
        assert request.client_id == "client-1"
        assert request.timeout == 10_000
        assert request.request_type == :command
        assert request.metadata == ""
        assert request.tags == %{}
        assert request.cache_key == ""
        assert request.cache_ttl == 0
        assert is_binary(request.id) and byte_size(request.id) > 0

        {:ok,
         %{
           request_id: request.id,
           executed: true,
           timestamp: System.system_time(:nanosecond),
           error: "",
           cache_hit: false,
           metadata: "",
           body: "",
           tags: %{}
         }}
      end)

      command = Command.new(channel: "cmd-ch", body: "cmd-data", timeout: 10_000)

      assert {:ok, %CommandResponse{executed: true}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "validates channel - empty" do
      command = Command.new(channel: "", body: "data", timeout: 10_000)

      assert {:error, %Error{code: :validation}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "validates content - both nil" do
      command = Command.new(channel: "cmd-ch", timeout: 10_000)

      assert {:error, %Error{code: :validation}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "validates timeout - zero" do
      command = Command.new(channel: "cmd-ch", body: "data", timeout: 0)

      assert {:error, %Error{code: :validation}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "validates timeout - negative" do
      command = Command.new(channel: "cmd-ch", body: "data", timeout: -1)

      assert {:error, %Error{code: :validation}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "gRPC error tuple" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      command = Command.new(channel: "cmd-ch", body: "data", timeout: 10_000)

      assert {:error, %Error{}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end

    test "generic error" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, _request ->
        {:error, :connection_refused}
      end)

      command = Command.new(channel: "cmd-ch", body: "data", timeout: 10_000)

      assert {:error, %Error{code: :transient}} =
               Sender.send_command(:fake_channel, KubeMQ.MockTransport, command, "client-1")
    end
  end

  describe "send_response/3" do
    test "success with correct response construction" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, response_map ->
        assert response_map.request_id == "req-123"
        assert response_map.reply_channel == "reply-ch"
        assert response_map.executed == true
        assert response_map.error == ""
        assert response_map.client_id == ""
        assert response_map.metadata == ""
        assert response_map.body == ""
        assert response_map.cache_hit == false
        assert is_integer(response_map.timestamp)
        assert response_map.tags == %{}
        :ok
      end)

      reply =
        CommandReply.new(
          request_id: "req-123",
          response_to: "reply-ch",
          executed: true
        )

      assert :ok = Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "validates request_id - nil" do
      reply = CommandReply.new(response_to: "reply-ch", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "validates request_id - empty" do
      reply = CommandReply.new(request_id: "", response_to: "reply-ch", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "validates response_to - nil" do
      reply = CommandReply.new(request_id: "req-123", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "validates response_to - empty" do
      reply = CommandReply.new(request_id: "req-123", response_to: "", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "gRPC error tuple" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, _response ->
        {:error, {14, "unavailable"}}
      end)

      reply =
        CommandReply.new(
          request_id: "req-123",
          response_to: "reply-ch",
          executed: true
        )

      assert {:error, %Error{}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, _response ->
        {:error, :connection_lost}
      end)

      reply =
        CommandReply.new(
          request_id: "req-124",
          response_to: "reply-ch",
          executed: true
        )

      assert {:error, %Error{retryable?: true}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end
  end
end
