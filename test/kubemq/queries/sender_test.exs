defmodule KubeMQ.Queries.SenderTest do
  use ExUnit.Case, async: false

  import Mox

  alias KubeMQ.{Error, Query, QueryReply, QueryResponse}
  alias KubeMQ.Queries.Sender

  setup :set_mox_global
  setup :verify_on_exit!

  describe "send_query/4" do
    test "success with correct request construction" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, request ->
        assert request.channel == "query-ch"
        assert request.body == "query-data"
        assert request.client_id == "client-1"
        assert request.timeout == 15_000
        assert request.request_type == :query
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
           metadata: "result-meta",
           body: "result-body",
           tags: %{}
         }}
      end)

      query = Query.new(channel: "query-ch", body: "query-data", timeout: 15_000)

      assert {:ok, %QueryResponse{executed: true}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "validates channel - empty" do
      query = Query.new(channel: "", body: "data", timeout: 10_000)

      assert {:error, %Error{code: :validation}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "validates content - both nil" do
      query = Query.new(channel: "query-ch", timeout: 10_000)

      assert {:error, %Error{code: :validation}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "validates timeout - zero" do
      query = Query.new(channel: "query-ch", body: "data", timeout: 0)

      assert {:error, %Error{code: :validation}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "cache_key with valid cache_ttl passes" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, request ->
        assert request.cache_key == "my-key"
        assert request.cache_ttl == 60

        {:ok,
         %{
           request_id: request.id,
           executed: true,
           timestamp: System.system_time(:nanosecond),
           error: "",
           cache_hit: true,
           metadata: "",
           body: "",
           tags: %{}
         }}
      end)

      query =
        Query.new(
          channel: "query-ch",
          body: "data",
          timeout: 10_000,
          cache_key: "my-key",
          cache_ttl: 60_000
        )

      assert {:ok, %QueryResponse{cache_hit: true}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "cache_ttl conversion from ms to seconds" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, request ->
        # 30_000 ms should become 30 seconds
        assert request.cache_ttl == 30

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

      query =
        Query.new(
          channel: "query-ch",
          body: "data",
          timeout: 10_000,
          cache_key: "key",
          cache_ttl: 30_000
        )

      assert {:ok, %QueryResponse{}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "cache_key with invalid cache_ttl fails validation" do
      query =
        Query.new(
          channel: "query-ch",
          body: "data",
          timeout: 10_000,
          cache_key: "my-key",
          cache_ttl: 0
        )

      assert {:error, %Error{code: :validation}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "gRPC error tuple" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, _request ->
        {:error, {14, "unavailable"}}
      end)

      query = Query.new(channel: "query-ch", body: "data", timeout: 10_000)

      assert {:error, %Error{}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :send_request, fn _channel, _request ->
        {:error, :timeout}
      end)

      query = Query.new(channel: "query-ch", body: "data", timeout: 10_000)

      assert {:error, %Error{retryable?: true}} =
               Sender.send_query(:fake_channel, KubeMQ.MockTransport, query, "client-1")
    end
  end

  describe "send_response/3" do
    test "success with correct response construction" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, response_map ->
        assert response_map.request_id == "req-456"
        assert response_map.reply_channel == "reply-ch"
        assert response_map.executed == true
        assert response_map.error == ""
        assert response_map.client_id == ""
        assert response_map.metadata == ""
        assert response_map.body == "response-body"
        assert response_map.cache_hit == false
        assert is_integer(response_map.timestamp)
        assert response_map.tags == %{}
        :ok
      end)

      reply =
        QueryReply.new(
          request_id: "req-456",
          response_to: "reply-ch",
          executed: true,
          body: "response-body"
        )

      assert :ok = Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "validates request_id and response_to" do
      reply = QueryReply.new(response_to: "reply-ch", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)

      reply2 = QueryReply.new(request_id: "req-456", executed: true)

      assert {:error, %Error{code: :validation}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply2)
    end

    test "gRPC error tuple is wrapped" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, _response_map ->
        {:error, {14, "unavailable"}}
      end)

      reply =
        QueryReply.new(
          request_id: "req-789",
          response_to: "reply-ch",
          executed: true
        )

      assert {:error, %Error{}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end

    test "generic error is wrapped as transient" do
      expect(KubeMQ.MockTransport, :send_response, fn _channel, _response_map ->
        {:error, :connection_lost}
      end)

      reply =
        QueryReply.new(
          request_id: "req-790",
          response_to: "reply-ch",
          executed: true
        )

      assert {:error, %Error{retryable?: true}} =
               Sender.send_response(:fake_channel, KubeMQ.MockTransport, reply)
    end
  end
end
