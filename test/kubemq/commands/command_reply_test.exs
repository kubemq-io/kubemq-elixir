defmodule KubeMQ.CommandReplyTest do
  use ExUnit.Case, async: true

  alias KubeMQ.CommandReply

  describe "new/0" do
    test "returns struct with default values" do
      reply = CommandReply.new()
      assert %CommandReply{} = reply
      assert reply.request_id == nil
      assert reply.response_to == nil
      assert reply.metadata == nil
      assert reply.body == nil
      assert reply.client_id == nil
      assert reply.executed == false
      assert reply.error == nil
      assert reply.tags == %{}
      assert reply.span == nil
    end
  end

  describe "new/1" do
    test "sets all fields from keyword options" do
      reply =
        CommandReply.new(
          request_id: "req-1",
          response_to: "reply-ch",
          metadata: "meta",
          body: <<1>>,
          client_id: "c1",
          executed: true,
          error: "oops",
          tags: %{"k" => "v"},
          span: <<9>>
        )

      assert reply.request_id == "req-1"
      assert reply.response_to == "reply-ch"
      assert reply.metadata == "meta"
      assert reply.body == <<1>>
      assert reply.client_id == "c1"
      assert reply.executed == true
      assert reply.error == "oops"
      assert reply.tags == %{"k" => "v"}
      assert reply.span == <<9>>
    end
  end

  describe "to_response_map/1" do
    test "converts reply struct to response map" do
      reply =
        CommandReply.new(
          request_id: "req-1",
          response_to: "reply-ch",
          client_id: "c1",
          metadata: "meta",
          body: <<5>>,
          executed: true,
          error: "err",
          tags: %{"a" => "b"}
        )

      result = CommandReply.to_response_map(reply)

      assert result.request_id == "req-1"
      assert result.reply_channel == "reply-ch"
      assert result.client_id == "c1"
      assert result.metadata == "meta"
      assert result.body == <<5>>
      assert result.executed == true
      assert result.error == "err"
      assert result.cache_hit == false
      assert result.tags == %{"a" => "b"}
      assert is_integer(result.timestamp)
    end
  end

  describe "to_response_map/1 nil defaults" do
    test "nil fields default to empty strings or empty map" do
      reply = CommandReply.new()
      result = CommandReply.to_response_map(reply)

      assert result.client_id == ""
      assert result.metadata == ""
      assert result.body == ""
      assert result.error == ""
      assert result.tags == %{}
    end
  end
end
