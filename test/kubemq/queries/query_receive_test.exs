defmodule KubeMQ.QueryReceiveTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueryReceive

  describe "from_proto/1" do
    test "maps proto request fields to struct" do
      proto = %{
        request_id: "qr-1",
        channel: "queries.ch",
        metadata: "meta",
        body: <<10, 20>>,
        reply_channel: "reply-ch",
        tags: %{"env" => "test"}
      }

      result = QueryReceive.from_proto(proto)

      assert %QueryReceive{} = result
      assert result.id == "qr-1"
      assert result.channel == "queries.ch"
      assert result.metadata == "meta"
      assert result.body == <<10, 20>>
      assert result.reply_channel == "reply-ch"
      assert result.tags == %{"env" => "test"}
      assert result.span == nil
    end
  end

  describe "from_proto/1 nil tags" do
    test "nil tags default to empty map" do
      proto = %{
        request_id: "qr-2",
        channel: "ch",
        metadata: "",
        body: "",
        reply_channel: "reply",
        tags: nil
      }

      result = QueryReceive.from_proto(proto)
      assert result.tags == %{}
    end
  end
end
