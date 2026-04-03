defmodule KubeMQ.CommandReceiveTest do
  use ExUnit.Case, async: true

  alias KubeMQ.CommandReceive

  describe "from_proto/1" do
    test "maps proto request fields to struct" do
      proto = %{
        request_id: "req-1",
        channel: "commands.ch",
        metadata: "meta-data",
        body: <<10, 20>>,
        reply_channel: "reply-ch",
        tags: %{"env" => "test"}
      }

      result = CommandReceive.from_proto(proto)

      assert %CommandReceive{} = result
      assert result.id == "req-1"
      assert result.channel == "commands.ch"
      assert result.metadata == "meta-data"
      assert result.body == <<10, 20>>
      assert result.reply_channel == "reply-ch"
      assert result.tags == %{"env" => "test"}
      assert result.span == nil
    end

    test "nil tags default to empty map" do
      proto = %{
        request_id: "req-2",
        channel: "ch",
        metadata: "",
        body: "",
        reply_channel: "reply",
        tags: nil
      }

      result = CommandReceive.from_proto(proto)
      assert result.tags == %{}
    end

    test "span is always nil" do
      proto = %{
        request_id: "req-3",
        channel: "ch",
        metadata: "",
        body: "",
        reply_channel: "reply",
        tags: %{}
      }

      result = CommandReceive.from_proto(proto)
      assert result.span == nil
    end
  end
end
