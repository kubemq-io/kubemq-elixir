defmodule KubeMQ.ChannelInfoTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.ChannelInfo

  alias KubeMQ.ChannelInfo

  describe "from_json/1" do
    test "empty string returns empty list" do
      assert {:ok, []} = ChannelInfo.from_json("")
    end

    test "valid JSON array returns channel info structs" do
      json =
        Jason.encode!([
          %{
            "name" => "events.ch1",
            "type" => "events",
            "lastActivity" => 1_700_000,
            "isActive" => true,
            "incoming" => %{"messages" => 100},
            "outgoing" => %{"messages" => 50}
          }
        ])

      assert {:ok, [info]} = ChannelInfo.from_json(json)
      assert %ChannelInfo{} = info
      assert info.name == "events.ch1"
      assert info.type == "events"
      assert info.last_activity == 1_700_000
      assert info.is_active == true
      assert info.incoming == 100
      assert info.outgoing == 50
    end

    test "non-array JSON returns empty list" do
      json = Jason.encode!(%{"key" => "value"})
      assert {:ok, []} = ChannelInfo.from_json(json)
    end

    test "invalid JSON returns error tuple" do
      assert {:error, _reason} = ChannelInfo.from_json("not valid json{")
    end

    test "nested stats with messages key extracted" do
      json =
        Jason.encode!([
          %{
            "name" => "q1",
            "type" => "queues",
            "incoming" => %{"messages" => 200, "volume" => 1024},
            "outgoing" => %{"messages" => 150}
          }
        ])

      assert {:ok, [info]} = ChannelInfo.from_json(json)
      assert info.incoming == 200
      assert info.outgoing == 150
    end

    test "missing fields use defaults" do
      json = Jason.encode!([%{}])

      assert {:ok, [info]} = ChannelInfo.from_json(json)
      assert info.name == ""
      assert info.type == ""
      assert info.last_activity == 0
      assert info.is_active == false
      assert info.incoming == 0
      assert info.outgoing == 0
    end
  end
end
