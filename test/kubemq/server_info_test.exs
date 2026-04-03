defmodule KubeMQ.ServerInfoTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.ServerInfo

  alias KubeMQ.ServerInfo

  describe "from_ping_result/1" do
    test "maps all fields from ping result" do
      ping_result = %{
        host: "kubemq-node-1",
        version: "2.5.0",
        server_start_time: 1_700_000_000,
        server_up_time_seconds: 3600
      }

      info = ServerInfo.from_ping_result(ping_result)
      assert %ServerInfo{} = info
      assert info.host == "kubemq-node-1"
      assert info.version == "2.5.0"
      assert info.server_start_time == 1_700_000_000
      assert info.server_up_time_seconds == 3600
    end

    test "missing timestamps default to 0" do
      ping_result = %{host: "node", version: "1.0"}

      info = ServerInfo.from_ping_result(ping_result)
      assert info.host == "node"
      assert info.version == "1.0"
      assert info.server_start_time == 0
      assert info.server_up_time_seconds == 0
    end
  end
end
