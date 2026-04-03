defmodule KubeMQ.CommandResponseTest do
  use ExUnit.Case, async: true

  alias KubeMQ.CommandResponse

  describe "new/0" do
    test "returns struct with default values" do
      resp = CommandResponse.new()
      assert %CommandResponse{} = resp
      assert resp.command_id == ""
      assert resp.executed == false
      assert resp.executed_at == 0
      assert resp.error == nil
      assert resp.tags == %{}
    end
  end

  describe "from_response/1" do
    test "maps response fields to struct" do
      response = %{
        request_id: "req-1",
        executed: true,
        timestamp: 1_700_000_000,
        error: "something failed",
        tags: %{"key" => "val"}
      }

      result = CommandResponse.from_response(response)

      assert %CommandResponse{} = result
      assert result.command_id == "req-1"
      assert result.executed == true
      assert result.executed_at == 1_700_000_000
      assert result.error == "something failed"
      assert result.tags == %{"key" => "val"}
    end
  end

  describe "from_response/1 empty error" do
    test "empty string error becomes nil" do
      response = %{
        request_id: "req-2",
        executed: true,
        timestamp: 0,
        error: "",
        tags: %{}
      }

      result = CommandResponse.from_response(response)
      assert result.error == nil
    end

    test "nil error stays nil" do
      response = %{
        request_id: "req-3",
        executed: true,
        timestamp: 0,
        error: nil,
        tags: %{}
      }

      result = CommandResponse.from_response(response)
      assert result.error == nil
    end
  end
end
