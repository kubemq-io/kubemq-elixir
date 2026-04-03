defmodule KubeMQ.QueryResponseTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueryResponse

  describe "new/0" do
    test "returns struct with default values" do
      resp = QueryResponse.new()
      assert %QueryResponse{} = resp
      assert resp.query_id == ""
      assert resp.executed == false
      assert resp.executed_at == 0
      assert resp.metadata == nil
      assert resp.body == nil
      assert resp.cache_hit == false
      assert resp.error == nil
      assert resp.tags == %{}
    end
  end

  describe "from_response/1" do
    test "maps response fields to struct" do
      response = %{
        request_id: "qr-1",
        executed: true,
        timestamp: 1_700_000_000,
        metadata: "meta",
        body: <<1, 2>>,
        cache_hit: true,
        error: "something failed",
        tags: %{"key" => "val"}
      }

      result = QueryResponse.from_response(response)

      assert %QueryResponse{} = result
      assert result.query_id == "qr-1"
      assert result.executed == true
      assert result.executed_at == 1_700_000_000
      assert result.metadata == "meta"
      assert result.body == <<1, 2>>
      assert result.cache_hit == true
      assert result.error == "something failed"
      assert result.tags == %{"key" => "val"}
    end
  end

  describe "from_response/1 empty error" do
    test "empty string error becomes nil" do
      response = %{
        request_id: "qr-2",
        executed: true,
        timestamp: 0,
        metadata: nil,
        body: nil,
        cache_hit: false,
        error: "",
        tags: %{}
      }

      result = QueryResponse.from_response(response)
      assert result.error == nil
    end

    test "nil error stays nil" do
      response = %{
        request_id: "qr-3",
        executed: true,
        timestamp: 0,
        metadata: nil,
        body: nil,
        cache_hit: false,
        error: nil,
        tags: %{}
      }

      result = QueryResponse.from_response(response)
      assert result.error == nil
    end
  end

  describe "from_response/1 cache_hit" do
    test "cache_hit is preserved from response" do
      response = %{
        request_id: "qr-4",
        executed: true,
        timestamp: 0,
        metadata: nil,
        body: nil,
        cache_hit: true,
        error: nil,
        tags: %{}
      }

      result = QueryResponse.from_response(response)
      assert result.cache_hit == true
    end
  end
end
