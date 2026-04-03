defmodule KubeMQ.QueryTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Query

  describe "new/0" do
    test "returns struct with default values" do
      query = Query.new()
      assert %Query{} = query
      assert query.id == nil
      assert query.channel == nil
      assert query.metadata == nil
      assert query.body == nil
      assert query.timeout == 10_000
      assert query.client_id == nil
      assert query.cache_key == nil
      assert query.cache_ttl == nil
      assert query.tags == %{}
      assert query.span == nil
    end
  end

  describe "new/1" do
    test "sets all fields including cache options" do
      query =
        Query.new(
          id: "q-1",
          channel: "products.lookup",
          metadata: "meta",
          body: "sku-123",
          timeout: 5_000,
          client_id: "client-1",
          cache_key: "product:sku-123",
          cache_ttl: 60_000,
          tags: %{"env" => "prod"},
          span: <<7>>
        )

      assert query.id == "q-1"
      assert query.channel == "products.lookup"
      assert query.metadata == "meta"
      assert query.body == "sku-123"
      assert query.timeout == 5_000
      assert query.client_id == "client-1"
      assert query.cache_key == "product:sku-123"
      assert query.cache_ttl == 60_000
      assert query.tags == %{"env" => "prod"}
      assert query.span == <<7>>
    end
  end
end
