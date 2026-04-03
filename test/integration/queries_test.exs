defmodule KubeMQ.Integration.QueriesTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-queries-#{System.unique_integer([:positive])}"

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-queries-test"
      )

    on_exit(fn ->
      if Process.alive?(client), do: Client.close(client)
    end)

    %{client: client}
  end

  describe "queries request/response" do
    test "subscribe -> send -> receive response with metadata", %{client: client} do
      channel = unique_channel()

      # Subscribe to queries and respond with data
      {:ok, _sub} =
        Client.subscribe_to_queries(client, channel,
          on_query: fn query_receive ->
            KubeMQ.QueryReply.new(
              request_id: query_receive.id,
              response_to: query_receive.reply_channel,
              executed: true,
              body: "query result data",
              metadata: "response-metadata"
            )
          end
        )

      Process.sleep(500)

      # Send a query
      query = %KubeMQ.Query{
        channel: channel,
        body: "get data",
        timeout: 10_000,
        client_id: "integration-queries-test",
        metadata: "request-metadata"
      }

      {:ok, response} = Client.send_query(client, query)
      assert response.executed == true
      assert response.body == "query result data"
      assert response.metadata == "response-metadata"
    end

    test "query with error response", %{client: client} do
      channel = unique_channel()

      {:ok, _sub} =
        Client.subscribe_to_queries(client, channel,
          on_query: fn query_receive ->
            KubeMQ.QueryReply.new(
              request_id: query_receive.id,
              response_to: query_receive.reply_channel,
              executed: false,
              error: "query processing failed"
            )
          end
        )

      Process.sleep(500)

      query = %KubeMQ.Query{
        channel: channel,
        body: "bad query",
        timeout: 10_000,
        client_id: "integration-queries-test"
      }

      {:ok, response} = Client.send_query(client, query)
      assert response.executed == false
      assert response.error == "query processing failed"
    end

    test "query with tags round-trips through subscriber", %{client: client} do
      channel = unique_channel()

      {:ok, _sub} =
        Client.subscribe_to_queries(client, channel,
          on_query: fn query_receive ->
            KubeMQ.QueryReply.new(
              request_id: query_receive.id,
              response_to: query_receive.reply_channel,
              executed: true,
              body: query_receive.body,
              tags: query_receive.tags
            )
          end
        )

      Process.sleep(500)

      query = %KubeMQ.Query{
        channel: channel,
        body: "echo body",
        timeout: 10_000,
        client_id: "integration-queries-test",
        tags: %{"request_id" => "r-123"}
      }

      {:ok, response} = Client.send_query(client, query)
      assert response.executed == true
      assert response.body == "echo body"
    end
  end
end
