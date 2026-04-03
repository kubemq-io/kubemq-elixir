unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Send a query — request/response pattern with data return

channel = "queries.product-lookup"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-query-sender")

# Set up a query handler
{:ok, sub} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    on_query: fn query ->
      IO.puts("[Handler] Query received: #{query.body}")

      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: ~s({"name": "Widget", "price": 29.99}),
        metadata: "application/json"
      )
    end
  )

Process.sleep(500)

# Send a query
query = KubeMQ.Query.new(
  channel: channel,
  body: "sku-12345",
  timeout: 10_000
)

case KubeMQ.Client.send_query(client, query) do
  {:ok, response} ->
    IO.puts("Query executed: #{response.executed}")
    IO.puts("Response body: #{response.body}")
    IO.puts("Response metadata: #{response.metadata}")

  {:error, err} ->
    IO.puts("Query failed: #{err.message}")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
