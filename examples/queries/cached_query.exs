unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Cached query — server-side caching for repeated queries

channel = "queries.cached-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-query-cache")

call_count = :counters.new(1, [:atomics])

{:ok, sub} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    on_query: fn query ->
      :counters.add(call_count, 1, 1)
      IO.puts("[Handler] Processing query (call ##{:counters.get(call_count, 1)}): #{query.body}")

      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: "Result for #{query.body} at #{System.system_time(:second)}"
      )
    end
  )

Process.sleep(500)

# First query — hits the handler, result gets cached
query = KubeMQ.Query.new(
  channel: channel,
  body: "product-info",
  timeout: 10_000,
  cache_key: "product-info-cache",
  cache_ttl: 60_000
)

case KubeMQ.Client.send_query(client, query) do
  {:ok, resp} ->
    IO.puts("First call: #{resp.body} (cache_hit: #{resp.cache_hit})")

  {:error, err} ->
    IO.puts("Error: #{err.message}")
end

# Second query with same cache key — should hit cache
Process.sleep(500)

case KubeMQ.Client.send_query(client, query) do
  {:ok, resp} ->
    IO.puts("Second call: #{resp.body} (cache_hit: #{resp.cache_hit})")

  {:error, err} ->
    IO.puts("Error: #{err.message}")
end

IO.puts("Handler was called #{:counters.get(call_count, 1)} time(s)")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
