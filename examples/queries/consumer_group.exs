unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Query consumer group — load-balance query processing

channel = "queries.group-demo"
group = "query-workers"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-query-group")

# Two query handlers in the same group
{:ok, sub1} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    group: group,
    on_query: fn query ->
      IO.puts("[Worker-1] Handling: #{query.body}")
      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: "Response from Worker-1"
      )
    end
  )

{:ok, sub2} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    group: group,
    on_query: fn query ->
      IO.puts("[Worker-2] Handling: #{query.body}")
      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: "Response from Worker-2"
      )
    end
  )

IO.puts("Two query workers in group '#{group}' ready")
Process.sleep(500)

for i <- 1..4 do
  query = KubeMQ.Query.new(channel: channel, body: "Query #{i}", timeout: 10_000)

  case KubeMQ.Client.send_query(client, query) do
    {:ok, resp} -> IO.puts("Query #{i} → #{resp.body}")
    {:error, err} -> IO.puts("Query #{i} failed: #{err.message}")
  end
end

KubeMQ.Subscription.cancel(sub1)
KubeMQ.Subscription.cancel(sub2)
KubeMQ.Client.close(client)
