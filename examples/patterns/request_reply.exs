unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Request/reply pattern — synchronous RPC using queries

channel = "patterns.request-reply"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-req-reply")

# Service: handles requests and returns replies
{:ok, sub} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    on_query: fn query ->
      IO.puts("[Service] Processing request: #{query.body}")

      # Simulate a database lookup
      response_data =
        case query.body do
          "user:" <> id ->
            ~s({"id": "#{id}", "name": "Alice", "status": "active"})

          _ ->
            ~s({"error": "unknown request"})
        end

      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: response_data,
        metadata: "application/json"
      )
    end
  )

IO.puts("Service ready")
Process.sleep(500)

# Client: sends requests and receives responses
for user_id <- ["u-001", "u-002", "u-003"] do
  query = KubeMQ.Query.new(
    channel: channel,
    body: "user:#{user_id}",
    timeout: 10_000
  )

  case KubeMQ.Client.send_query(client, query) do
    {:ok, resp} ->
      IO.puts("Request user:#{user_id} → #{resp.body}")

    {:error, err} ->
      IO.puts("Request failed: #{err.message}")
  end
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
