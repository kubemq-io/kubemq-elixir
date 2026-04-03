unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Handle queries — subscriber processes queries and returns data

channel = "queries.calculator"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-query-handler")

# Query handler that performs calculations
{:ok, sub} =
  KubeMQ.Client.subscribe_to_queries(client, channel,
    on_query: fn query ->
      IO.puts("Calculating: #{query.body}")

      result =
        case query.body do
          "sum:" <> nums ->
            nums |> String.split(",") |> Enum.map(&String.to_integer(String.trim(&1))) |> Enum.sum()
          _ ->
            0
        end

      KubeMQ.QueryReply.new(
        request_id: query.id,
        response_to: query.reply_channel,
        executed: true,
        body: "#{result}"
      )
    end,
    on_error: fn err -> IO.puts("Error: #{err.message}") end
  )

IO.puts("Calculator query handler ready")
Process.sleep(500)

# Send queries
for input <- ["sum:1,2,3", "sum:10,20,30,40"] do
  query = KubeMQ.Query.new(channel: channel, body: input, timeout: 10_000)

  case KubeMQ.Client.send_query(client, query) do
    {:ok, resp} -> IO.puts("#{input} = #{resp.body}")
    {:error, err} -> IO.puts("Failed: #{err.message}")
  end
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
