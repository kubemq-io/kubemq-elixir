unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Stream sending for Events Store — efficient batch sending with confirmation
# Demonstrates sending multiple events in rapid succession for persistence.

channel = "es.stream-send"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-stream")

IO.puts("Sending 5 events to Event Store...")

for i <- 1..5 do
  event = KubeMQ.EventStore.new(channel: channel, body: "Streamed store event #{i}")

  case KubeMQ.Client.send_event_store(client, event) do
    {:ok, result} ->
      IO.puts("Event #{i} confirmed: sent=#{result.sent}")

    {:error, err} ->
      IO.puts("Event #{i} failed: #{err.message}")
  end

  Process.sleep(100)
end

KubeMQ.Client.close(client)
IO.puts("All events persisted. Done.")
