unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Basic persistent pub/sub — events are stored and can be replayed

channel = "es.persistent"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-basic")

# Publish a persistent event
event = KubeMQ.EventStore.new(channel: channel, body: "Persistent event", metadata: "audit")

case KubeMQ.Client.send_event_store(client, event) do
  {:ok, result} ->
    IO.puts("Event stored! Sent: #{result.sent}")

  {:error, err} ->
    IO.puts("Store failed: #{err.message}")
end

# Subscribe to receive new events
parent = self()

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_new_only,
    on_event: fn event ->
      IO.puts("Received stored event: #{event.body}")
      send(parent, :got_it)
    end
  )

Process.sleep(500)

# Send another event — subscriber will receive it
event2 = KubeMQ.EventStore.new(channel: channel, body: "Second persistent event")
{:ok, _} = KubeMQ.Client.send_event_store(client, event2)

receive do
  :got_it -> IO.puts("Event delivery confirmed!")
after
  5_000 -> IO.puts("Timeout")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
