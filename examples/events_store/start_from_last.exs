unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Start from last — receive only the most recent stored event, then new ones

channel = "es.from-last"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-last")

# Store several events
for i <- 1..5 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Event #{i}"))
end

IO.puts("Stored 5 events")
parent = self()

# Subscribe from last — gets only the latest stored event
{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_from_last,
    on_event: fn event ->
      IO.puts("Received: #{event.body} (seq: #{event.sequence})")
      send(parent, :received)
    end
  )

receive do
  :received -> IO.puts("Got the last stored event!")
after
  5_000 -> IO.puts("Timeout")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
