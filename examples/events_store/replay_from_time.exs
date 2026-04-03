unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Replay from time — start reading from a specific timestamp

channel = "es.from-time"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-time")

# Store some old events
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Old event #{i}"))
end

# Record the timestamp, then store new events
cutoff = System.system_time(:second)
Process.sleep(1_000)

for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "New event #{i}"))
end

IO.puts("Subscribing from timestamp #{cutoff}...")
parent = self()

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: {:start_at_time, cutoff},
    on_event: fn event ->
      IO.puts("Replayed: #{event.body}")
      send(parent, :replayed)
    end
  )

for _ <- 1..3 do
  receive do
    :replayed -> :ok
  after
    5_000 -> :timeout
  end
end

IO.puts("Replayed events from cutoff time")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
