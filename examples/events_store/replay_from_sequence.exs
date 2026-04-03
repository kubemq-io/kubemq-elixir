unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Replay from sequence — start reading from a specific sequence number

channel = "es.from-seq"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-seq")

# Store events to build up sequence numbers
for i <- 1..5 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Event #{i}"))
end

IO.puts("Stored 5 events. Subscribing from sequence 3...")
parent = self()

# Subscribe from sequence 3 — skips events 1 and 2
{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: {:start_at_sequence, 3},
    on_event: fn event ->
      IO.puts("Replayed seq #{event.sequence}: #{event.body}")
      send(parent, :replayed)
    end
  )

# Expect events 3, 4, 5
for _ <- 1..3 do
  receive do
    :replayed -> :ok
  after
    5_000 -> :timeout
  end
end

IO.puts("Replayed from sequence 3 onwards")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
