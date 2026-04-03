unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Start from first — replay all stored events from the beginning

channel = "es.from-first"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-first")

# Store some events
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Historical event #{i}"))
end

IO.puts("Stored 3 events")
parent = self()
counter = :counters.new(1, [:atomics])

# Subscribe from first — will replay all stored events
{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_from_first,
    on_event: fn event ->
      :counters.add(counter, 1, 1)
      IO.puts("Replayed: #{event.body} (seq: #{event.sequence})")
      send(parent, :replayed)
    end
  )

# Wait for replayed events
for _ <- 1..3 do
  receive do
    :replayed -> :ok
  after
    5_000 -> :timeout
  end
end

IO.puts("Total replayed: #{:counters.get(counter, 1)}")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
