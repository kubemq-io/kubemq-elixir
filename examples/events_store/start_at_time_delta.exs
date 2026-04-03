unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Start at time delta — replay events from N milliseconds ago

channel = "es.time-delta"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-delta")

# Store some events
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Recent event #{i}"))
end

IO.puts("Subscribing with time delta of 60 seconds (60_000ms)...")
parent = self()

# Subscribe from 60 seconds ago (60_000 milliseconds)
{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: {:start_at_time_delta, 60_000},
    on_event: fn event ->
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

IO.puts("Replayed events from the last 60 seconds")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
