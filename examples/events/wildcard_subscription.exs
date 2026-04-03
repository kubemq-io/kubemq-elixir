unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Wildcard channel subscription — subscribe to events.* pattern

{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-wildcard")
parent = self()
counter = :counters.new(1, [:atomics])

# Subscribe with wildcard pattern
{:ok, sub} =
  KubeMQ.Client.subscribe_to_events(client, "events.>",
    on_event: fn event ->
      :counters.add(counter, 1, 1)
      IO.puts("Received on channel '#{event.channel}': #{event.body}")
      send(parent, :received)
    end
  )

IO.puts("Subscribed to 'events.>' (wildcard)")
Process.sleep(500)

# Send to multiple sub-channels
for topic <- ["events.orders", "events.users", "events.logs"] do
  event = KubeMQ.Event.new(channel: topic, body: "Message to #{topic}")
  :ok = KubeMQ.Client.send_event(client, event)
  IO.puts("Sent to '#{topic}'")
end

# Wait for all messages
for _ <- 1..3 do
  receive do
    :received -> :ok
  after
    5_000 -> IO.puts("Timeout waiting")
  end
end

IO.puts("Total received: #{:counters.get(counter, 1)}")
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
