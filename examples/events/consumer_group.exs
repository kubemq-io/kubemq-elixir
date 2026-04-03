unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Consumer group — only one subscriber in the group receives each event

channel = "events.group-demo"
group = "my-consumer-group"

{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-group")
parent = self()

# Start two subscribers in the same group
{:ok, sub1} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    group: group,
    on_event: fn event ->
      IO.puts("[Sub-1] Got: #{event.body}")
      send(parent, {:sub1, event.body})
    end
  )

{:ok, sub2} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    group: group,
    on_event: fn event ->
      IO.puts("[Sub-2] Got: #{event.body}")
      send(parent, {:sub2, event.body})
    end
  )

IO.puts("Two subscribers in group '#{group}' ready")
Process.sleep(500)

# Send multiple events — each goes to exactly one subscriber
for i <- 1..6 do
  event = KubeMQ.Event.new(channel: channel, body: "Event #{i}")
  :ok = KubeMQ.Client.send_event(client, event)
end

IO.puts("Sent 6 events. Waiting for distribution...")
Process.sleep(2_000)

KubeMQ.Subscription.cancel(sub1)
KubeMQ.Subscription.cancel(sub2)
KubeMQ.Client.close(client)
IO.puts("Done. Each event was delivered to only one subscriber in the group.")
