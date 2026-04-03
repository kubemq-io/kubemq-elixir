unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Basic event pub/sub — send and receive a fire-and-forget event

channel = "events.basic"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-basic")

# Subscribe first (events are not persisted, subscriber must be active)
parent = self()

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    on_event: fn event ->
      IO.puts("Received event: #{event.body}")
      send(parent, :received)
    end
  )

IO.puts("Subscribed to '#{channel}'")
Process.sleep(500)

# Publish an event
event = KubeMQ.Event.new(channel: channel, body: "Hello from Elixir!", metadata: "greeting")

case KubeMQ.Client.send_event(client, event) do
  :ok -> IO.puts("Event sent successfully")
  {:error, err} -> IO.puts("Send failed: #{err.message}")
end

# Wait for the event to arrive
receive do
  :received -> IO.puts("Event delivery confirmed!")
after
  5_000 -> IO.puts("Timeout waiting for event")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
