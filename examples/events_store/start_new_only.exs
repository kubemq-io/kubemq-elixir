unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Start new only — subscriber receives only events published after subscribing

channel = "es.start-new"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-new")

# Publish event BEFORE subscribing (won't be received)
{:ok, _} = KubeMQ.Client.send_event_store(client,
  KubeMQ.EventStore.new(channel: channel, body: "Before subscribe"))
IO.puts("Sent event before subscribing")

parent = self()

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_new_only,
    on_event: fn event ->
      IO.puts("Received: #{event.body}")
      send(parent, :received)
    end
  )

IO.puts("Subscribed with :start_new_only")
Process.sleep(500)

# Publish event AFTER subscribing (will be received)
{:ok, _} = KubeMQ.Client.send_event_store(client,
  KubeMQ.EventStore.new(channel: channel, body: "After subscribe"))

receive do
  :received -> IO.puts("Only the post-subscribe event was received!")
after
  5_000 -> IO.puts("Timeout")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
