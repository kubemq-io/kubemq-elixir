unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Cancel an Events Store subscription

channel = "es.cancel-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-cancel")

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_new_only,
    on_event: fn event -> IO.puts("Got: #{event.body}") end
  )

IO.puts("Subscription active? #{KubeMQ.Subscription.active?(sub)}")

# Send and receive one event
{:ok, _} = KubeMQ.Client.send_event_store(client,
  KubeMQ.EventStore.new(channel: channel, body: "Before cancel"))
Process.sleep(500)

# Cancel
:ok = KubeMQ.Subscription.cancel(sub)
IO.puts("Cancelled. Active? #{KubeMQ.Subscription.active?(sub)}")

# This event won't be received
{:ok, _} = KubeMQ.Client.send_event_store(client,
  KubeMQ.EventStore.new(channel: channel, body: "After cancel"))
Process.sleep(500)
IO.puts("Post-cancel event sent but not received.")

KubeMQ.Client.close(client)
