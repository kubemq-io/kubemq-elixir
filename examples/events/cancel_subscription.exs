unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Cancel a subscription — demonstrates subscription lifecycle

channel = "events.cancel-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-cancel")

{:ok, sub} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    on_event: fn event -> IO.puts("Got event: #{event.body}") end
  )

IO.puts("Subscription active? #{KubeMQ.Subscription.active?(sub)}")

# Send an event (should be received)
:ok = KubeMQ.Client.send_event(client, KubeMQ.Event.new(channel: channel, body: "Before cancel"))
Process.sleep(500)

# Cancel the subscription
:ok = KubeMQ.Subscription.cancel(sub)
IO.puts("Subscription cancelled. Active? #{KubeMQ.Subscription.active?(sub)}")

# Send another event (won't be received)
:ok = KubeMQ.Client.send_event(client, KubeMQ.Event.new(channel: channel, body: "After cancel"))
Process.sleep(500)

IO.puts("Second event sent but no subscriber to receive it.")
KubeMQ.Client.close(client)
