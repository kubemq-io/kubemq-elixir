unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Multiple subscribers — each subscriber gets a copy of every event (fan-out)

channel = "events.multi-sub"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-multi")
parent = self()

# Start 3 independent subscribers (no group = each gets all events)
subs =
  for i <- 1..3 do
    {:ok, sub} =
      KubeMQ.Client.subscribe_to_events(client, channel,
        on_event: fn event ->
          IO.puts("[Subscriber #{i}] #{event.body}")
          send(parent, {:sub, i})
        end
      )

    sub
  end

IO.puts("3 subscribers ready on '#{channel}'")
Process.sleep(500)

# Send one event — all 3 subscribers should receive it
event = KubeMQ.Event.new(channel: channel, body: "Broadcast message")
:ok = KubeMQ.Client.send_event(client, event)
IO.puts("Event sent. Waiting for all subscribers...")

# Collect all 3 receipts
for _ <- 1..3 do
  receive do
    {:sub, id} -> IO.puts("  Confirmed from subscriber #{id}")
  after
    5_000 -> IO.puts("  Timeout")
  end
end

Enum.each(subs, &KubeMQ.Subscription.cancel/1)
KubeMQ.Client.close(client)
IO.puts("Done.")
