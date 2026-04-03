unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Fan-out pattern — one publisher, multiple independent subscribers

channel = "patterns.fan-out"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-fan-out")
parent = self()

# Start 3 independent services (no consumer group = each gets all messages)
services = ["email-service", "analytics-service", "audit-service"]

subs =
  Enum.map(services, fn service ->
    {:ok, sub} =
      KubeMQ.Client.subscribe_to_events(client, channel,
        on_event: fn event ->
          IO.puts("[#{service}] Processing: #{event.body}")
          send(parent, {:processed, service})
        end
      )

    {service, sub}
  end)

IO.puts("#{length(services)} services subscribed (fan-out)")
Process.sleep(500)

# Publish a single event — all services receive it
event = KubeMQ.Event.new(
  channel: channel,
  body: "New user registered: user@example.com",
  tags: %{"event_type" => "user.registered"}
)

:ok = KubeMQ.Client.send_event(client, event)
IO.puts("\nPublished event. Waiting for all services to process...")

# Wait for all 3 services to acknowledge
received = for _ <- 1..3 do
  receive do
    {:processed, service} -> service
  after
    5_000 -> "timeout"
  end
end

IO.puts("Processed by: #{Enum.join(received, ", ")}")

Enum.each(subs, fn {_, sub} -> KubeMQ.Subscription.cancel(sub) end)
KubeMQ.Client.close(client)
