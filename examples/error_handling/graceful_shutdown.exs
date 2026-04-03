unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Graceful shutdown — clean up subscriptions and connections

channel = "error.shutdown"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-shutdown")

# Start some subscriptions
{:ok, sub1} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    on_event: fn _event -> :ok end
  )

{:ok, sub2} =
  KubeMQ.Client.subscribe_to_events(client, "#{channel}.other",
    on_event: fn _event -> :ok end
  )

IO.puts("Active subscriptions: sub1=#{KubeMQ.Subscription.active?(sub1)}, sub2=#{KubeMQ.Subscription.active?(sub2)}")

# Graceful shutdown: cancel subscriptions first, then close client
IO.puts("Starting graceful shutdown...")

:ok = KubeMQ.Subscription.cancel(sub1)
IO.puts("  Sub1 cancelled")

:ok = KubeMQ.Subscription.cancel(sub2)
IO.puts("  Sub2 cancelled")

KubeMQ.Client.close(client)
IO.puts("  Client closed")

IO.puts("Shutdown complete. All resources cleaned up.")
