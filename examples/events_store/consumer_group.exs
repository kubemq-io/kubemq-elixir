unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Events Store consumer group — load-balance persistent event delivery

channel = "es.group-demo"
group = "es-processing-group"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-es-group")

parent = self()

# Two subscribers in the same group
{:ok, sub1} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_new_only,
    group: group,
    on_event: fn event ->
      IO.puts("[Worker-1] #{event.body}")
      send(parent, {:worker, 1})
    end
  )

{:ok, sub2} =
  KubeMQ.Client.subscribe_to_events_store(client, channel,
    start_at: :start_new_only,
    group: group,
    on_event: fn event ->
      IO.puts("[Worker-2] #{event.body}")
      send(parent, {:worker, 2})
    end
  )

IO.puts("Two workers in group '#{group}' ready")
Process.sleep(500)

# Send events — distributed across group members
for i <- 1..6 do
  {:ok, _} = KubeMQ.Client.send_event_store(client,
    KubeMQ.EventStore.new(channel: channel, body: "Task #{i}"))
end

IO.puts("Sent 6 tasks. Waiting for distribution...")
Process.sleep(2_000)

KubeMQ.Subscription.cancel(sub1)
KubeMQ.Subscription.cancel(sub2)
KubeMQ.Client.close(client)
