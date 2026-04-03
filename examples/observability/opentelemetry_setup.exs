unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Telemetry handler setup — attach handlers to KubeMQ telemetry events

# Attach telemetry handlers before starting the client
:telemetry.attach_many(
  "kubemq-logger",
  [
    [:kubemq, :client, :ping, :stop],
    [:kubemq, :client, :send_event, :start],
    [:kubemq, :client, :send_event, :stop],
    [:kubemq, :client, :send_event, :exception],
    [:kubemq, :client, :send_command, :stop],
    [:kubemq, :client, :send_query, :stop],
    [:kubemq, :client, :send_queue_message, :stop],
    [:kubemq, :client, :poll_queue, :stop]
  ],
  fn event, measurements, metadata, _config ->
    [_, _, action, phase] = event

    IO.puts("[Telemetry] #{action}:#{phase}")
    IO.puts("  measurements: #{inspect(measurements)}")
    IO.puts("  metadata: #{inspect(Map.take(metadata, [:operation, :channel, :client_id]))}")
  end,
  nil
)

IO.puts("Telemetry handlers attached")

# Now use the client — telemetry events will fire
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-telemetry")

IO.puts("\n--- Ping ---")
KubeMQ.Client.ping(client)

IO.puts("\n--- Send Event ---")
KubeMQ.Client.send_event(client,
  KubeMQ.Event.new(channel: "telemetry.demo", body: "traced event"))

Process.sleep(500)
IO.puts("\nTelemetry events logged above.")

KubeMQ.Client.close(client)
