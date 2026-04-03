unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Delete a channel

{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-mgmt-delete")

# Create a channel first
channel_name = "mgmt.to-delete"
:ok = KubeMQ.Client.create_channel(client, channel_name, :events)
IO.puts("Created channel '#{channel_name}'")

# Delete it
case KubeMQ.Client.delete_channel(client, channel_name, :events) do
  :ok -> IO.puts("Deleted channel '#{channel_name}'")
  {:error, err} -> IO.puts("Delete failed: #{err.message}")
end

# Convenience alias — create and delete an events store channel
:ok = KubeMQ.Client.create_events_store_channel(client, "mgmt.es-delete-test")

case KubeMQ.Client.delete_events_store_channel(client, "mgmt.es-delete-test") do
  :ok -> IO.puts("Deleted events store channel via convenience alias")
  {:error, err} -> IO.puts("Delete failed: #{err.message}")
end

KubeMQ.Client.close(client)
