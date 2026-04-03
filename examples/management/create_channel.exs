unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Create channels of various types

{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-mgmt-create")

channels = [
  {"mgmt.events-ch", :events},
  {"mgmt.es-ch", :events_store},
  {"mgmt.cmd-ch", :commands},
  {"mgmt.query-ch", :queries},
  {"mgmt.queue-ch", :queues}
]

for {name, type} <- channels do
  case KubeMQ.Client.create_channel(client, name, type) do
    :ok -> IO.puts("Created #{type} channel '#{name}'")
    {:error, err} -> IO.puts("Failed to create '#{name}': #{err.message}")
  end
end

IO.puts("\nAll channels created.")
KubeMQ.Client.close(client)
