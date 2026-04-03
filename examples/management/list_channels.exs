unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# List channels by type with optional search filter

{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-mgmt-list")

for type <- [:events, :events_store, :commands, :queries, :queues] do
  case KubeMQ.Client.list_channels(client, type) do
    {:ok, channels} ->
      IO.puts("\n#{type} channels (#{length(channels)}):")

      Enum.each(channels, fn ch ->
        IO.puts("  - #{ch.name} (active: #{ch.is_active}, in: #{ch.incoming}, out: #{ch.outgoing})")
      end)

    {:error, err} ->
      IO.puts("\n#{type}: #{err.message}")
  end
end

# Search with filter
IO.puts("\n--- Filtered search ---")

case KubeMQ.Client.list_queues_channels(client, "mgmt") do
  {:ok, channels} ->
    IO.puts("Queue channels matching 'mgmt': #{length(channels)}")

  {:error, err} ->
    IO.puts("Search failed: #{err.message}")
end

KubeMQ.Client.close(client)
