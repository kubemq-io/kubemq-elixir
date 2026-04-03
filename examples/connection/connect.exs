unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Basic connection to KubeMQ server

IO.puts("Connecting to KubeMQ server...")

case KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-connect-example") do
  {:ok, client} ->
    IO.puts("Connected successfully!")
    IO.puts("Connection state: #{KubeMQ.Client.connection_state(client)}")
    IO.puts("Connected? #{KubeMQ.Client.connected?(client)}")
    KubeMQ.Client.close(client)
    IO.puts("Done.")

  {:error, reason} ->
    IO.puts("Connection failed: #{inspect(reason)}")
end
