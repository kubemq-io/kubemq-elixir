unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Connect and close — demonstrates clean lifecycle management

IO.puts("Connecting to KubeMQ server...")
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-close-example")
IO.puts("Connected. State: #{KubeMQ.Client.connection_state(client)}")

IO.puts("Closing connection...")
KubeMQ.Client.close(client)
IO.puts("Connection closed. Process alive? #{Process.alive?(client)}")
