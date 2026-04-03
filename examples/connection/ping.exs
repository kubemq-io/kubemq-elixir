unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Ping the KubeMQ server and display server information

IO.puts("Connecting to KubeMQ server...")
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-ping-example")

case KubeMQ.Client.ping(client) do
  {:ok, info} ->
    IO.puts("Ping successful!")
    IO.puts("  Host: #{info.host}")
    IO.puts("  Version: #{info.version}")
    IO.puts("  Server start time: #{info.server_start_time}")
    IO.puts("  Server up time: #{info.server_up_time_seconds}s")

  {:error, error} ->
    IO.puts("Ping failed: #{error.message}")
end

KubeMQ.Client.close(client)
