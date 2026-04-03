unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Connect with custom timeout and keepalive configuration

IO.puts("Connecting with custom timeouts...")

{:ok, client} =
  KubeMQ.Client.start_link(
    address: "localhost:50000",
    client_id: "elixir-timeout-example",
    rpc_timeout: 15_000,
    reconnect_policy: [initial_delay: 2_000, max_attempts: 10],
    default_cache_ttl: 300_000
  )

IO.puts("Connected with custom timeout configuration!")
IO.puts("Connection state: #{KubeMQ.Client.connection_state(client)}")

case KubeMQ.Client.ping(client) do
  {:ok, info} -> IO.puts("Server version: #{info.version}")
  {:error, err} -> IO.puts("Ping failed: #{err.message}")
end

KubeMQ.Client.close(client)
IO.puts("Done.")
