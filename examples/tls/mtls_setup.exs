unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# mTLS setup — mutual TLS with client certificate authentication

ca_cert = System.get_env("KUBEMQ_CA_CERT", "/path/to/ca.pem")
client_cert = System.get_env("KUBEMQ_CLIENT_CERT", "/path/to/client.pem")
client_key = System.get_env("KUBEMQ_CLIENT_KEY", "/path/to/client-key.pem")

IO.puts("Connecting with mTLS (mutual TLS)...")
IO.puts("  CA cert: #{ca_cert}")
IO.puts("  Client cert: #{client_cert}")
IO.puts("  Client key: #{client_key}")

case KubeMQ.Client.start_link(
       address: "localhost:50000",
       client_id: "elixir-mtls-example",
       tls: [
         cacertfile: ca_cert,
         certfile: client_cert,
         keyfile: client_key,
         verify: :verify_peer
       ]
     ) do
  {:ok, client} ->
    IO.puts("mTLS connection established!")

    case KubeMQ.Client.ping(client) do
      {:ok, info} -> IO.puts("Server version: #{info.version}")
      {:error, err} -> IO.puts("Ping failed: #{err.message}")
    end

    KubeMQ.Client.close(client)

  {:error, reason} ->
    IO.puts("mTLS connection failed: #{inspect(reason)}")
    IO.puts("Ensure all certificate paths are correct and broker has mTLS enabled.")
end
