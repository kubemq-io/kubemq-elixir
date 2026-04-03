unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# TLS setup — connect with server-side TLS verification

ca_cert = System.get_env("KUBEMQ_CA_CERT", "/path/to/ca.pem")

IO.puts("Connecting with TLS (server verification)...")
IO.puts("CA cert: #{ca_cert}")

case KubeMQ.Client.start_link(
       address: "localhost:50000",
       client_id: "elixir-tls-example",
       tls: [cacertfile: ca_cert]
     ) do
  {:ok, client} ->
    IO.puts("TLS connection established!")

    case KubeMQ.Client.ping(client) do
      {:ok, info} -> IO.puts("Server version: #{info.version}")
      {:error, err} -> IO.puts("Ping failed: #{err.message}")
    end

    KubeMQ.Client.close(client)

  {:error, reason} ->
    IO.puts("TLS connection failed: #{inspect(reason)}")
    IO.puts("Make sure the CA cert path is correct and the broker has TLS enabled.")
end
