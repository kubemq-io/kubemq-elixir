unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Connect with an authentication token

auth_token = System.get_env("KUBEMQ_AUTH_TOKEN", "your-auth-token-here")
IO.puts("Connecting with auth token...")

case KubeMQ.Client.start_link(
       address: "localhost:50000",
       client_id: "elixir-auth-example",
       auth_token: auth_token
     ) do
  {:ok, client} ->
    IO.puts("Authenticated connection established!")

    case KubeMQ.Client.ping(client) do
      {:ok, info} -> IO.puts("Server version: #{info.version}")
      {:error, err} -> IO.puts("Ping failed: #{err.message}")
    end

    KubeMQ.Client.close(client)

  {:error, reason} ->
    IO.puts("Auth connection failed: #{inspect(reason)}")
end
