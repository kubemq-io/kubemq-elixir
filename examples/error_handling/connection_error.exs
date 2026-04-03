unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Connection error handling — gracefully handle unreachable servers

IO.puts("Attempting connection to unreachable server...")

case KubeMQ.Client.start_link(
       address: "localhost:99999",
       client_id: "elixir-error-example"
     ) do
  {:ok, client} ->
    IO.puts("Connected (unexpected for this demo)")
    KubeMQ.Client.close(client)

  {:error, err} when is_exception(err) ->
    IO.puts("KubeMQ error caught:")
    IO.puts("  Code: #{Map.get(err, :code)}")
    IO.puts("  Message: #{Exception.message(err)}")
    IO.puts("  Operation: #{Map.get(err, :operation)}")

  {:error, reason} ->
    IO.puts("Connection error: #{inspect(reason)}")
end

IO.puts("\nAttempting operation on a valid connection with bad channel...")

case KubeMQ.Client.start_link(
       address: "localhost:50000",
       client_id: "elixir-error-example-2"
     ) do
  {:ok, client} ->
    # Send with empty channel — validation error
    bad_event = %KubeMQ.Event{channel: "", body: "test"}

    case KubeMQ.Client.send_event(client, bad_event) do
      :ok -> IO.puts("Sent (unexpected)")
      {:error, err} -> IO.puts("Validation error: #{err.message} (code: #{err.code})")
    end

    KubeMQ.Client.close(client)

  {:error, reason} ->
    IO.puts("Could not connect: #{inspect(reason)}")
end
