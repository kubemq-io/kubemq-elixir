unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Command timeout — demonstrates handling when no handler responds in time

channel = "commands.timeout-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-cmd-timeout")

# Send a command with a short timeout and NO handler subscribed
IO.puts("Sending command with 3-second timeout (no handler)...")

cmd = KubeMQ.Command.new(
  channel: channel,
  body: "this will timeout",
  timeout: 3_000
)

case KubeMQ.Client.send_command(client, cmd) do
  {:ok, response} ->
    if response.error do
      IO.puts("Command returned with error: #{response.error}")
    else
      IO.puts("Command executed: #{response.executed}")
    end

  {:error, err} ->
    IO.puts("Command timed out or failed: #{err.message}")
    IO.puts("Error code: #{err.code}")
end

IO.puts("Timeout handling complete.")
KubeMQ.Client.close(client)
