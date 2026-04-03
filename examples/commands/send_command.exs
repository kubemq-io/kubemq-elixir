unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Send a command — fire-and-wait RPC pattern

channel = "commands.process-order"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-cmd-sender")

# Set up a command handler first
{:ok, sub} =
  KubeMQ.Client.subscribe_to_commands(client, channel,
    on_command: fn cmd ->
      IO.puts("[Handler] Received command: #{cmd.body}")
      KubeMQ.CommandReply.new(
        request_id: cmd.id,
        response_to: cmd.reply_channel,
        executed: true
      )
    end
  )

Process.sleep(500)

# Send a command and wait for the response
command = KubeMQ.Command.new(
  channel: channel,
  body: "process order #1234",
  timeout: 10_000
)

case KubeMQ.Client.send_command(client, command) do
  {:ok, response} ->
    IO.puts("Command executed: #{response.executed}")
    IO.puts("Command ID: #{response.command_id}")

  {:error, err} ->
    IO.puts("Command failed: #{err.message}")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
