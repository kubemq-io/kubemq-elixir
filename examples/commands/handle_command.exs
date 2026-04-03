unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Handle commands with auto-response — subscriber returns a reply struct

channel = "commands.handler-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-cmd-handler")

# The on_command callback returns a CommandReply — the SDK sends it automatically
{:ok, sub} =
  KubeMQ.Client.subscribe_to_commands(client, channel,
    on_command: fn cmd ->
      IO.puts("Processing command: #{cmd.body}")

      # Simulate processing
      result = String.upcase(cmd.body)
      IO.puts("Result: #{result}")

      # Return reply — SDK sends the response back to the caller
      KubeMQ.CommandReply.new(
        request_id: cmd.id,
        response_to: cmd.reply_channel,
        executed: true,
        metadata: result
      )
    end,
    on_error: fn err ->
      IO.puts("Subscription error: #{err.message}")
    end
  )

IO.puts("Command handler ready on '#{channel}'")
Process.sleep(500)

# Send a command to the handler
cmd = KubeMQ.Command.new(channel: channel, body: "hello world", timeout: 10_000)

case KubeMQ.Client.send_command(client, cmd) do
  {:ok, resp} -> IO.puts("Response: executed=#{resp.executed}")
  {:error, err} -> IO.puts("Error: #{err.message}")
end

KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
