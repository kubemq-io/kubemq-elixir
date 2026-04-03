unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Command consumer group — load-balance command processing

channel = "commands.group-demo"
group = "cmd-workers"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-cmd-group")

# Two command handlers in the same group
{:ok, sub1} =
  KubeMQ.Client.subscribe_to_commands(client, channel,
    group: group,
    on_command: fn cmd ->
      IO.puts("[Worker-1] Handling: #{cmd.body}")
      KubeMQ.CommandReply.new(
        request_id: cmd.id, response_to: cmd.reply_channel, executed: true)
    end
  )

{:ok, sub2} =
  KubeMQ.Client.subscribe_to_commands(client, channel,
    group: group,
    on_command: fn cmd ->
      IO.puts("[Worker-2] Handling: #{cmd.body}")
      KubeMQ.CommandReply.new(
        request_id: cmd.id, response_to: cmd.reply_channel, executed: true)
    end
  )

IO.puts("Two command workers in group '#{group}' ready")
Process.sleep(500)

# Send commands — each goes to one worker
for i <- 1..4 do
  cmd = KubeMQ.Command.new(channel: channel, body: "Task #{i}", timeout: 10_000)

  case KubeMQ.Client.send_command(client, cmd) do
    {:ok, resp} -> IO.puts("Task #{i} executed: #{resp.executed}")
    {:error, err} -> IO.puts("Task #{i} failed: #{err.message}")
  end
end

KubeMQ.Subscription.cancel(sub1)
KubeMQ.Subscription.cancel(sub2)
KubeMQ.Client.close(client)
