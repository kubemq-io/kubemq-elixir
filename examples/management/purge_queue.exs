unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Purge a queue — remove all pending messages

channel = "mgmt.purge-demo"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-mgmt-purge")

# Send messages to the queue
for i <- 1..10 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Purgeable #{i}"))
end

IO.puts("Sent 10 messages to '#{channel}'")
Process.sleep(1_000)

# Purge the queue
try do
  case KubeMQ.Client.purge_queue_channel(client, channel) do
    :ok -> IO.puts("Queue purged successfully!")
    {:error, err} -> IO.puts("Purge failed: #{err.message}")
  end
catch
  :exit, {:timeout, _} ->
    IO.puts("Purge timed out (server may still be processing)")
end

# Verify queue is empty
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 100,
       wait_timeout: 2_000
     ) do
  {:ok, poll} ->
    IO.puts("Messages after purge: #{length(poll.messages)}")
    if poll.messages != [], do: KubeMQ.PollResponse.ack_all(poll)

  {:error, _} ->
    IO.puts("Queue is empty (confirmed)")
end

KubeMQ.Client.close(client)
