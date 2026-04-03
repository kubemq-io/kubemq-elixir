unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Ack all — acknowledge all pending messages on a queue

channel = "queues.ack-all"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-ackall")

# Send several messages
for i <- 1..5 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Message #{i}"))
end

IO.puts("Sent 5 messages")

# Ack all messages at once
case KubeMQ.Client.ack_all_queue_messages(client, channel, wait_timeout: 5_000) do
  {:ok, result} ->
    IO.puts("Acknowledged #{result.affected_messages} messages")
    IO.puts("Error: #{result.is_error}")

  {:error, err} ->
    IO.puts("Ack all failed: #{err.message}")
end

# Verify queue is empty
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 2_000
     ) do
  {:ok, result} ->
    IO.puts("Messages remaining: #{result.messages_received}")

  {:error, _} ->
    IO.puts("Queue is empty (as expected)")
end

KubeMQ.Client.close(client)
