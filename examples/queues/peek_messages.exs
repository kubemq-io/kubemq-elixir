unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Peek messages — inspect queue contents without consuming

channel = "queues.peek"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-peek")

# Send some messages
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Peekable message #{i}"))
end

IO.puts("Sent 3 messages")

# Peek — view messages without removing them
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 5_000,
       is_peek: true
     ) do
  {:ok, result} ->
    IO.puts("Peeked #{result.messages_received} messages (is_peek: #{result.is_peek})")

    Enum.each(result.messages, fn msg ->
      IO.puts("  #{msg.body}")
    end)

  {:error, err} ->
    IO.puts("Peek failed: #{err.message}")
end

# Messages are still in the queue — receive them normally
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 5_000
     ) do
  {:ok, result} ->
    IO.puts("After peek, received #{result.messages_received} messages (still available)")

  {:error, err} ->
    IO.puts("Receive failed: #{err.message}")
end

KubeMQ.Client.close(client)
