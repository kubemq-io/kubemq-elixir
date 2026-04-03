unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Auto ack — automatically acknowledge messages on receive

channel = "qs.auto-ack"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-autoack")

# Send messages
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Auto-ack msg #{i}"))
end

IO.puts("Sent 3 messages")

# Poll with auto_ack: true — messages are acked automatically by the server
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 10,
       wait_timeout: 5_000,
       auto_ack: true
     ) do
  {:ok, poll} ->
    IO.puts("Received #{length(poll.messages)} messages (auto-acked)")

    Enum.each(poll.messages, fn msg ->
      IO.puts("  #{msg.body}")
    end)

    IO.puts("No manual ack needed — server handled it")

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

# Verify queue is empty
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 2_000
     ) do
  {:ok, result} ->
    IO.puts("Remaining messages: #{result.messages_received}")

  {:error, _} ->
    IO.puts("Queue empty (confirmed)")
end

KubeMQ.Client.close(client)
