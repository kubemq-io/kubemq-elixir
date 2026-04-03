unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Basic queue send/receive — simple pull-based messaging

channel = "queues.basic"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-basic")

# Send a message to the queue
msg = KubeMQ.QueueMessage.new(channel: channel, body: "Hello Queue!", metadata: "order-data")

case KubeMQ.Client.send_queue_message(client, msg) do
  {:ok, result} ->
    IO.puts("Message sent! ID: #{result.message_id}")

  {:error, err} ->
    IO.puts("Send failed: #{err.message}")
end

# Receive the message
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 1,
       wait_timeout: 5_000
     ) do
  {:ok, result} ->
    IO.puts("Received #{result.messages_received} message(s)")

    Enum.each(result.messages, fn msg ->
      IO.puts("  Body: #{msg.body}")
      IO.puts("  Metadata: #{msg.metadata}")
    end)

  {:error, err} ->
    IO.puts("Receive failed: #{err.message}")
end

KubeMQ.Client.close(client)
