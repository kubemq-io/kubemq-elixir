unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Nack all — reject all messages in a poll transaction (re-queued for retry)

channel = "qs.nack-all"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-nackall")

# Send messages
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Retriable msg #{i}"))
end

IO.puts("Sent 3 messages")

# Poll and nack all (simulating a processing failure)
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 10,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("Polled #{length(poll.messages)} messages")
    IO.puts("Simulating processing failure — nacking all...")

    case KubeMQ.PollResponse.nack_all(poll) do
      {:ok, _} -> IO.puts("All messages nacked (returned to queue for retry)")
      {:error, err} -> IO.puts("Nack failed: #{err.message}")
    end

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

# Messages should be available again
Process.sleep(500)

case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 3_000
     ) do
  {:ok, result} ->
    IO.puts("After nack, #{result.messages_received} messages available again")

  {:error, _} ->
    IO.puts("No messages available")
end

KubeMQ.Client.close(client)
