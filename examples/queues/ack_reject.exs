unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Ack/reject with stream API — selective message acknowledgement

channel = "queues.ack-reject"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-ack-reject")

# Send messages
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Message #{i}"))
end

IO.puts("Sent 3 messages")

# Poll with stream API for transaction control
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 3,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("Polled #{length(poll.messages)} messages")
    IO.puts("Transaction ID: #{poll.transaction_id}")

    Enum.each(poll.messages, fn msg ->
      IO.puts("  #{msg.body}")
    end)

    # Ack all messages in this transaction
    case KubeMQ.PollResponse.ack_all(poll) do
      {:ok, _} -> IO.puts("All messages acknowledged!")
      {:error, err} -> IO.puts("Ack failed: #{err.message}")
    end

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

KubeMQ.Client.close(client)
