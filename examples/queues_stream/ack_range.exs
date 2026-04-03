unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Ack range — selectively acknowledge specific messages by sequence

channel = "qs.ack-range"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-ackrange")

# Send messages
for i <- 1..5 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Message #{i}"))
end

IO.puts("Sent 5 messages")

# Poll messages
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 5,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("Polled #{length(poll.messages)} messages")

    # Get sequence numbers from message attributes
    sequences =
      poll.messages
      |> Enum.filter(& &1.attributes)
      |> Enum.map(& &1.attributes.sequence)

    # Ack only the first 3 messages
    ack_seqs = Enum.take(sequences, 3)
    IO.puts("Acking sequences: #{inspect(ack_seqs)}")

    case KubeMQ.PollResponse.ack_range(poll, ack_seqs) do
      :ok -> IO.puts("Range ack successful!")
      {:error, err} -> IO.puts("Ack range failed: #{err.message}")
    end

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

KubeMQ.Client.close(client)
