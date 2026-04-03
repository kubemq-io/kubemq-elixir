unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Stream receive — poll queue messages via downstream stream

channel = "qs.stream-recv"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-recv")

# First, send some messages
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: channel, body: "Queued item #{i}"))
end

IO.puts("Sent 3 messages")

# Poll via stream API
case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 10,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("Polled #{length(poll.messages)} messages")

    Enum.each(poll.messages, fn msg ->
      IO.puts("  Channel: #{msg.channel}")
      IO.puts("  Body: #{msg.body}")
    end)

    # Acknowledge all received messages
    {:ok, _} = KubeMQ.PollResponse.ack_all(poll)
    IO.puts("All messages acknowledged")

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

KubeMQ.Client.close(client)
