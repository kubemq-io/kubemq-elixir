unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Poll mode — continuously poll a queue for new messages

channel = "qs.poll-mode"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-poll")

# Send some messages in the background
Task.start(fn ->
  Process.sleep(1_000)

  for i <- 1..5 do
    {:ok, _} = KubeMQ.Client.send_queue_message(client,
      KubeMQ.QueueMessage.new(channel: channel, body: "Poll msg #{i}"))
    Process.sleep(500)
  end

  IO.puts("All messages sent")
end)

# Poll loop — process messages as they arrive
IO.puts("Starting poll loop (3 iterations)...")

for iteration <- 1..3 do
  IO.puts("\n--- Poll iteration #{iteration} ---")

  case KubeMQ.Client.poll_queue(client,
         channel: channel,
         max_items: 5,
         wait_timeout: 3_000
       ) do
    {:ok, poll} when length(poll.messages) > 0 ->
      IO.puts("Got #{length(poll.messages)} messages")

      Enum.each(poll.messages, fn msg ->
        IO.puts("  Processing: #{msg.body}")
      end)

      {:ok, _} = KubeMQ.PollResponse.ack_all(poll)
      IO.puts("  Acked all")

    {:ok, _poll} ->
      IO.puts("No messages available")

    {:error, err} ->
      IO.puts("Poll error: #{err.message}")
  end
end

KubeMQ.Client.close(client)
IO.puts("\nPoll loop complete.")
