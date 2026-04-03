unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Requeue all — move all messages to a different queue channel

source_channel = "qs.requeue-source"
target_channel = "qs.requeue-target"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-requeue")

# Send messages to source
for i <- 1..3 do
  {:ok, _} = KubeMQ.Client.send_queue_message(client,
    KubeMQ.QueueMessage.new(channel: source_channel, body: "Requeue msg #{i}"))
end

IO.puts("Sent 3 messages to '#{source_channel}'")

# Poll from source and requeue to target
case KubeMQ.Client.poll_queue(client,
       channel: source_channel,
       max_items: 10,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("Polled #{length(poll.messages)} messages from source")

    case KubeMQ.PollResponse.requeue_all(poll, target_channel) do
      {:ok, _} -> IO.puts("All messages requeued to '#{target_channel}'")
      {:error, err} -> IO.puts("Requeue failed: #{err.message}")
    end

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

# Verify messages arrived at target
case KubeMQ.Client.receive_queue_messages(client, target_channel,
       max_messages: 10,
       wait_timeout: 3_000
     ) do
  {:ok, result} ->
    IO.puts("Target queue has #{result.messages_received} messages")

  {:error, _} ->
    IO.puts("No messages in target")
end

KubeMQ.Client.close(client)
