unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Batch send — send multiple queue messages in a single operation

channel = "queues.batch"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-batch")

# Build a batch of messages
messages =
  for i <- 1..5 do
    KubeMQ.QueueMessage.new(
      channel: channel,
      body: "Batch message #{i}",
      tags: %{"index" => "#{i}"}
    )
  end

IO.puts("Sending batch of #{length(messages)} messages...")

case KubeMQ.Client.send_queue_messages(client, messages) do
  {:ok, result} ->
    IO.puts("Batch ID: #{result.batch_id}")
    IO.puts("Has errors: #{result.have_errors}")

    Enum.each(result.results, fn r ->
      IO.puts("  Message #{r.message_id}: error=#{r.is_error}")
    end)

  {:error, err} ->
    IO.puts("Batch send failed: #{err.message}")
end

# Receive all messages
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 10,
       wait_timeout: 5_000
     ) do
  {:ok, result} ->
    IO.puts("Received #{result.messages_received} messages from batch")

  {:error, err} ->
    IO.puts("Receive failed: #{err.message}")
end

KubeMQ.Client.close(client)
