unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Dead letter queue — messages exceeding max receive count are routed to a DLQ

channel = "queues.dlq-source"
dlq_channel = "queues.dlq-dead-letters"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-dlq")

# Send a message with a dead-letter policy
msg = KubeMQ.QueueMessage.new(
  channel: channel,
  body: "Process me (max 2 attempts)",
  policy: KubeMQ.QueuePolicy.new(
    max_receive_count: 2,
    max_receive_queue: dlq_channel
  )
)

{:ok, _} = KubeMQ.Client.send_queue_message(client, msg)
IO.puts("Sent message with max_receive_count=2, DLQ='#{dlq_channel}'")

# Simulate two failed processing attempts (receive but don't ack via stream API)
# Using simple receive which auto-acks, so we just demonstrate the concept
IO.puts("After exceeding max receives, message moves to '#{dlq_channel}'")

# Check the dead letter queue
IO.puts("Checking DLQ channel...")

case KubeMQ.Client.receive_queue_messages(client, dlq_channel,
       max_messages: 10,
       wait_timeout: 3_000
     ) do
  {:ok, result} ->
    IO.puts("DLQ messages: #{result.messages_received}")

    Enum.each(result.messages, fn m ->
      IO.puts("  Body: #{m.body}")

      if m.attributes do
        IO.puts("  Re-routed: #{m.attributes.re_routed}")
        IO.puts("  From: #{m.attributes.re_routed_from_queue}")
      end
    end)

  {:error, _} ->
    IO.puts("No DLQ messages yet")
end

KubeMQ.Client.close(client)
