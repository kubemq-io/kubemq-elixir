unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Dead letter policy via stream API — messages move to DLQ after max receives

channel = "qs.dlq-stream"
dlq = "qs.dlq-stream-dead"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-dlq-stream")

# Send via upstream with dead letter policy
{:ok, handle} = KubeMQ.Client.queue_upstream(client)

msg = KubeMQ.QueueMessage.new(
  channel: channel,
  body: "Fragile message",
  policy: KubeMQ.QueuePolicy.new(
    max_receive_count: 2,
    max_receive_queue: dlq
  )
)

{:ok, _} = KubeMQ.QueueUpstreamHandle.send(handle, [msg])
IO.puts("Sent message with DLQ policy (max 2 receives)")

# Simulate failed processing: poll and nack twice
for attempt <- 1..2 do
  case KubeMQ.Client.poll_queue(client,
         channel: channel,
         max_items: 1,
         wait_timeout: 3_000
       ) do
    {:ok, poll} when length(poll.messages) > 0 ->
      IO.puts("Attempt #{attempt}: received, nacking...")
      :ok = KubeMQ.PollResponse.nack_all(poll)
      Process.sleep(500)

    _ ->
      IO.puts("Attempt #{attempt}: no message")
  end
end

# Check the DLQ
Process.sleep(1_000)

case KubeMQ.Client.receive_queue_messages(client, dlq,
       max_messages: 10,
       wait_timeout: 3_000
     ) do
  {:ok, result} ->
    IO.puts("DLQ has #{result.messages_received} messages")
    Enum.each(result.messages, fn m -> IO.puts("  #{m.body}") end)

  {:error, _} ->
    IO.puts("DLQ empty (message may still be in transit)")
end

KubeMQ.QueueUpstreamHandle.close(handle)
KubeMQ.Client.close(client)
