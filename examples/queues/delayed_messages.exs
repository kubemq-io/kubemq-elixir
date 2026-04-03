unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Delayed messages — defer message delivery by a specified duration

channel = "queues.delayed"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-queue-delayed")

# Send a message with a 5-second delay
msg = KubeMQ.QueueMessage.new(
  channel: channel,
  body: "Delayed delivery!",
  policy: KubeMQ.QueuePolicy.new(delay_seconds: 5)
)

{:ok, result} = KubeMQ.Client.send_queue_message(client, msg)
IO.puts("Message sent with 5s delay. Delayed to: #{result.delayed_to}")

# Try to receive immediately — should get nothing
IO.puts("Trying to receive immediately...")

case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 1,
       wait_timeout: 2_000
     ) do
  {:ok, result} ->
    IO.puts("Immediate receive: #{result.messages_received} messages (expected 0)")

  {:error, _} ->
    IO.puts("No messages yet (as expected)")
end

# Wait for the delay to expire
IO.puts("Waiting for delay to expire...")
Process.sleep(6_000)

case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 1,
       wait_timeout: 5_000
     ) do
  {:ok, result} ->
    IO.puts("After delay: #{result.messages_received} message(s)")
    Enum.each(result.messages, fn m -> IO.puts("  Body: #{m.body}") end)

  {:error, err} ->
    IO.puts("Receive failed: #{err.message}")
end

KubeMQ.Client.close(client)
