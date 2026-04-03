unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Expiration policy — messages expire after a specified duration

channel = "qs.expiration"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-expire")

# Send a message that expires in 5 seconds
msg = KubeMQ.QueueMessage.new(
  channel: channel,
  body: "Expires in 5 seconds",
  policy: KubeMQ.QueuePolicy.new(expiration_seconds: 5)
)

{:ok, result} = KubeMQ.Client.send_queue_message(client, msg)
IO.puts("Message sent with 5s expiration. Expires at: #{result.expiration_at}")

# Receive immediately — should work
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 1,
       wait_timeout: 2_000,
       is_peek: true
     ) do
  {:ok, result} ->
    IO.puts("Before expiry: #{result.messages_received} message(s) available")

  {:error, _} ->
    IO.puts("No messages")
end

# Wait for expiration
IO.puts("Waiting 6 seconds for expiration...")
Process.sleep(6_000)

# Try again — message should be expired
case KubeMQ.Client.receive_queue_messages(client, channel,
       max_messages: 1,
       wait_timeout: 2_000
     ) do
  {:ok, result} ->
    IO.puts("After expiry: #{result.messages_received} messages (expected 0)")
    IO.puts("Messages expired: #{result.messages_expired}")

  {:error, _} ->
    IO.puts("No messages (expired as expected)")
end

KubeMQ.Client.close(client)
