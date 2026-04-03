unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Delay policy — defer message delivery via stream API

channel = "qs.delay-policy"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-delay")

# Open upstream and send a delayed message
{:ok, handle} = KubeMQ.Client.queue_upstream(client)

msg = KubeMQ.QueueMessage.new(
  channel: channel,
  body: "Delayed via stream",
  policy: KubeMQ.QueuePolicy.new(delay_seconds: 3)
)

case KubeMQ.QueueUpstreamHandle.send(handle, [msg]) do
  {:ok, results} ->
    r = hd(results)
    IO.puts("Sent with delay. Delayed to: #{r.delayed_to}")

  {:error, err} ->
    IO.puts("Send failed: #{err.message}")
end

# Poll immediately — nothing yet
IO.puts("Polling immediately...")

case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 1,
       wait_timeout: 2_000
     ) do
  {:ok, poll} ->
    IO.puts("Immediate poll: #{length(poll.messages)} messages")

  {:error, _} ->
    IO.puts("No messages yet")
end

# Wait for delay
IO.puts("Waiting 4 seconds...")
Process.sleep(4_000)

case KubeMQ.Client.poll_queue(client,
       channel: channel,
       max_items: 1,
       wait_timeout: 5_000
     ) do
  {:ok, poll} ->
    IO.puts("After delay: #{length(poll.messages)} message(s)")
    Enum.each(poll.messages, fn m -> IO.puts("  #{m.body}") end)
    :ok = KubeMQ.PollResponse.ack_all(poll)

  {:error, err} ->
    IO.puts("Poll failed: #{err.message}")
end

KubeMQ.QueueUpstreamHandle.close(handle)
KubeMQ.Client.close(client)
