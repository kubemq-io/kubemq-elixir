unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Stream send — send queue messages via upstream bidirectional stream

channel = "qs.stream-send"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-qs-send")

# Open an upstream handle for efficient sending
{:ok, handle} = KubeMQ.Client.queue_upstream(client)
IO.puts("Upstream stream opened")

# Send a batch of messages through the stream
messages =
  for i <- 1..5 do
    KubeMQ.QueueMessage.new(channel: channel, body: "Stream msg #{i}")
  end

case KubeMQ.QueueUpstreamHandle.send(handle, messages) do
  {:ok, results} ->
    IO.puts("Sent #{length(results)} messages via stream")

    Enum.each(results, fn r ->
      IO.puts("  ID: #{r.message_id}, error: #{r.is_error}")
    end)

  {:error, err} ->
    IO.puts("Stream send failed: #{err.message}")
end

KubeMQ.QueueUpstreamHandle.close(handle)
KubeMQ.Client.close(client)
IO.puts("Done.")
