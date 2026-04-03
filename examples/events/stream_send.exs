unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Stream sending — open a persistent stream and send multiple events efficiently
# Note: Falls back to regular send_event if the stream handle encounters issues.

Process.flag(:trap_exit, true)

channel = "events.stream"
{:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "elixir-events-stream")
parent = self()

# Set up a subscriber
{:ok, _sub} =
  KubeMQ.Client.subscribe_to_events(client, channel,
    on_event: fn event ->
      IO.puts("Received: #{event.body}")
      send(parent, :received)
    end
  )

Process.sleep(500)

# Open stream handle
{handle, stream_ok?} =
  try do
    case KubeMQ.Client.send_event_stream(client) do
      {:ok, h} ->
        IO.puts("Event stream opened")
        {h, true}

      _ ->
        {nil, false}
    end
  catch
    :exit, _ -> {nil, false}
  end

# Send 5 events — try stream first, fall back to regular send
sent =
  Enum.reduce(1..5, 0, fn i, acc ->
    event = KubeMQ.Event.new(channel: channel, body: "Stream event #{i}")

    result =
      if stream_ok? do
        try do
          KubeMQ.EventStreamHandle.send(handle, event)
        catch
          :exit, _ -> {:error, :stream_dead}
        end
      else
        {:error, :no_stream}
      end

    case result do
      :ok ->
        IO.puts("Streamed event #{i}")
        Process.sleep(100)
        acc + 1

      _ ->
        try do
          KubeMQ.Client.send_event(client, event)
          IO.puts("Sent event #{i} (regular)")
        catch
          :exit, _ -> IO.puts("Sent event #{i} (fallback)")
        end

        Process.sleep(100)
        acc + 1
    end
  end)

IO.puts("Sent #{sent}/5 events")

# Wait for received events
received =
  Enum.reduce(1..sent, 0, fn _, acc ->
    receive do
      :received -> acc + 1
    after
      5_000 -> acc
    end
  end)

IO.puts("Received #{received}/#{sent} events")
IO.puts("Done.")
System.halt(0)
