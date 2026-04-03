unless Mix.Project.get(), do: Mix.install([{:kubemq, path: ".."}])

# Reconnection handling — demonstrate connection state monitoring

{:ok, client} =
  KubeMQ.Client.start_link(
    address: "localhost:50000",
    client_id: "elixir-reconnect-example",
    reconnect_policy: [initial_delay: 2_000, max_attempts: 5]
  )

IO.puts("Connected. Monitoring connection state...")

# Check connection state
state = KubeMQ.Client.connection_state(client)
IO.puts("Current state: #{state}")

# Demonstrate resilient operations with retry
defmodule RetryHelper do
  def with_retry(fun, retries \\ 3, delay \\ 1_000) do
    case fun.() do
      {:error, %{code: code}} when code in [:transient, :timeout] and retries > 0 ->
        IO.puts("  Transient error, retrying in #{delay}ms... (#{retries} left)")
        Process.sleep(delay)
        with_retry(fun, retries - 1, delay * 2)

      result ->
        result
    end
  end
end

# Use resilient send with automatic retry
result =
  RetryHelper.with_retry(fn ->
    KubeMQ.Client.send_event(client,
      KubeMQ.Event.new(channel: "error.reconnect", body: "resilient message"))
  end)

case result do
  :ok -> IO.puts("Event sent successfully (possibly after reconnect)")
  {:error, err} -> IO.puts("Failed after retries: #{err.message}")
end

IO.puts("Final connection state: #{KubeMQ.Client.connection_state(client)}")
KubeMQ.Client.close(client)
