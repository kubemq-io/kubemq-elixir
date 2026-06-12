defmodule Burnin.ChannelSetup do
  @moduledoc """
  Concurrent channel creation for burn-in workers.

  `KubeMQ.Client.create_channel/3` is a synchronous `GenServer.call` on a
  single client pid, so creating ~56 channels through one client serializes
  on that client's mailbox. To actually parallelize, channels are spread
  round-robin across the worker's sender pool and created with bounded
  concurrency via `Task.async_stream`. Failures are swallowed per-channel
  (matching the previous best-effort behaviour) so one bad channel can't
  abort setup.
  """

  require Logger

  # Bounded concurrency — enough to hide per-RPC latency without flooding the
  # broker. Effective parallelism is also capped by the number of clients.
  @max_concurrency 10

  # Generous per-channel ceiling; channel creation is a lightweight control
  # RPC. Kept above the SDK's default call timeout so a slow create surfaces
  # as a caught error rather than a stream timeout kill.
  @timeout_ms 30_000

  @doc """
  Create every channel in `channels` of `type`, distributing the work across
  the given `clients` (a list of `KubeMQ.Client` pids) concurrently.
  """
  @spec create_all([pid()], [String.t()], atom()) :: :ok
  def create_all([], _channels, _type), do: :ok
  def create_all(_clients, [], _type), do: :ok

  def create_all(clients, channels, type) do
    client_tuple = List.to_tuple(clients)
    client_count = tuple_size(client_tuple)
    concurrency = min(@max_concurrency, client_count)

    channels
    |> Enum.with_index()
    |> Task.async_stream(
      fn {channel, idx} ->
        client = elem(client_tuple, rem(idx, client_count))

        try do
          KubeMQ.Client.create_channel(client, channel, type)
        catch
          kind, reason ->
            Logger.debug("create_channel #{type} #{channel} failed: #{kind}: #{inspect(reason)}")
            :error
        end
      end,
      max_concurrency: concurrency,
      timeout: @timeout_ms,
      on_timeout: :kill_task,
      ordered: false
    )
    |> Stream.run()

    :ok
  end
end
