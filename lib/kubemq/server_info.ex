defmodule KubeMQ.ServerInfo do
  @moduledoc """
  Server information returned from a Ping request.

  Obtained via `KubeMQ.Client.ping/1`. Contains the broker's host, version,
  and uptime information.

  ## Fields

    * `host` (`String.t()`) — Hostname of the KubeMQ server node.
    * `version` (`String.t()`) — Server version string (e.g., `"2.5.0"`).
    * `server_start_time` (`integer()`) — Unix timestamp when the server started.
    * `server_up_time_seconds` (`integer()`) — Server uptime in seconds.

  ## Usage

      {:ok, info} = KubeMQ.Client.ping(client)
      IO.puts("Connected to \#{info.host} v\#{info.version}")
  """

  @type t :: %__MODULE__{
          host: String.t(),
          version: String.t(),
          server_start_time: integer(),
          server_up_time_seconds: integer()
        }

  defstruct [:host, :version, :server_start_time, :server_up_time_seconds]

  @doc """
  Build a `ServerInfo` struct from a raw ping result map.

  ## Examples

      iex> result = %{host: "node-1", version: "2.5.0", server_start_time: 1000, server_up_time_seconds: 3600}
      iex> info = KubeMQ.ServerInfo.from_ping_result(result)
      iex> info.host
      "node-1"
      iex> info.version
      "2.5.0"
  """
  @spec from_ping_result(map()) :: t()
  def from_ping_result(%{host: host, version: version} = result) do
    %__MODULE__{
      host: host,
      version: version,
      server_start_time: Map.get(result, :server_start_time, 0),
      server_up_time_seconds: Map.get(result, :server_up_time_seconds, 0)
    }
  end
end
