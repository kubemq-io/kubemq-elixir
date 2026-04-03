defmodule KubeMQ.ServerInfo do
  @moduledoc """
  Server information returned from a Ping request.
  """

  @type t :: %__MODULE__{
          host: String.t(),
          version: String.t(),
          server_start_time: integer(),
          server_up_time_seconds: integer()
        }

  defstruct [:host, :version, :server_start_time, :server_up_time_seconds]

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
