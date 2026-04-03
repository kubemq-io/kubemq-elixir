defmodule KubeMQ.ReconnectPolicy do
  @moduledoc """
  Reconnection policy for the KubeMQ connection GenServer.

  Controls automatic reconnection behaviour when the gRPC connection is lost.
  A `max_attempts` of `0` means unlimited reconnection attempts.

  ## Defaults

      %KubeMQ.ReconnectPolicy{
        enabled: true,
        initial_delay: 1_000,
        max_delay: 30_000,
        max_attempts: 0,
        multiplier: 2.0
      }
  """

  @type t :: %__MODULE__{
          enabled: boolean(),
          initial_delay: pos_integer(),
          max_delay: pos_integer(),
          max_attempts: non_neg_integer(),
          multiplier: float()
        }

  defstruct enabled: true,
            initial_delay: 1_000,
            max_delay: 30_000,
            max_attempts: 0,
            multiplier: 2.0

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      enabled: Keyword.get(opts, :enabled, true),
      initial_delay: Keyword.get(opts, :initial_delay, 1_000),
      max_delay: Keyword.get(opts, :max_delay, 30_000),
      max_attempts: Keyword.get(opts, :max_attempts, 0),
      multiplier: Keyword.get(opts, :multiplier, 2.0)
    }
  end

  @doc """
  Build from the `:reconnect_policy` keyword list in client configuration.
  """
  @spec from_config(keyword()) :: t()
  def from_config(config) when is_list(config) do
    new(config)
  end
end
