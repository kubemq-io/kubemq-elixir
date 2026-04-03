defmodule KubeMQ.ReconnectPolicy do
  @moduledoc """
  Reconnection policy for the KubeMQ connection GenServer.

  Controls automatic reconnection behaviour when the gRPC connection is lost.
  A `max_attempts` of `0` means unlimited reconnection attempts.

  ## Fields

    * `enabled` (`boolean()`) — Whether auto-reconnect is enabled. Default: `true`.
    * `initial_delay` (`pos_integer()`) — First reconnect delay in milliseconds. Default: `1_000`.
    * `max_delay` (`pos_integer()`) — Maximum reconnect delay in milliseconds. Default: `30_000`.
    * `max_attempts` (`non_neg_integer()`) — Maximum reconnection attempts, 0 = unlimited. Default: `0`.
    * `multiplier` (`float()`) — Exponential backoff multiplier. Default: `2.0`.

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

  @doc """
  Create a new ReconnectPolicy struct from keyword options.

  ## Options

    * `:enabled` — Enable/disable auto-reconnect (default: `true`)
    * `:initial_delay` — First reconnect delay in ms (default: `1_000`)
    * `:max_delay` — Maximum reconnect delay in ms (default: `30_000`)
    * `:max_attempts` — Maximum reconnection attempts, 0 = unlimited (default: `0`)
    * `:multiplier` — Exponential backoff multiplier (default: `2.0`)

  ## Examples

      iex> policy = KubeMQ.ReconnectPolicy.new(max_attempts: 10, initial_delay: 2_000)
      iex> policy.max_attempts
      10
      iex> policy.enabled
      true
  """
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

  ## Examples

      iex> policy = KubeMQ.ReconnectPolicy.from_config(enabled: false, max_attempts: 10)
      iex> policy.enabled
      false
      iex> policy.max_attempts
      10
  """
  @spec from_config(keyword()) :: t()
  def from_config(config) when is_list(config) do
    new(config)
  end
end
