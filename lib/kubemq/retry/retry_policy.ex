defmodule KubeMQ.RetryPolicy do
  @moduledoc """
  Retry policy for transient gRPC errors.

  Applied by the transport layer when operations fail with retryable error codes
  (`:transient`, `:timeout`, `:throttling`).

  ## Fields

    * `max_retries` (`non_neg_integer()`) — Maximum number of retry attempts. Default: `3`.
    * `initial_delay` (`pos_integer()`) — First retry delay in milliseconds. Default: `100`.
    * `max_delay` (`pos_integer()`) — Maximum retry delay in milliseconds. Default: `1_000`.
    * `multiplier` (`float()`) — Exponential backoff multiplier. Default: `2.0`.

  ## Defaults

      %KubeMQ.RetryPolicy{
        max_retries: 3,
        initial_delay: 100,
        max_delay: 1_000,
        multiplier: 2.0
      }
  """

  @type t :: %__MODULE__{
          max_retries: non_neg_integer(),
          initial_delay: pos_integer(),
          max_delay: pos_integer(),
          multiplier: float()
        }

  defstruct max_retries: 3,
            initial_delay: 100,
            max_delay: 1_000,
            multiplier: 2.0

  @doc """
  Create a new RetryPolicy struct from keyword options.

  ## Options

    * `:max_retries` — Maximum retry attempts (default: `3`)
    * `:initial_delay` — First retry delay in ms (default: `100`)
    * `:max_delay` — Maximum retry delay in ms (default: `1_000`)
    * `:multiplier` — Exponential backoff multiplier (default: `2.0`)

  ## Examples

      iex> policy = KubeMQ.RetryPolicy.new(max_retries: 5, initial_delay: 200)
      iex> policy.max_retries
      5
      iex> policy.max_delay
      1000
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      max_retries: Keyword.get(opts, :max_retries, 3),
      initial_delay: Keyword.get(opts, :initial_delay, 100),
      max_delay: Keyword.get(opts, :max_delay, 1_000),
      multiplier: Keyword.get(opts, :multiplier, 2.0)
    }
  end

  @doc """
  Build from the `:retry_policy` keyword list in client configuration.

  ## Examples

      iex> policy = KubeMQ.RetryPolicy.from_config(max_retries: 5, initial_delay: 200)
      iex> policy.max_retries
      5
      iex> policy.initial_delay
      200
  """
  @spec from_config(keyword()) :: t()
  def from_config(config) when is_list(config) do
    new(config)
  end

  @doc """
  Compute the delay for the given attempt number (0-based).
  """
  @spec delay_for_attempt(t(), non_neg_integer()) :: non_neg_integer()
  def delay_for_attempt(%__MODULE__{} = policy, attempt) do
    base = trunc(policy.initial_delay * :math.pow(policy.multiplier, attempt))
    min(base, policy.max_delay)
  end

  @doc """
  Returns `true` if the attempt number is within the retry limit.
  """
  @spec should_retry?(t(), non_neg_integer()) :: boolean()
  def should_retry?(%__MODULE__{max_retries: max}, attempt) do
    attempt < max
  end
end
