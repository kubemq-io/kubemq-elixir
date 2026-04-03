defmodule KubeMQ.RetryPolicy do
  @moduledoc """
  Retry policy for transient gRPC errors.

  Applied by the transport layer when operations fail with retryable error codes
  (`:transient`, `:timeout`, `:throttling`).

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
