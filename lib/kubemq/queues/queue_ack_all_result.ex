defmodule KubeMQ.QueueAckAllResult do
  @moduledoc """
  Result of acknowledging all messages in a queue.

  ## Fields

    * `request_id` (`String.t()`) — Request identifier.
    * `affected_messages` (`non_neg_integer()`) — Number of messages that were acknowledged.
    * `is_error` (`boolean()`) — Whether the operation failed.
    * `error` (`String.t() | nil`) — Error message if `is_error` is true.
  """

  @type t :: %__MODULE__{
          request_id: String.t(),
          affected_messages: non_neg_integer(),
          is_error: boolean(),
          error: String.t() | nil
        }

  defstruct [:request_id, :affected_messages, :is_error, :error]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      request_id: Keyword.get(opts, :request_id, ""),
      affected_messages: Keyword.get(opts, :affected_messages, 0),
      is_error: Keyword.get(opts, :is_error, false),
      error: Keyword.get(opts, :error)
    }
  end

  @spec from_transport(map()) :: t()
  def from_transport(result) do
    %__MODULE__{
      request_id: result.request_id || "",
      affected_messages: result.affected_messages || 0,
      is_error: result.is_error || false,
      error: if(result.error in [nil, ""], do: nil, else: result.error)
    }
  end
end
