defmodule KubeMQ.QueueReceiveResult do
  @moduledoc """
  Result of receiving messages from a queue via the Simple API.
  """

  @type t :: %__MODULE__{
          request_id: String.t(),
          messages: [KubeMQ.QueueMessage.t()],
          messages_received: non_neg_integer(),
          messages_expired: non_neg_integer(),
          is_peek: boolean(),
          is_error: boolean(),
          error: String.t() | nil
        }

  defstruct [
    :request_id,
    :messages,
    :messages_received,
    :messages_expired,
    :is_peek,
    :is_error,
    :error
  ]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      request_id: Keyword.get(opts, :request_id, ""),
      messages: Keyword.get(opts, :messages, []),
      messages_received: Keyword.get(opts, :messages_received, 0),
      messages_expired: Keyword.get(opts, :messages_expired, 0),
      is_peek: Keyword.get(opts, :is_peek, false),
      is_error: Keyword.get(opts, :is_error, false),
      error: Keyword.get(opts, :error)
    }
  end

  @spec from_transport(map()) :: t()
  def from_transport(result) do
    messages = Enum.map(result.messages || [], &KubeMQ.QueueMessage.from_transport/1)

    %__MODULE__{
      request_id: result.request_id || "",
      messages: messages,
      messages_received: result.messages_received || 0,
      messages_expired: result.messages_expired || 0,
      is_peek: result.is_peek || false,
      is_error: result.is_error || false,
      error: if(result.error in [nil, ""], do: nil, else: result.error)
    }
  end
end
