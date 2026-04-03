defmodule KubeMQ.QueueSendResult do
  @moduledoc """
  Result of sending a queue message.

  Returned from `KubeMQ.Client.send_queue_message/2`.

  ## Fields

    * `message_id` (`String.t()`) — ID of the sent message.
    * `sent_at` (`integer()`) — Unix timestamp when the message was accepted by the server.
    * `expiration_at` (`integer()`) — Unix timestamp when the message will expire (0 if no expiration).
    * `delayed_to` (`integer()`) — Unix timestamp when the message will become available (0 if no delay).
    * `is_error` (`boolean()`) — Whether sending failed.
    * `error` (`String.t() | nil`) — Error message if `is_error` is true.
  """

  @type t :: %__MODULE__{
          message_id: String.t(),
          sent_at: integer(),
          expiration_at: integer(),
          delayed_to: integer(),
          is_error: boolean(),
          error: String.t() | nil
        }

  defstruct [:message_id, :sent_at, :expiration_at, :delayed_to, :is_error, :error]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      message_id: Keyword.get(opts, :message_id, ""),
      sent_at: Keyword.get(opts, :sent_at, 0),
      expiration_at: Keyword.get(opts, :expiration_at, 0),
      delayed_to: Keyword.get(opts, :delayed_to, 0),
      is_error: Keyword.get(opts, :is_error, false),
      error: Keyword.get(opts, :error)
    }
  end

  @spec from_transport(map()) :: t()
  def from_transport(result) do
    %__MODULE__{
      message_id: result.message_id || "",
      sent_at: result.sent_at || 0,
      expiration_at: result.expiration_at || 0,
      delayed_to: result.delayed_to || 0,
      is_error: result.is_error || false,
      error: if(result.error in [nil, ""], do: nil, else: result.error)
    }
  end
end
