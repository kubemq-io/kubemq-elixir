defmodule KubeMQ.QueuePolicy do
  @moduledoc """
  Delivery policy for queue messages.

  - `expiration_seconds` — message expires after N seconds (0 = no expiry)
  - `delay_seconds` — delay delivery by N seconds (0 = immediate)
  - `max_receive_count` — max delivery attempts before dead-letter (0 = unlimited)
  - `max_receive_queue` — dead-letter queue name (required when `max_receive_count > 0`)
  """

  @type t :: %__MODULE__{
          expiration_seconds: non_neg_integer(),
          delay_seconds: non_neg_integer(),
          max_receive_count: non_neg_integer(),
          max_receive_queue: String.t()
        }

  defstruct expiration_seconds: 0,
            delay_seconds: 0,
            max_receive_count: 0,
            max_receive_queue: ""

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      expiration_seconds: Keyword.get(opts, :expiration_seconds, 0),
      delay_seconds: Keyword.get(opts, :delay_seconds, 0),
      max_receive_count: Keyword.get(opts, :max_receive_count, 0),
      max_receive_queue: Keyword.get(opts, :max_receive_queue, "")
    }
  end
end
