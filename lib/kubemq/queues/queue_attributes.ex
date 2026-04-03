defmodule KubeMQ.QueueAttributes do
  @moduledoc """
  Server-populated attributes attached to received queue messages.
  """

  @type t :: %__MODULE__{
          timestamp: integer(),
          sequence: non_neg_integer(),
          md5_of_body: String.t(),
          receive_count: non_neg_integer(),
          re_routed: boolean(),
          re_routed_from_queue: String.t(),
          expiration_at: integer(),
          delayed_to: integer()
        }

  defstruct [
    :timestamp,
    :sequence,
    :md5_of_body,
    :receive_count,
    :re_routed,
    :re_routed_from_queue,
    :expiration_at,
    :delayed_to
  ]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      timestamp: Keyword.get(opts, :timestamp, 0),
      sequence: Keyword.get(opts, :sequence, 0),
      md5_of_body: Keyword.get(opts, :md5_of_body, ""),
      receive_count: Keyword.get(opts, :receive_count, 0),
      re_routed: Keyword.get(opts, :re_routed, false),
      re_routed_from_queue: Keyword.get(opts, :re_routed_from_queue, ""),
      expiration_at: Keyword.get(opts, :expiration_at, 0),
      delayed_to: Keyword.get(opts, :delayed_to, 0)
    }
  end
end
