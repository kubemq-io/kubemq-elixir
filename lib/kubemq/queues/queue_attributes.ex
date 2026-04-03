defmodule KubeMQ.QueueAttributes do
  @moduledoc """
  Server-populated attributes attached to received queue messages.

  These attributes are read-only and populated by the KubeMQ server when
  messages are received. They are not set by the sender.

  ## Fields

    * `timestamp` (`integer()`) — Unix timestamp when the message was enqueued.
    * `sequence` (`non_neg_integer()`) — Monotonic sequence number within the queue.
    * `md5_of_body` (`String.t()`) — MD5 hash of the message body.
    * `receive_count` (`non_neg_integer()`) — Number of times this message has been delivered.
    * `re_routed` (`boolean()`) — Whether this message was re-routed from a dead-letter queue.
    * `re_routed_from_queue` (`String.t()`) — Original queue name if `re_routed` is true.
    * `expiration_at` (`integer()`) — Unix timestamp when the message will expire.
    * `delayed_to` (`integer()`) — Unix timestamp when the message became available.
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
