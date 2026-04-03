defmodule KubeMQ.EventStoreReceive do
  @moduledoc """
  Received event from a KubeMQ Events Store subscription.

  Delivered to the `:on_event` callback or `:notify` PID registered via
  `KubeMQ.Client.subscribe_to_events_store/3`.

  ## Fields

    * `id` (`String.t()`) — Unique event identifier assigned by the server.
    * `channel` (`String.t()`) — Channel the event was published to.
    * `metadata` (`String.t()`) — Metadata string set by the publisher.
    * `body` (`binary()`) — Message payload.
    * `timestamp` (`integer()`) — Unix timestamp (nanoseconds) when the event was received by the server.
    * `sequence` (`non_neg_integer()`) — Monotonic sequence number within the channel.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags set by the publisher.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          channel: String.t(),
          metadata: String.t(),
          body: binary(),
          timestamp: integer(),
          sequence: non_neg_integer(),
          tags: %{String.t() => String.t()}
        }

  defstruct [:id, :channel, :metadata, :body, :timestamp, :sequence, tags: %{}]

  @spec from_proto(map()) :: t()
  def from_proto(event) do
    %__MODULE__{
      id: event.event_id,
      channel: event.channel,
      metadata: event.metadata,
      body: event.body,
      timestamp: event.timestamp || 0,
      sequence: event.sequence || 0,
      tags: event.tags || %{}
    }
  end
end
