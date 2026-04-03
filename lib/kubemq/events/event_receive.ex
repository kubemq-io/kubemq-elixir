defmodule KubeMQ.EventReceive do
  @moduledoc """
  Received event from a KubeMQ Pub/Sub subscription.
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
