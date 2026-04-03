defmodule KubeMQ.Event do
  @moduledoc """
  Event message for the KubeMQ Pub/Sub pattern.

  Events are fire-and-forget messages delivered to all active subscribers on a channel.
  The `store` field must remain `false` for non-persistent events (use `KubeMQ.EventStore`
  for persistent events).

  ## Usage

      event = KubeMQ.Event.new(channel: "notifications", body: "hello")
      :ok = KubeMQ.Client.send_event(client, event)
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          channel: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          client_id: String.t() | nil,
          tags: %{String.t() => String.t()},
          store: boolean()
        }

  defstruct [:id, :channel, :metadata, :body, :client_id, store: false, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id),
      channel: Keyword.get(opts, :channel),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      client_id: Keyword.get(opts, :client_id),
      tags: Keyword.get(opts, :tags, %{}),
      store: false
    }
  end
end
