defmodule KubeMQ.EventStore do
  @moduledoc """
  Persistent event message for the KubeMQ Events Store pattern.

  Unlike regular events, store events are persisted and can be replayed from
  various start positions. The `store` field is always `true` internally.

  ## Usage

      event = KubeMQ.EventStore.new(channel: "audit-log", body: "user login")
      {:ok, result} = KubeMQ.Client.send_event_store(client, event)
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          channel: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          client_id: String.t() | nil,
          tags: %{String.t() => String.t()}
        }

  defstruct [:id, :channel, :metadata, :body, :client_id, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id),
      channel: Keyword.get(opts, :channel),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      client_id: Keyword.get(opts, :client_id),
      tags: Keyword.get(opts, :tags, %{})
    }
  end
end
