defmodule KubeMQ.QueueMessage do
  @moduledoc """
  Message for the KubeMQ Queues pattern.

  Supports delivery policies (expiration, delay, dead-letter) and
  server-populated attributes on receive.

  ## Usage

      msg = KubeMQ.QueueMessage.new(
        channel: "orders",
        body: payload,
        policy: KubeMQ.QueuePolicy.new(delay_seconds: 30)
      )
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          channel: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          client_id: String.t() | nil,
          tags: %{String.t() => String.t()},
          policy: KubeMQ.QueuePolicy.t() | nil,
          attributes: KubeMQ.QueueAttributes.t() | nil
        }

  defstruct [:id, :channel, :metadata, :body, :client_id, :policy, :attributes, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id),
      channel: Keyword.get(opts, :channel),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      client_id: Keyword.get(opts, :client_id),
      tags: Keyword.get(opts, :tags, %{}),
      policy: Keyword.get(opts, :policy),
      attributes: Keyword.get(opts, :attributes)
    }
  end

  @spec from_transport(map()) :: t()
  def from_transport(msg) do
    %__MODULE__{
      id: msg.id,
      channel: msg.channel,
      metadata: msg.metadata,
      body: msg.body,
      client_id: msg.client_id,
      tags: msg.tags || %{},
      policy: build_policy(msg.policy),
      attributes: build_attributes(msg.attributes)
    }
  end

  defp build_policy(nil), do: nil

  defp build_policy(p) do
    %KubeMQ.QueuePolicy{
      expiration_seconds: p.expiration_seconds || 0,
      delay_seconds: p.delay_seconds || 0,
      max_receive_count: p.max_receive_count || 0,
      max_receive_queue: p.max_receive_queue || ""
    }
  end

  defp build_attributes(nil), do: nil

  defp build_attributes(a) do
    %KubeMQ.QueueAttributes{
      timestamp: a.timestamp,
      sequence: a.sequence,
      md5_of_body: a.md5_of_body,
      receive_count: a.receive_count,
      re_routed: a.re_routed,
      re_routed_from_queue: a.re_routed_from_queue,
      expiration_at: a.expiration_at,
      delayed_to: a.delayed_to
    }
  end
end
