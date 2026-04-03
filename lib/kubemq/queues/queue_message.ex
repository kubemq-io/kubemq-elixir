defmodule KubeMQ.QueueMessage do
  @moduledoc """
  Message for the KubeMQ Queues pattern.

  Supports delivery policies (expiration, delay, dead-letter) and
  server-populated attributes on receive.

  ## Fields

    * `id` (`String.t() | nil`) — Unique message identifier. Auto-generated if nil.
    * `channel` (`String.t()`) — Target queue channel name. Required for sending.
    * `metadata` (`String.t() | nil`) — Optional metadata string.
    * `body` (`binary() | nil`) — Message payload.
    * `client_id` (`String.t() | nil`) — Sender identifier. Set automatically by `KubeMQ.Client`.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags. Default: `%{}`.
    * `policy` (`KubeMQ.QueuePolicy.t() | nil`) — Delivery policy (expiration, delay, dead-letter).
    * `attributes` (`KubeMQ.QueueAttributes.t() | nil`) — Server-populated attributes (present only on received messages).

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

  @doc """
  Create a new QueueMessage struct from keyword options.

  ## Options

    * `:id` — Message ID string (auto-generated if nil)
    * `:channel` — Target queue channel name (required for sending)
    * `:metadata` — Metadata string
    * `:body` — Message payload (binary)
    * `:client_id` — Client identifier (set automatically by `KubeMQ.Client`)
    * `:tags` — Key-value tags map (default: `%{}`)
    * `:policy` — `KubeMQ.QueuePolicy` struct for delivery policy
    * `:attributes` — `KubeMQ.QueueAttributes` struct (server-populated on receive)

  ## Examples

      iex> msg = KubeMQ.QueueMessage.new(channel: "orders", body: "order-data")
      iex> msg.channel
      "orders"
      iex> msg.tags
      %{}
  """
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
