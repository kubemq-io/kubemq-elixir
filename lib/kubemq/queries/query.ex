defmodule KubeMQ.Query do
  @moduledoc """
  Query message for the KubeMQ RPC pattern with optional response caching.

  Queries are request-response messages where the sender expects data back.
  Supports server-side caching via `cache_key` and `cache_ttl`.

  ## Fields

    * `id` (`String.t() | nil`) — Unique query identifier. Auto-generated if nil.
    * `channel` (`String.t()`) — Target channel name. Required for sending.
    * `metadata` (`String.t() | nil`) — Optional metadata string.
    * `body` (`binary() | nil`) — Query payload.
    * `timeout` (`pos_integer()`) — Timeout in milliseconds. Default: `10_000`.
    * `client_id` (`String.t() | nil`) — Sender identifier. Set automatically by `KubeMQ.Client`.
    * `cache_key` (`String.t() | nil`) — Server-side cache key. Set to enable response caching.
    * `cache_ttl` (`pos_integer() | nil`) — Cache time-to-live in milliseconds.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags. Default: `%{}`.
    * `span` (`binary() | nil`) — Tracing span context (internal).

  ## Usage

      query = KubeMQ.Query.new(
        channel: "products.lookup",
        body: "sku-123",
        timeout: 10_000,
        cache_key: "product:sku-123",
        cache_ttl: 60_000
      )
      {:ok, response} = KubeMQ.Client.send_query(client, query)
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          channel: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          timeout: pos_integer(),
          client_id: String.t() | nil,
          cache_key: String.t() | nil,
          cache_ttl: pos_integer() | nil,
          tags: %{String.t() => String.t()},
          span: binary() | nil
        }

  defstruct [
    :id,
    :channel,
    :metadata,
    :body,
    :timeout,
    :client_id,
    :cache_key,
    :cache_ttl,
    :span,
    tags: %{}
  ]

  @doc """
  Create a new Query struct from keyword options.

  ## Options

    * `:id` — Query ID string (auto-generated if nil)
    * `:channel` — Target channel name (required for sending)
    * `:metadata` — Metadata string
    * `:body` — Query payload (binary)
    * `:timeout` — Timeout in milliseconds (default: `10_000`)
    * `:client_id` — Client identifier (set automatically by `KubeMQ.Client`)
    * `:cache_key` — Server-side cache key
    * `:cache_ttl` — Cache time-to-live in milliseconds

  ## Examples

      iex> query = KubeMQ.Query.new(channel: "products.lookup", cache_key: "p:123", cache_ttl: 60_000)
      iex> query.channel
      "products.lookup"
      iex> query.cache_key
      "p:123"
      iex> query.timeout
      10000
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id),
      channel: Keyword.get(opts, :channel),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      timeout: Keyword.get(opts, :timeout, 10_000),
      client_id: Keyword.get(opts, :client_id),
      cache_key: Keyword.get(opts, :cache_key),
      cache_ttl: Keyword.get(opts, :cache_ttl),
      tags: Keyword.get(opts, :tags, %{}),
      span: Keyword.get(opts, :span)
    }
  end
end
