defmodule KubeMQ.Query do
  @moduledoc """
  Query message for the KubeMQ RPC pattern with optional response caching.

  Queries are request-response messages where the sender expects data back.
  Supports server-side caching via `cache_key` and `cache_ttl`.

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
