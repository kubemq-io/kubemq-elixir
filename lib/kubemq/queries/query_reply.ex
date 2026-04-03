defmodule KubeMQ.QueryReply do
  @moduledoc """
  Reply constructed by a query subscriber to respond to a received query.

  ## Fields

    * `request_id` (`String.t()`) — ID of the original query (from `KubeMQ.QueryReceive.id`).
    * `response_to` (`String.t()`) — Reply channel (from `KubeMQ.QueryReceive.reply_channel`).
    * `metadata` (`String.t() | nil`) — Optional response metadata.
    * `body` (`binary() | nil`) — Response payload.
    * `client_id` (`String.t() | nil`) — Responder identifier. Set automatically.
    * `executed` (`boolean()`) — Whether the query was executed successfully. Default: `false`.
    * `error` (`String.t() | nil`) — Error message if execution failed.
    * `cache_hit` (`boolean()`) — Whether this reply is from cache. Default: `false`.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags. Default: `%{}`.
    * `span` (`binary() | nil`) — Tracing span context (internal).

  ## Usage

      reply = KubeMQ.QueryReply.new(
        request_id: query_receive.id,
        response_to: query_receive.reply_channel,
        executed: true,
        body: result_data
      )
  """

  @type t :: %__MODULE__{
          request_id: String.t(),
          response_to: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          client_id: String.t() | nil,
          executed: boolean(),
          error: String.t() | nil,
          cache_hit: boolean(),
          tags: %{String.t() => String.t()},
          span: binary() | nil
        }

  defstruct [
    :request_id,
    :response_to,
    :metadata,
    :body,
    :client_id,
    :error,
    :span,
    executed: false,
    cache_hit: false,
    tags: %{}
  ]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      request_id: Keyword.get(opts, :request_id),
      response_to: Keyword.get(opts, :response_to),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      client_id: Keyword.get(opts, :client_id),
      executed: Keyword.get(opts, :executed, false),
      error: Keyword.get(opts, :error),
      cache_hit: Keyword.get(opts, :cache_hit, false),
      tags: Keyword.get(opts, :tags, %{}),
      span: Keyword.get(opts, :span)
    }
  end

  @spec to_response_map(t()) :: map()
  def to_response_map(%__MODULE__{} = reply) do
    %{
      request_id: reply.request_id,
      reply_channel: reply.response_to,
      client_id: reply.client_id || "",
      metadata: reply.metadata || "",
      body: reply.body || "",
      cache_hit: reply.cache_hit,
      timestamp: System.system_time(:second),
      executed: reply.executed,
      error: reply.error || "",
      tags: reply.tags || %{}
    }
  end
end
