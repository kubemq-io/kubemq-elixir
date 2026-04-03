defmodule KubeMQ.QueryResponse do
  @moduledoc """
  Response received after sending a query via `KubeMQ.Client.send_query/2`.

  ## Fields

    * `query_id` (`String.t()`) — ID of the query that was executed.
    * `executed` (`boolean()`) — Whether the query was executed successfully.
    * `executed_at` (`integer()`) — Unix timestamp when the query was executed.
    * `metadata` (`String.t() | nil`) — Response metadata from the responder.
    * `body` (`binary() | nil`) — Response payload from the responder.
    * `cache_hit` (`boolean()`) — Whether the response was served from server-side cache.
    * `error` (`String.t() | nil`) — Error message if execution failed.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags from the responder.
  """

  @type t :: %__MODULE__{
          query_id: String.t(),
          executed: boolean(),
          executed_at: integer(),
          metadata: String.t() | nil,
          body: binary() | nil,
          cache_hit: boolean(),
          error: String.t() | nil,
          tags: %{String.t() => String.t()}
        }

  defstruct [:query_id, :executed, :executed_at, :metadata, :body, :cache_hit, :error, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      query_id: Keyword.get(opts, :query_id, ""),
      executed: Keyword.get(opts, :executed, false),
      executed_at: Keyword.get(opts, :executed_at, 0),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      cache_hit: Keyword.get(opts, :cache_hit, false),
      error: Keyword.get(opts, :error),
      tags: Keyword.get(opts, :tags, %{})
    }
  end

  @spec from_response(map()) :: t()
  def from_response(response) do
    %__MODULE__{
      query_id: response.request_id || "",
      executed: response.executed || false,
      executed_at: response.timestamp || 0,
      metadata: response.metadata,
      body: response.body,
      cache_hit: response.cache_hit || false,
      error: if(response.error in [nil, ""], do: nil, else: response.error),
      tags: response.tags || %{}
    }
  end
end
