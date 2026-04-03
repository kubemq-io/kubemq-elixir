defmodule KubeMQ.Queries.Sender do
  @moduledoc false

  alias KubeMQ.{Error, Query, QueryResponse, UUID, Validation}

  @spec send_query(GRPC.Channel.t(), module(), Query.t(), String.t()) ::
          {:ok, QueryResponse.t()} | {:error, Error.t()}
  def send_query(channel, transport, %Query{} = query, client_id) do
    with :ok <- Validation.validate_channel(query.channel),
         :ok <- Validation.validate_content(query.metadata, query.body),
         :ok <- Validation.validate_timeout(query.timeout),
         :ok <- Validation.validate_cache(query.cache_key, query.cache_ttl) do
      cache_ttl_seconds =
        if query.cache_ttl && query.cache_ttl > 0,
          do: div(query.cache_ttl, 1000),
          else: 0

      request = %{
        id: UUID.ensure(query.id),
        request_type: :query,
        channel: query.channel,
        metadata: query.metadata || "",
        body: query.body || "",
        client_id: query.client_id || client_id,
        timeout: query.timeout,
        tags: query.tags || %{},
        cache_key: query.cache_key || "",
        cache_ttl: cache_ttl_seconds
      }

      case transport.send_request(channel, request) do
        {:ok, response} ->
          {:ok, QueryResponse.from_response(response)}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "send_query",
             channel: query.channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("send_query failed: #{inspect(reason)}",
             operation: "send_query",
             channel: query.channel
           )}
      end
    end
  end

  @spec send_response(GRPC.Channel.t(), module(), KubeMQ.QueryReply.t()) ::
          :ok | {:error, Error.t()}
  def send_response(channel, transport, %KubeMQ.QueryReply{} = reply) do
    with :ok <- Validation.validate_response_fields(reply) do
      response_map = KubeMQ.QueryReply.to_response_map(reply)

      case transport.send_response(channel, response_map) do
        :ok ->
          :ok

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code, operation: "send_query_response", message: message)}

        {:error, reason} ->
          {:error,
           Error.transient("send_query_response failed: #{inspect(reason)}",
             operation: "send_query_response"
           )}
      end
    end
  end
end
