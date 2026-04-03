defmodule KubeMQ.Channels.Manager do
  @moduledoc false

  alias KubeMQ.{ChannelInfo, Error, UUID, Validation}

  @request_channel "kubemq.cluster.internal.requests"
  @default_timeout 10_000
  @snapshot_retries 3
  @snapshot_delay 200

  @type channel_type :: :events | :events_store | :commands | :queries | :queues

  @channel_type_strings %{
    events: "events",
    events_store: "events_store",
    commands: "commands",
    queries: "queries",
    queues: "queues"
  }

  @spec create_channel(GRPC.Channel.t(), module(), String.t(), String.t(), channel_type()) ::
          :ok | {:error, Error.t()}
  def create_channel(channel, transport, client_id, name, type) do
    with :ok <- validate_channel_type(type, "create_channel"),
         :ok <- Validation.validate_channel(name) do
      tags = %{
        "channel_type" => channel_type_string(type),
        "channel" => name,
        "client_id" => client_id
      }

      request = build_internal_request(client_id, "create-channel", tags)

      case execute_with_snapshot_retry(transport, channel, request) do
        {:ok, _response} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  @spec delete_channel(GRPC.Channel.t(), module(), String.t(), String.t(), channel_type()) ::
          :ok | {:error, Error.t()}
  def delete_channel(channel, transport, client_id, name, type) do
    with :ok <- validate_channel_type(type, "delete_channel"),
         :ok <- Validation.validate_channel(name) do
      tags = %{
        "channel_type" => channel_type_string(type),
        "channel" => name
      }

      request = build_internal_request(client_id, "delete-channel", tags)

      case execute_with_snapshot_retry(transport, channel, request) do
        {:ok, _response} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  @spec list_channels(GRPC.Channel.t(), module(), String.t(), channel_type(), String.t()) ::
          {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_channels(channel, transport, client_id, type, search \\ "") do
    with :ok <- validate_channel_type(type, "list_channels") do
      tags = %{"channel_type" => channel_type_string(type)}
      tags = if search != "", do: Map.put(tags, "channel_search", search), else: tags

      request = build_internal_request(client_id, "list-channels", tags)

      case execute_with_snapshot_retry(transport, channel, request) do
        {:ok, response} ->
          body = Map.get(response, :body, "")
          body_str = if is_binary(body), do: body, else: to_string(body)
          ChannelInfo.from_json(body_str)

        {:error, _} = err ->
          err
      end
    end
  end

  @spec purge_queue_channel(GRPC.Channel.t(), module(), String.t(), String.t()) ::
          :ok | {:error, Error.t()}
  def purge_queue_channel(channel, transport, client_id, name) do
    with :ok <- Validation.validate_channel(name) do
      request = %{
        request_id: UUID.generate(),
        client_id: client_id,
        channel: name,
        wait_time_seconds: 5
      }

      case transport.ack_all_queue_messages(channel, request) do
        {:ok, result} ->
          if Map.get(result, :is_error, false) do
            {:error,
             Error.fatal("purge failed: #{Map.get(result, :error, "")}",
               operation: "purge_queue_channel",
               channel: name
             )}
          else
            :ok
          end

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "purge_queue_channel",
             channel: name,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("purge failed: #{inspect(reason)}",
             operation: "purge_queue_channel",
             channel: name
           )}
      end
    end
  end

  # --- Private ---

  defp build_internal_request(client_id, metadata, tags) do
    %{
      id: UUID.generate(),
      request_type: :query,
      client_id: client_id,
      channel: @request_channel,
      metadata: metadata,
      body: "",
      reply_channel: "",
      timeout: @default_timeout,
      cache_key: "",
      cache_ttl: 0,
      tags: tags
    }
  end

  defp execute_with_snapshot_retry(transport, channel, request, attempt \\ 0) do
    case transport.send_request(channel, request) do
      {:ok, response} ->
        error_msg = Map.get(response, :error, "")

        if is_binary(error_msg) and String.contains?(error_msg, "cluster snapshot not ready") and
             attempt < @snapshot_retries do
          Process.sleep(@snapshot_delay)
          execute_with_snapshot_retry(transport, channel, request, attempt + 1)
        else
          if error_msg != nil and error_msg != "" and not Map.get(response, :executed, true) do
            {:error,
             Error.fatal(error_msg,
               operation: Map.get(request, :metadata, "channel_management"),
               channel: Map.get(request, :channel, "")
             )}
          else
            {:ok, response}
          end
        end

      {:error, {code, message}} ->
        {:error,
         Error.from_grpc_status(code,
           operation: Map.get(request, :metadata, "channel_management"),
           message: message
         )}

      {:error, reason} ->
        {:error,
         Error.transient("channel management failed: #{inspect(reason)}",
           operation: Map.get(request, :metadata, "channel_management")
         )}
    end
  end

  defp validate_channel_type(type, operation) do
    if Map.has_key?(@channel_type_strings, type) do
      :ok
    else
      {:error,
       Error.validation(
         "invalid channel type #{inspect(type)}; must be one of: :events, :events_store, :commands, :queries, :queues",
         operation: operation
       )}
    end
  end

  defp channel_type_string(type), do: Map.fetch!(@channel_type_strings, type)
end
