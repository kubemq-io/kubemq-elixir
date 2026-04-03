defmodule KubeMQ.Transport.GRPC do
  @moduledoc false
  @behaviour KubeMQ.Transport

  require Logger

  @default_send_timeout 5_000
  @default_rpc_timeout 10_000

  @doc false
  @spec send_timeout(keyword()) :: pos_integer()
  def send_timeout(opts), do: Keyword.get(opts, :send_timeout, @default_send_timeout)

  @doc false
  @spec rpc_timeout(keyword()) :: pos_integer()
  def rpc_timeout(opts), do: Keyword.get(opts, :rpc_timeout, @default_rpc_timeout)

  # --- Ping ---

  @impl KubeMQ.Transport
  @spec ping(GRPC.Channel.t()) :: {:ok, map()} | {:error, term()}
  def ping(channel) do
    request = %KubeMQ.Proto.Empty{}

    case KubeMQ.Proto.Stub.ping(channel, request, timeout: @default_rpc_timeout) do
      {:ok, result} ->
        {:ok,
         %{
           host: result.host,
           version: result.version,
           server_start_time: result.server_start_time,
           server_up_time_seconds: result.server_up_time_seconds
         }}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Event (unary) ---

  @impl KubeMQ.Transport
  @spec send_event(GRPC.Channel.t(), map()) :: :ok | {:error, term()}
  def send_event(channel, request) do
    pb_event = build_event_proto(request)

    case KubeMQ.Proto.Stub.send_event(channel, pb_event, timeout: @default_send_timeout) do
      {:ok, result} ->
        if result.sent do
          :ok
        else
          {:error, {:event_not_sent, result.error}}
        end

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Events Stream (bidirectional) ---

  @impl KubeMQ.Transport
  @spec send_event_stream(GRPC.Channel.t()) :: {:ok, pid()} | {:error, term()}
  def send_event_stream(channel) do
    case KubeMQ.Proto.Stub.send_events_stream(channel) do
      %GRPC.Client.Stream{} = stream ->
        {:ok, stream}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Subscribe (server-streaming for events/events_store/commands/queries) ---

  @impl KubeMQ.Transport
  @spec subscribe(GRPC.Channel.t(), map()) :: {:ok, pid()} | {:error, term()}
  def subscribe(channel, subscribe_request) do
    pb_sub = build_subscribe_proto(subscribe_request)

    result =
      case subscribe_request.subscribe_type do
        type when type in [:events, :events_store] ->
          KubeMQ.Proto.Stub.subscribe_to_events(channel, pb_sub)

        type when type in [:commands, :queries] ->
          KubeMQ.Proto.Stub.subscribe_to_requests(channel, pb_sub)
      end

    case result do
      {:ok, stream} ->
        {:ok, stream}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Request (commands/queries) ---

  @impl KubeMQ.Transport
  @spec send_request(GRPC.Channel.t(), map()) :: {:ok, map()} | {:error, term()}
  def send_request(channel, request) do
    pb_request = build_request_proto(request)
    timeout = Map.get(request, :timeout, @default_rpc_timeout)

    case KubeMQ.Proto.Stub.send_request(channel, pb_request, timeout: timeout) do
      {:ok, response} ->
        {:ok, response_from_proto(response)}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Response (commands/queries reply) ---

  @impl KubeMQ.Transport
  @spec send_response(GRPC.Channel.t(), map()) :: :ok | {:error, term()}
  def send_response(channel, response) do
    pb_response = build_response_proto(response)

    case KubeMQ.Proto.Stub.send_response(channel, pb_response, timeout: @default_rpc_timeout) do
      {:ok, _empty} ->
        :ok

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Queue Message (unary) ---

  @impl KubeMQ.Transport
  @spec send_queue_message(GRPC.Channel.t(), map()) :: {:ok, map()} | {:error, term()}
  def send_queue_message(channel, msg) do
    pb_msg = build_queue_message_proto(msg)

    case KubeMQ.Proto.Stub.send_queue_message(channel, pb_msg, timeout: @default_send_timeout) do
      {:ok, result} ->
        {:ok, queue_send_result_from_proto(result)}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Send Queue Messages Batch ---

  @impl KubeMQ.Transport
  @spec send_queue_messages_batch(GRPC.Channel.t(), map()) :: {:ok, map()} | {:error, term()}
  def send_queue_messages_batch(channel, batch) do
    pb_batch =
      %KubeMQ.Proto.QueueMessagesBatchRequest{
        batch_id: batch.batch_id,
        messages: Enum.map(batch.messages, &build_queue_message_proto/1)
      }

    case KubeMQ.Proto.Stub.send_queue_messages_batch(channel, pb_batch,
           timeout: @default_send_timeout
         ) do
      {:ok, result} ->
        {:ok,
         %{
           batch_id: result.batch_id,
           results: Enum.map(result.results || [], &queue_send_result_from_proto/1),
           have_errors: result.have_errors
         }}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Receive Queue Messages ---

  @impl KubeMQ.Transport
  @spec receive_queue_messages(GRPC.Channel.t(), map()) :: {:ok, map()} | {:error, term()}
  def receive_queue_messages(channel, request) do
    pb_request =
      %KubeMQ.Proto.ReceiveQueueMessagesRequest{
        request_id: Map.get(request, :request_id, ""),
        client_id: Map.get(request, :client_id, ""),
        channel: request.channel,
        max_number_of_messages: Map.get(request, :max_messages, 1),
        wait_time_seconds: Map.get(request, :wait_time_seconds, 5),
        is_peak: Map.get(request, :is_peek, false)
      }

    timeout = Map.get(request, :wait_time_seconds, 5) * 1_000 + 5_000

    case KubeMQ.Proto.Stub.receive_queue_messages(channel, pb_request, timeout: timeout) do
      {:ok, result} ->
        {:ok, receive_result_from_proto(result)}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Ack All Queue Messages ---

  @impl KubeMQ.Transport
  @spec ack_all_queue_messages(GRPC.Channel.t(), map()) :: {:ok, map()} | {:error, term()}
  def ack_all_queue_messages(channel, request) do
    pb_request =
      %KubeMQ.Proto.AckAllQueueMessagesRequest{
        request_id: Map.get(request, :request_id, ""),
        client_id: Map.get(request, :client_id, ""),
        channel: request.channel,
        wait_time_seconds: Map.get(request, :wait_time_seconds, 5)
      }

    timeout = Map.get(request, :wait_time_seconds, 5) * 1_000 + 5_000

    case KubeMQ.Proto.Stub.ack_all_queue_messages(channel, pb_request, timeout: timeout) do
      {:ok, result} ->
        {:ok,
         %{
           request_id: result.request_id,
           affected_messages: result.affected_messages,
           is_error: result.is_error,
           error: result.error
         }}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Queue Upstream (bidirectional stream) ---

  @impl KubeMQ.Transport
  @spec queue_upstream(GRPC.Channel.t()) :: {:ok, pid()} | {:error, term()}
  def queue_upstream(channel) do
    case KubeMQ.Proto.Stub.queues_upstream(channel) do
      %GRPC.Client.Stream{} = stream ->
        {:ok, stream}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Queue Downstream (bidirectional stream) ---

  @impl KubeMQ.Transport
  @spec queue_downstream(GRPC.Channel.t()) :: {:ok, pid()} | {:error, term()}
  def queue_downstream(channel) do
    case KubeMQ.Proto.Stub.queues_downstream(channel) do
      %GRPC.Client.Stream{} = stream ->
        {:ok, stream}

      {:error, %GRPC.RPCError{} = err} ->
        {:error, grpc_error_to_tuple(err)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Proto Builders ---

  @spec build_event_proto(map()) :: KubeMQ.Proto.Event.t()
  defp build_event_proto(request) do
    %KubeMQ.Proto.Event{
      event_id: Map.get(request, :id, ""),
      client_id: Map.get(request, :client_id, ""),
      channel: Map.get(request, :channel, ""),
      metadata: Map.get(request, :metadata, ""),
      body: Map.get(request, :body, ""),
      store: Map.get(request, :store, false),
      tags: Map.get(request, :tags, %{})
    }
  end

  @spec build_subscribe_proto(map()) :: KubeMQ.Proto.Subscribe.t()
  defp build_subscribe_proto(request) do
    subscribe_type_data =
      case request.subscribe_type do
        :events -> :Events
        :events_store -> :EventsStore
        :commands -> :Commands
        :queries -> :Queries
      end

    %KubeMQ.Proto.Subscribe{
      subscribe_type_data: subscribe_type_data,
      client_id: Map.get(request, :client_id, ""),
      channel: Map.get(request, :channel, ""),
      group: Map.get(request, :group, ""),
      events_store_type_data:
        Map.get(request, :events_store_type_data, :EventsStoreTypeUndefined),
      events_store_type_value: Map.get(request, :events_store_type_value, 0)
    }
  end

  @spec build_request_proto(map()) :: KubeMQ.Proto.Request.t()
  defp build_request_proto(request) do
    request_type =
      case Map.get(request, :request_type, :command) do
        :command -> :Command
        :query -> :Query
        _ -> :RequestTypeUnknown
      end

    %KubeMQ.Proto.Request{
      request_id: Map.get(request, :id, ""),
      request_type_data: request_type,
      client_id: Map.get(request, :client_id, ""),
      channel: Map.get(request, :channel, ""),
      metadata: Map.get(request, :metadata, ""),
      body: Map.get(request, :body, ""),
      reply_channel: Map.get(request, :reply_channel, ""),
      timeout: Map.get(request, :timeout, @default_rpc_timeout),
      cache_key: Map.get(request, :cache_key, ""),
      cache_ttl: Map.get(request, :cache_ttl, 0),
      span: Map.get(request, :span),
      tags: Map.get(request, :tags, %{})
    }
  end

  @spec build_response_proto(map()) :: KubeMQ.Proto.Response.t()
  defp build_response_proto(response) do
    %KubeMQ.Proto.Response{
      request_id: Map.get(response, :request_id, ""),
      reply_channel: Map.get(response, :reply_channel, ""),
      client_id: Map.get(response, :client_id, ""),
      metadata: Map.get(response, :metadata, ""),
      body: Map.get(response, :body, ""),
      cache_hit: Map.get(response, :cache_hit, false),
      timestamp: Map.get(response, :timestamp, 0),
      executed: Map.get(response, :executed, false),
      error: Map.get(response, :error, ""),
      span: Map.get(response, :span),
      tags: Map.get(response, :tags, %{})
    }
  end

  @spec build_queue_message_proto(map()) :: KubeMQ.Proto.QueueMessage.t()
  defp build_queue_message_proto(msg) do
    policy = Map.get(msg, :policy)

    pb_policy =
      if policy do
        %KubeMQ.Proto.QueueMessagePolicy{
          expiration_seconds: Map.get(policy, :expiration_seconds, 0),
          delay_seconds: Map.get(policy, :delay_seconds, 0),
          max_receive_count: Map.get(policy, :max_receive_count, 0),
          max_receive_queue: Map.get(policy, :max_receive_queue, "")
        }
      end

    %KubeMQ.Proto.QueueMessage{
      message_id: Map.get(msg, :id, ""),
      client_id: Map.get(msg, :client_id, ""),
      channel: Map.get(msg, :channel, ""),
      metadata: Map.get(msg, :metadata, ""),
      body: Map.get(msg, :body, ""),
      tags: Map.get(msg, :tags, %{}),
      policy: pb_policy
    }
  end

  # --- Proto Response Converters ---

  @spec response_from_proto(KubeMQ.Proto.Response.t()) :: map()
  defp response_from_proto(response) do
    %{
      request_id: response.request_id,
      reply_channel: response.reply_channel,
      client_id: response.client_id,
      metadata: response.metadata,
      body: response.body,
      cache_hit: response.cache_hit,
      timestamp: response.timestamp,
      executed: response.executed,
      error: response.error,
      span: response.span,
      tags: response.tags || %{}
    }
  end

  @spec queue_send_result_from_proto(KubeMQ.Proto.SendQueueMessageResult.t()) :: map()
  defp queue_send_result_from_proto(result) do
    %{
      message_id: result.message_id,
      sent_at: result.sent_at,
      expiration_at: result.expiration_at,
      delayed_to: result.delayed_to,
      is_error: result.is_error,
      error: result.error
    }
  end

  @spec receive_result_from_proto(KubeMQ.Proto.ReceiveQueueMessagesResponse.t()) :: map()
  defp receive_result_from_proto(result) do
    messages =
      (result.messages || [])
      |> Enum.map(&queue_message_from_proto/1)

    %{
      request_id: result.request_id,
      messages: messages,
      messages_received: result.messages_received,
      messages_expired: result.messages_expired,
      is_peek: result.is_peak,
      is_error: result.is_error,
      error: result.error
    }
  end

  @doc false
  @spec queue_message_from_proto(KubeMQ.Proto.QueueMessage.t()) :: map()
  def queue_message_from_proto(msg) do
    msg_attributes = msg.attributes

    attributes =
      if msg_attributes do
        %{
          timestamp: msg_attributes.timestamp,
          sequence: msg_attributes.sequence,
          md5_of_body: msg_attributes.md5_of_body,
          receive_count: msg_attributes.receive_count,
          re_routed: msg_attributes.re_routed,
          re_routed_from_queue: msg_attributes.re_routed_from_queue,
          expiration_at: msg_attributes.expiration_at,
          delayed_to: msg_attributes.delayed_to
        }
      end

    msg_policy = msg.policy

    policy =
      if msg_policy do
        %{
          expiration_seconds: msg_policy.expiration_seconds,
          delay_seconds: msg_policy.delay_seconds,
          max_receive_count: msg_policy.max_receive_count,
          max_receive_queue: msg_policy.max_receive_queue
        }
      end

    %{
      id: msg.message_id,
      channel: msg.channel,
      metadata: msg.metadata,
      body: msg.body,
      client_id: msg.client_id,
      tags: msg.tags || %{},
      policy: policy,
      attributes: attributes
    }
  end

  @spec grpc_error_to_tuple(GRPC.RPCError.t()) :: {atom(), String.t()}
  defp grpc_error_to_tuple(%GRPC.RPCError{status: status, message: message}) do
    code = grpc_status_to_error_code(status)
    {code, message || "unknown gRPC error"}
  end

  @spec grpc_status_to_error_code(integer()) :: atom()
  defp grpc_status_to_error_code(status) do
    case status do
      1 -> :cancellation
      2 -> :fatal
      3 -> :validation
      4 -> :timeout
      5 -> :not_found
      6 -> :validation
      7 -> :authorization
      8 -> :throttling
      9 -> :validation
      10 -> :transient
      11 -> :validation
      12 -> :fatal
      13 -> :fatal
      14 -> :transient
      15 -> :fatal
      16 -> :authentication
      _ -> :fatal
    end
  end
end
