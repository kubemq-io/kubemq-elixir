defmodule KubeMQ.Queues.Simple do
  @moduledoc false

  alias KubeMQ.{
    Error,
    QueueAckAllResult,
    QueueBatchResult,
    QueueMessage,
    QueueReceiveResult,
    QueueSendResult,
    UUID,
    Validation
  }

  @spec send_queue_message(GRPC.Channel.t(), module(), QueueMessage.t(), String.t()) ::
          {:ok, QueueSendResult.t()} | {:error, Error.t()}
  def send_queue_message(channel, transport, %QueueMessage{} = msg, client_id) do
    with :ok <- Validation.validate_channel(msg.channel),
         :ok <- Validation.validate_content(msg.metadata, msg.body) do
      request = build_transport_message(msg, client_id)

      case transport.send_queue_message(channel, request) do
        {:ok, result} ->
          {:ok, QueueSendResult.from_transport(result)}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "send_queue_message",
             channel: msg.channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("send_queue_message failed: #{inspect(reason)}",
             operation: "send_queue_message",
             channel: msg.channel
           )}
      end
    end
  end

  @spec send_queue_messages(GRPC.Channel.t(), module(), [QueueMessage.t()], String.t()) ::
          {:ok, QueueBatchResult.t()} | {:error, Error.t()}
  def send_queue_messages(channel, transport, messages, client_id) when is_list(messages) do
    batch_id = UUID.generate()
    transport_messages = Enum.map(messages, &build_transport_message(&1, client_id))

    batch = %{
      batch_id: batch_id,
      messages: transport_messages
    }

    case transport.send_queue_messages_batch(channel, batch) do
      {:ok, result} ->
        {:ok, QueueBatchResult.from_transport(result)}

      {:error, {code, message}} ->
        {:error, Error.from_grpc_status(code, operation: "send_queue_messages", message: message)}

      {:error, reason} ->
        {:error,
         Error.transient("send_queue_messages failed: #{inspect(reason)}",
           operation: "send_queue_messages"
         )}
    end
  end

  @spec receive_queue_messages(GRPC.Channel.t(), module(), String.t(), String.t(), keyword()) ::
          {:ok, QueueReceiveResult.t()} | {:error, Error.t()}
  def receive_queue_messages(channel, transport, queue_channel, client_id, opts \\ []) do
    max_messages = Keyword.get(opts, :max_messages, 1)
    wait_timeout_ms = Keyword.get(opts, :wait_timeout, 5_000)
    is_peek = Keyword.get(opts, :is_peek, false)

    with :ok <- Validation.validate_channel(queue_channel),
         :ok <- Validation.validate_max_messages(max_messages),
         :ok <- Validation.validate_wait_timeout(wait_timeout_ms) do
      wait_time_seconds = max(div(wait_timeout_ms, 1_000), 1)

      request = %{
        request_id: UUID.generate(),
        client_id: client_id,
        channel: queue_channel,
        max_messages: max_messages,
        wait_time_seconds: wait_time_seconds,
        is_peek: is_peek
      }

      case transport.receive_queue_messages(channel, request) do
        {:ok, result} ->
          {:ok, QueueReceiveResult.from_transport(result)}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "receive_queue_messages",
             channel: queue_channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("receive_queue_messages failed: #{inspect(reason)}",
             operation: "receive_queue_messages",
             channel: queue_channel
           )}
      end
    end
  end

  @spec ack_all_queue_messages(GRPC.Channel.t(), module(), String.t(), String.t(), keyword()) ::
          {:ok, QueueAckAllResult.t()} | {:error, Error.t()}
  def ack_all_queue_messages(channel, transport, queue_channel, client_id, opts \\ []) do
    wait_timeout_ms = Keyword.get(opts, :wait_timeout, 5_000)

    with :ok <- Validation.validate_channel(queue_channel),
         :ok <- Validation.validate_wait_timeout(wait_timeout_ms) do
      wait_time_seconds = max(div(wait_timeout_ms, 1_000), 1)

      request = %{
        request_id: UUID.generate(),
        client_id: client_id,
        channel: queue_channel,
        wait_time_seconds: wait_time_seconds
      }

      case transport.ack_all_queue_messages(channel, request) do
        {:ok, result} ->
          {:ok, QueueAckAllResult.from_transport(result)}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "ack_all_queue_messages",
             channel: queue_channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("ack_all_queue_messages failed: #{inspect(reason)}",
             operation: "ack_all_queue_messages",
             channel: queue_channel
           )}
      end
    end
  end

  # --- Private ---

  defp build_transport_message(%QueueMessage{} = msg, default_client_id) do
    policy =
      if msg.policy do
        %{
          expiration_seconds: msg.policy.expiration_seconds,
          delay_seconds: msg.policy.delay_seconds,
          max_receive_count: msg.policy.max_receive_count,
          max_receive_queue: msg.policy.max_receive_queue
        }
      end

    %{
      id: UUID.ensure(msg.id),
      channel: msg.channel,
      metadata: msg.metadata || "",
      body: msg.body || "",
      client_id: msg.client_id || default_client_id,
      tags: msg.tags || %{},
      policy: policy
    }
  end
end
