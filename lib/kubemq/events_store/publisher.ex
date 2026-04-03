defmodule KubeMQ.EventsStore.Publisher do
  @moduledoc false

  alias KubeMQ.{Error, EventStore, EventStoreResult, UUID, Validation}

  @spec send_event_store(GRPC.Channel.t(), module(), EventStore.t(), String.t()) ::
          {:ok, EventStoreResult.t()} | {:error, Error.t()}
  def send_event_store(channel, transport, %EventStore{} = event, client_id) do
    with :ok <- Validation.validate_channel(event.channel),
         :ok <- Validation.validate_content(event.metadata, event.body) do
      request = %{
        id: UUID.ensure(event.id),
        channel: event.channel,
        metadata: event.metadata || "",
        body: event.body || "",
        client_id: event.client_id || client_id,
        tags: event.tags || %{},
        store: true
      }

      case transport.send_event(channel, request) do
        :ok ->
          {:ok, %EventStoreResult{id: request.id, sent: true, error: nil}}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "send_event_store",
             channel: event.channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("send_event_store failed: #{inspect(reason)}",
             operation: "send_event_store",
             channel: event.channel
           )}
      end
    end
  end
end
