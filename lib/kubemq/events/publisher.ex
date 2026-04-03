defmodule KubeMQ.Events.Publisher do
  @moduledoc false

  alias KubeMQ.{Error, Event, UUID, Validation}

  @spec send_event(GRPC.Channel.t(), module(), Event.t(), String.t()) ::
          :ok | {:error, Error.t()}
  def send_event(channel, transport, %Event{} = event, client_id) do
    with :ok <- Validation.validate_channel(event.channel),
         :ok <- Validation.validate_content(event.metadata, event.body) do
      request = %{
        id: UUID.ensure(event.id),
        channel: event.channel,
        metadata: event.metadata || "",
        body: event.body || "",
        client_id: event.client_id || client_id,
        tags: event.tags || %{},
        store: false
      }

      case transport.send_event(channel, request) do
        :ok ->
          :ok

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "send_event",
             channel: event.channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("send_event failed: #{inspect(reason)}",
             operation: "send_event",
             channel: event.channel
           )}
      end
    end
  end
end
