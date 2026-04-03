defmodule KubeMQ.Commands.Sender do
  @moduledoc false

  alias KubeMQ.{Command, CommandResponse, Error, UUID, Validation}

  @spec send_command(GRPC.Channel.t(), module(), Command.t(), String.t()) ::
          {:ok, CommandResponse.t()} | {:error, Error.t()}
  def send_command(channel, transport, %Command{} = command, client_id) do
    with :ok <- Validation.validate_channel(command.channel),
         :ok <- Validation.validate_content(command.metadata, command.body),
         :ok <- Validation.validate_timeout(command.timeout) do
      request = %{
        id: UUID.ensure(command.id),
        request_type: :command,
        channel: command.channel,
        metadata: command.metadata || "",
        body: command.body || "",
        client_id: command.client_id || client_id,
        timeout: command.timeout,
        tags: command.tags || %{},
        cache_key: "",
        cache_ttl: 0
      }

      case transport.send_request(channel, request) do
        {:ok, response} ->
          {:ok, CommandResponse.from_response(response)}

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code,
             operation: "send_command",
             channel: command.channel,
             message: message
           )}

        {:error, reason} ->
          {:error,
           Error.transient("send_command failed: #{inspect(reason)}",
             operation: "send_command",
             channel: command.channel
           )}
      end
    end
  end

  @spec send_response(GRPC.Channel.t(), module(), KubeMQ.CommandReply.t()) ::
          :ok | {:error, Error.t()}
  def send_response(channel, transport, %KubeMQ.CommandReply{} = reply) do
    with :ok <- Validation.validate_response_fields(reply) do
      response_map = KubeMQ.CommandReply.to_response_map(reply)

      case transport.send_response(channel, response_map) do
        :ok ->
          :ok

        {:error, {code, message}} ->
          {:error,
           Error.from_grpc_status(code, operation: "send_command_response", message: message)}

        {:error, reason} ->
          {:error,
           Error.transient("send_command_response failed: #{inspect(reason)}",
             operation: "send_command_response"
           )}
      end
    end
  end
end
