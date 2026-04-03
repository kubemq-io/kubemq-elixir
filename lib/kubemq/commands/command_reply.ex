defmodule KubeMQ.CommandReply do
  @moduledoc """
  Reply constructed by a command subscriber to respond to a received command.

  ## Usage

      reply = KubeMQ.CommandReply.new(
        request_id: command_receive.id,
        response_to: command_receive.reply_channel,
        executed: true
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
      cache_hit: false,
      timestamp: System.system_time(:second),
      executed: reply.executed,
      error: reply.error || "",
      tags: reply.tags || %{}
    }
  end
end
