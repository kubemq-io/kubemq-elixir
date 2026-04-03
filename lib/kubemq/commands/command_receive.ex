defmodule KubeMQ.CommandReceive do
  @moduledoc """
  Received command from a KubeMQ command subscription.

  The `reply_channel` field must be copied to `KubeMQ.CommandReply.response_to`
  when constructing a reply.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          channel: String.t(),
          metadata: String.t(),
          body: binary(),
          reply_channel: String.t(),
          tags: %{String.t() => String.t()},
          span: binary() | nil
        }

  defstruct [:id, :channel, :metadata, :body, :reply_channel, :span, tags: %{}]

  @spec from_proto(map()) :: t()
  def from_proto(request) do
    %__MODULE__{
      id: request.request_id,
      channel: request.channel,
      metadata: request.metadata,
      body: request.body,
      reply_channel: request.reply_channel,
      tags: request.tags || %{},
      span: nil
    }
  end
end
