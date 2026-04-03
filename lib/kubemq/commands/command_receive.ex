defmodule KubeMQ.CommandReceive do
  @moduledoc """
  Received command from a KubeMQ command subscription.

  Delivered to the `:on_command` callback registered via
  `KubeMQ.Client.subscribe_to_commands/3`. The `reply_channel` field must be
  copied to `KubeMQ.CommandReply.response_to` when constructing a reply.

  ## Fields

    * `id` (`String.t()`) — Unique command identifier.
    * `channel` (`String.t()`) — Channel the command was sent to.
    * `metadata` (`String.t()`) — Metadata string set by the sender.
    * `body` (`binary()`) — Command payload.
    * `reply_channel` (`String.t()`) — Channel for sending the reply back (copy to `CommandReply.response_to`).
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags set by the sender.
    * `span` (`binary() | nil`) — Tracing span context (internal).
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
