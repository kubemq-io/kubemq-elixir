defmodule KubeMQ.Command do
  @moduledoc """
  Command message for the KubeMQ RPC pattern.

  Commands are fire-and-wait messages: the sender blocks until the receiver
  executes and responds, or the timeout expires.

  ## Usage

      cmd = KubeMQ.Command.new(channel: "orders.process", body: payload, timeout: 10_000)
      {:ok, response} = KubeMQ.Client.send_command(client, cmd)
  """

  @type t :: %__MODULE__{
          id: String.t() | nil,
          channel: String.t(),
          metadata: String.t() | nil,
          body: binary() | nil,
          timeout: pos_integer(),
          client_id: String.t() | nil,
          tags: %{String.t() => String.t()},
          span: binary() | nil
        }

  defstruct [:id, :channel, :metadata, :body, :timeout, :client_id, :span, tags: %{}]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id),
      channel: Keyword.get(opts, :channel),
      metadata: Keyword.get(opts, :metadata),
      body: Keyword.get(opts, :body),
      timeout: Keyword.get(opts, :timeout, 10_000),
      client_id: Keyword.get(opts, :client_id),
      tags: Keyword.get(opts, :tags, %{}),
      span: Keyword.get(opts, :span)
    }
  end
end
