defmodule KubeMQ.Command do
  @moduledoc """
  Command message for the KubeMQ RPC pattern.

  Commands are fire-and-wait messages: the sender blocks until the receiver
  executes and responds, or the timeout expires.

  ## Fields

    * `id` (`String.t() | nil`) — Unique command identifier. Auto-generated if nil.
    * `channel` (`String.t()`) — Target channel name. Required for sending.
    * `metadata` (`String.t() | nil`) — Optional metadata string.
    * `body` (`binary() | nil`) — Command payload.
    * `timeout` (`pos_integer()`) — Timeout in milliseconds. Default: `10_000`.
    * `client_id` (`String.t() | nil`) — Sender identifier. Set automatically by `KubeMQ.Client`.
    * `tags` (`%{String.t() => String.t()}`) — Key-value tags. Default: `%{}`.
    * `span` (`binary() | nil`) — Tracing span context (internal).

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

  @doc """
  Create a new Command struct from keyword options.

  ## Options

    * `:id` — Command ID string (auto-generated if nil)
    * `:channel` — Target channel name (required for sending)
    * `:metadata` — Metadata string
    * `:body` — Command payload (binary)
    * `:timeout` — Timeout in milliseconds (default: `10_000`)
    * `:client_id` — Client identifier (set automatically by `KubeMQ.Client`)
    * `:tags` — Key-value tags map (default: `%{}`)

  ## Examples

      iex> cmd = KubeMQ.Command.new(channel: "orders.process", body: "data")
      iex> cmd.channel
      "orders.process"
      iex> cmd.timeout
      10000
  """
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
