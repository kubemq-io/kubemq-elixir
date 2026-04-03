defmodule KubeMQ.PollRequest do
  @moduledoc """
  Request parameters for polling queue messages via the Stream API.

  ## Fields

    * `channel` (`String.t()`) — Queue channel to poll from.
    * `max_items` (`pos_integer()`) — Maximum number of messages to receive. Default: `1`.
    * `wait_timeout` (`pos_integer()`) — How long to wait for messages in milliseconds. Default: `5_000`.
    * `auto_ack` (`boolean()`) — Automatically acknowledge messages on receive. Default: `false`.
  """

  @type t :: %__MODULE__{
          channel: String.t(),
          max_items: pos_integer(),
          wait_timeout: pos_integer(),
          auto_ack: boolean()
        }

  defstruct [:channel, max_items: 1, wait_timeout: 5_000, auto_ack: false]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      channel: Keyword.get(opts, :channel),
      max_items: Keyword.get(opts, :max_items, 1),
      wait_timeout: Keyword.get(opts, :wait_timeout, 5_000),
      auto_ack: Keyword.get(opts, :auto_ack, false)
    }
  end
end
