defmodule KubeMQ.PollRequest do
  @moduledoc """
  Request parameters for polling queue messages via the Stream API.
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
