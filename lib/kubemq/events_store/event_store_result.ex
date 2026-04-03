defmodule KubeMQ.EventStoreResult do
  @moduledoc """
  Result of sending a persistent event to KubeMQ Events Store.

  Returned from `KubeMQ.Client.send_event_store/2`.

  ## Fields

    * `id` (`String.t()`) — Event identifier echoed from the server.
    * `sent` (`boolean()`) — Whether the event was successfully persisted.
    * `error` (`String.t() | nil`) — Error message if `sent` is `false`.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          sent: boolean(),
          error: String.t() | nil
        }

  defstruct [:id, :sent, :error]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id, ""),
      sent: Keyword.get(opts, :sent, false),
      error: Keyword.get(opts, :error)
    }
  end

  @spec from_proto(map()) :: t()
  def from_proto(result) do
    %__MODULE__{
      id: result.event_id || "",
      sent: result.sent || false,
      error: if(result.error in [nil, ""], do: nil, else: result.error)
    }
  end
end
