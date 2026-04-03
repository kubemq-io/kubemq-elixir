defmodule KubeMQ.QueueBatchResult do
  @moduledoc """
  Result of sending a batch of queue messages.

  Returned from `KubeMQ.Client.send_queue_messages/2`.

  ## Fields

    * `batch_id` (`String.t()`) — Batch identifier.
    * `results` (`[KubeMQ.QueueSendResult.t()]`) — Per-message send results.
    * `have_errors` (`boolean()`) — Whether any message in the batch failed to send.
  """

  @type t :: %__MODULE__{
          batch_id: String.t(),
          results: [KubeMQ.QueueSendResult.t()],
          have_errors: boolean()
        }

  defstruct [:batch_id, :results, :have_errors]

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      batch_id: Keyword.get(opts, :batch_id, ""),
      results: Keyword.get(opts, :results, []),
      have_errors: Keyword.get(opts, :have_errors, false)
    }
  end

  @spec from_transport(map()) :: t()
  def from_transport(result) do
    %__MODULE__{
      batch_id: result.batch_id || "",
      results: Enum.map(result.results || [], &KubeMQ.QueueSendResult.from_transport/1),
      have_errors: result.have_errors || false
    }
  end
end
