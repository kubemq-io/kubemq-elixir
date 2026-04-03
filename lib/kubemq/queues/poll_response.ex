defmodule KubeMQ.PollResponse do
  @moduledoc """
  Response from polling queue messages, with transaction management methods.

  Tracks transaction state to prevent double-ack/nack. Transaction methods
  delegate to the downstream GenServer that owns the bidi stream.

  ## Fields

    * `transaction_id` (`String.t()`) — Server-assigned transaction identifier.
    * `messages` (`[KubeMQ.QueueMessage.t()]`) — List of received queue messages.
    * `is_error` (`boolean()`) — Whether the poll itself returned an error.
    * `error` (`String.t() | nil`) — Error message if `is_error` is true.
    * `state` (`state()`) — Transaction state: `:pending`, `:acked`, `:nacked`, or `:expired`. Default: `:pending`.
    * `downstream_pid` (`pid() | nil`) — PID of the downstream GenServer managing the bidi stream.

  ## Usage

      {:ok, poll} = KubeMQ.Client.poll_queue(client, channel: "orders", max_items: 10)
      # Process messages...
      {:ok, _} = KubeMQ.PollResponse.ack_all(poll)
  """

  alias KubeMQ.{Error, Validation}

  @type state :: :pending | :acked | :nacked | :expired

  @type t :: %__MODULE__{
          transaction_id: String.t(),
          messages: [KubeMQ.QueueMessage.t()],
          is_error: boolean(),
          error: String.t() | nil,
          state: state(),
          downstream_pid: pid() | nil
        }

  defstruct [:transaction_id, :messages, :is_error, :error, :downstream_pid, state: :pending]

  @doc """
  Create a new PollResponse struct.

  ## Options

    * `:transaction_id` — Transaction ID string (default: `""`)
    * `:messages` — List of `KubeMQ.QueueMessage` structs (default: `[]`)
    * `:is_error` — Whether the poll returned an error (default: `false`)
    * `:error` — Error message string if `is_error` is true
    * `:state` — Transaction state atom (default: `:pending`)
    * `:downstream_pid` — PID of the downstream GenServer

  ## Examples

      iex> poll = KubeMQ.PollResponse.new(transaction_id: "tx-1", messages: [])
      iex> poll.state
      :pending
      iex> poll.transaction_id
      "tx-1"
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      transaction_id: Keyword.get(opts, :transaction_id, ""),
      messages: Keyword.get(opts, :messages, []),
      is_error: Keyword.get(opts, :is_error, false),
      error: Keyword.get(opts, :error),
      state: Keyword.get(opts, :state, :pending),
      downstream_pid: Keyword.get(opts, :downstream_pid)
    }
  end

  @doc """
  Acknowledge all messages in this poll response.

  Transitions the response state from `:pending` to `:acked`. Returns
  `{:error, %KubeMQ.Error{}}` if already acked/nacked or if the downstream
  process is not alive.

  ## Errors

    * `:validation` — transaction is not in `:pending` state
    * `:stream_broken` — downstream process is not alive
  """
  @spec ack_all(t()) :: {:ok, t()} | {:error, Error.t()}
  def ack_all(%__MODULE__{} = poll) do
    with :ok <- validate_pending(poll) do
      case safe_call(poll.downstream_pid, {:ack_all, poll.transaction_id}) do
        :ok -> {:ok, %{poll | state: :acked}}
        {:error, _} = err -> err
      end
    end
  end

  @doc """
  Reject (negative-acknowledge) all messages in this poll response.

  Transitions the response state from `:pending` to `:nacked`, making
  the messages available for redelivery.

  ## Errors

    * `:validation` — transaction is not in `:pending` state
    * `:stream_broken` — downstream process is not alive
  """
  @spec nack_all(t()) :: {:ok, t()} | {:error, Error.t()}
  def nack_all(%__MODULE__{} = poll) do
    with :ok <- validate_pending(poll) do
      case safe_call(poll.downstream_pid, {:nack_all, poll.transaction_id}) do
        :ok -> {:ok, %{poll | state: :nacked}}
        {:error, _} = err -> err
      end
    end
  end

  @doc """
  Requeue all messages to a different channel.

  Moves all messages from this poll response to `requeue_channel` and
  transitions the state to `:acked`.

  ## Errors

    * `:validation` — transaction is not in `:pending` state, or requeue channel is invalid
    * `:stream_broken` — downstream process is not alive
  """
  @spec requeue_all(t(), channel :: String.t()) :: {:ok, t()} | {:error, Error.t()}
  def requeue_all(%__MODULE__{} = poll, requeue_channel) do
    with :ok <- validate_pending(poll),
         :ok <- Validation.validate_requeue_channel(requeue_channel) do
      case safe_call(poll.downstream_pid, {:requeue_all, poll.transaction_id, requeue_channel}) do
        :ok -> {:ok, %{poll | state: :acked}}
        {:error, _} = err -> err
      end
    end
  end

  @doc """
  Acknowledge a specific range of messages by their sequence numbers.

  Unlike `ack_all/1`, this does not transition the overall transaction state,
  allowing partial acknowledgment within a poll response.

  ## Errors

    * `:validation` — transaction is not in `:pending` state
    * `:stream_broken` — downstream process is not alive
  """
  @spec ack_range(t(), sequences :: [integer()]) :: :ok | {:error, Error.t()}
  def ack_range(%__MODULE__{} = poll, sequences) when is_list(sequences) do
    with :ok <- validate_pending(poll) do
      safe_call(poll.downstream_pid, {:ack_range, poll.transaction_id, sequences})
    end
  end

  @doc """
  Reject a specific range of messages by their sequence numbers.

  Rejected messages become available for redelivery. Does not transition
  the overall transaction state.

  ## Errors

    * `:validation` — transaction is not in `:pending` state
    * `:stream_broken` — downstream process is not alive
  """
  @spec nack_range(t(), sequences :: [integer()]) :: :ok | {:error, Error.t()}
  def nack_range(%__MODULE__{} = poll, sequences) when is_list(sequences) do
    with :ok <- validate_pending(poll) do
      safe_call(poll.downstream_pid, {:nack_range, poll.transaction_id, sequences})
    end
  end

  @doc """
  Requeue a specific range of messages to a different channel.

  Moves messages identified by `sequences` to `requeue_channel`. Does not
  transition the overall transaction state.

  ## Errors

    * `:validation` — transaction is not in `:pending` state, or requeue channel is invalid
    * `:stream_broken` — downstream process is not alive
  """
  @spec requeue_range(t(), sequences :: [integer()], channel :: String.t()) ::
          :ok | {:error, Error.t()}
  def requeue_range(%__MODULE__{} = poll, sequences, requeue_channel)
      when is_list(sequences) do
    with :ok <- validate_pending(poll),
         :ok <- Validation.validate_requeue_channel(requeue_channel) do
      safe_call(
        poll.downstream_pid,
        {:requeue_range, poll.transaction_id, sequences, requeue_channel}
      )
    end
  end

  @doc """
  Get the list of active (unacknowledged) message offsets in this transaction.

  ## Errors

    * `:validation` — transaction is not in `:pending` state
    * `:stream_broken` — downstream process is not alive
  """
  @spec active_offsets(t()) :: {:ok, [integer()]} | {:error, Error.t()}
  def active_offsets(%__MODULE__{} = poll) do
    with :ok <- validate_pending(poll) do
      safe_call(poll.downstream_pid, {:active_offsets, poll.transaction_id})
    end
  end

  @doc """
  Check whether the transaction is still active on the server.

  Returns `{:ok, true}` if the transaction is active, `{:ok, false}` if expired.

  ## Errors

    * `:stream_broken` — downstream process is not alive
  """
  @spec transaction_status(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def transaction_status(%__MODULE__{} = poll) do
    safe_call(poll.downstream_pid, {:transaction_status, poll.transaction_id})
  end

  # --- Private ---

  defp safe_call(pid, message, timeout \\ 5_000) do
    GenServer.call(pid, message, timeout)
  catch
    :exit, {:noproc, _} ->
      {:error,
       Error.stream_broken("downstream process is not alive", operation: "queue_transaction")}

    :exit, {:normal, _} ->
      {:error,
       Error.stream_broken("downstream process has stopped", operation: "queue_transaction")}

    :exit, reason ->
      {:error,
       Error.stream_broken("downstream process exited: #{inspect(reason)}",
         operation: "queue_transaction"
       )}
  end

  defp validate_pending(%__MODULE__{state: :pending}), do: :ok

  defp validate_pending(%__MODULE__{state: current_state}) do
    {:error, Error.validation("transaction already #{current_state}, cannot perform operation")}
  end
end
