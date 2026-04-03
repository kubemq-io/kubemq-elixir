if Code.ensure_loaded?(Broadway) do
  defmodule KubeMQ.Broadway.Queues do
    @moduledoc """
    Broadway producer for KubeMQ Queues.

    Polls queue messages via `KubeMQ.Client.poll_queue/2` on demand and delivers
    each `KubeMQ.QueueMessage` as a `Broadway.Message`. Includes a built-in
    `Broadway.Acknowledger` that maps Broadway ack/reject to queue ack/nack.

    ## Usage

        defmodule MyQueuePipeline do
          use Broadway

          def start_link(_opts) do
            Broadway.start_link(__MODULE__,
              name: __MODULE__,
              producer: [
                module: {KubeMQ.Broadway.Queues, [
                  client: MyApp.KubeMQ,
                  channel: "tasks",
                  max_items: 10,
                  wait_timeout: 5_000,
                  auto_ack: false
                ]}
              ],
              processors: [default: [concurrency: 2]]
            )
          end

          @impl true
          def handle_message(_processor, message, _context) do
            queue_msg = message.data
            IO.puts("Processing: \#{queue_msg.channel}")
            message
          end
        end
    """

    use GenStage
    require Logger
    @behaviour Broadway.Producer

    @type option ::
            {:client, GenServer.server()}
            | {:channel, String.t()}
            | {:max_items, pos_integer()}
            | {:wait_timeout, pos_integer()}
            | {:auto_ack, boolean()}

    @impl GenStage
    def init(opts) do
      client = Keyword.fetch!(opts, :client)
      channel = Keyword.fetch!(opts, :channel)

      {:producer,
       %{
         client: client,
         channel: channel,
         max_items: Keyword.get(opts, :max_items, 10),
         wait_timeout: Keyword.get(opts, :wait_timeout, 5_000),
         auto_ack: Keyword.get(opts, :auto_ack, false),
         buffer: :queue.new(),
         demand: 0
       }}
    end

    @impl GenStage
    def handle_demand(incoming_demand, state) do
      new_state = %{state | demand: state.demand + incoming_demand}
      poll_and_emit(new_state)
    end

    @impl GenStage
    def handle_info(:poll, state) do
      poll_and_emit(state)
    end

    def handle_info(_msg, state) do
      {:noreply, [], state}
    end

    @impl Broadway.Producer
    def prepare_for_draining(state) do
      {:noreply, [], state}
    end

    defp poll_and_emit(%{demand: 0} = state), do: {:noreply, [], state}

    defp poll_and_emit(state) do
      items_to_fetch = min(state.demand, state.max_items)

      poll_opts = [
        channel: state.channel,
        max_items: items_to_fetch,
        wait_timeout: state.wait_timeout,
        auto_ack: state.auto_ack
      ]

      case KubeMQ.Client.poll_queue(state.client, poll_opts) do
        {:ok, poll_response} ->
          if poll_response.is_error do
            schedule_poll(state.wait_timeout)
            {:noreply, [], state}
          else
            ack_ref = make_ref()

            acknowledger =
              {__MODULE__, ack_ref, %{poll_response: poll_response, auto_ack: state.auto_ack}}

            messages =
              Enum.map(poll_response.messages, fn msg ->
                %Broadway.Message{
                  data: msg,
                  acknowledger: acknowledger
                }
              end)

            fulfilled = length(messages)
            new_state = %{state | demand: max(state.demand - fulfilled, 0)}

            if new_state.demand > 0 do
              schedule_poll(100)
            end

            {:noreply, messages, new_state}
          end

        {:error, _error} ->
          schedule_poll(state.wait_timeout)
          {:noreply, [], state}
      end
    end

    defp schedule_poll(delay) do
      Process.send_after(self(), :poll, delay)
    end

    @doc false
    @spec ack(term(), [Broadway.Message.t()], [Broadway.Message.t()]) :: :ok
    def ack(_ack_ref, successful, failed) do
      # Group by transaction to avoid duplicate ack/nack for the same poll response
      successful
      |> Enum.uniq_by(fn %{acknowledger: {_, _, %{poll_response: poll}}} ->
        poll.transaction_id
      end)
      |> Enum.each(fn %{acknowledger: {_, _, %{poll_response: poll, auto_ack: auto_ack}}} ->
        unless auto_ack do
          case KubeMQ.PollResponse.ack_all(poll) do
            {:ok, _} ->
              :ok

            {:error, reason} ->
              Logger.warning("[Broadway.Queues] Failed to ack: #{inspect(reason)}")
          end
        end
      end)

      failed
      |> Enum.uniq_by(fn %{acknowledger: {_, _, %{poll_response: poll}}} ->
        poll.transaction_id
      end)
      |> Enum.each(fn %{acknowledger: {_, _, %{poll_response: poll, auto_ack: auto_ack}}} ->
        unless auto_ack do
          case KubeMQ.PollResponse.nack_all(poll) do
            {:ok, _} ->
              :ok

            {:error, reason} ->
              Logger.warning("[Broadway.Queues] Failed to nack: #{inspect(reason)}")
          end
        end
      end)

      :ok
    end
  end
end
