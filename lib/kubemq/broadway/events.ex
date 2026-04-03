if Code.ensure_loaded?(Broadway) do
  defmodule KubeMQ.Broadway.Events do
    @moduledoc """
    Broadway producer for KubeMQ Events (Pub/Sub).

    Wraps `KubeMQ.Client.subscribe_to_events/3` and delivers each
    `KubeMQ.EventReceive` as a `Broadway.Message`.

    ## Usage

        defmodule MyPipeline do
          use Broadway

          def start_link(_opts) do
            Broadway.start_link(__MODULE__,
              name: __MODULE__,
              producer: [
                module: {KubeMQ.Broadway.Events, [
                  client: MyApp.KubeMQ,
                  channel: "events.>",
                  group: "pipeline"
                ]}
              ],
              processors: [default: [concurrency: 4]]
            )
          end

          @impl true
          def handle_message(_processor, message, _context) do
            event = message.data
            IO.inspect(event.channel, label: "received on")
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
            | {:group, String.t()}
            | {:max_buffer_size, pos_integer()}

    @impl GenStage
    def init(opts) do
      client = Keyword.fetch!(opts, :client)
      channel = Keyword.fetch!(opts, :channel)
      group = Keyword.get(opts, :group, "")
      max_buffer = Keyword.get(opts, :max_buffer_size, 10_000)

      {:producer,
       %{
         client: client,
         channel: channel,
         group: group,
         subscription: nil,
         sub_ref: nil,
         buffer: :queue.new(),
         buffer_size: 0,
         max_buffer_size: max_buffer,
         demand: 0
       }, {:continue, :subscribe}}
    end

    def handle_continue(:subscribe, state) do
      subscribe_opts = [group: state.group, notify: self()]

      case KubeMQ.Client.subscribe_to_events(state.client, state.channel, subscribe_opts) do
        {:ok, subscription} ->
          ref = Process.monitor(subscription.pid)
          {:noreply, [], %{state | subscription: subscription, sub_ref: ref}}

        {:error, _error} ->
          Process.send_after(self(), :retry_subscribe, 5_000)
          {:noreply, [], state}
      end
    end

    @impl GenStage
    def handle_demand(incoming_demand, state) do
      {messages, new_state} = drain_buffer(%{state | demand: state.demand + incoming_demand})
      {:noreply, messages, new_state}
    end

    @impl GenStage
    def handle_info({:DOWN, ref, :process, _pid, _reason}, %{sub_ref: ref} = state) do
      Logger.warning("[Broadway.Events] Subscription process died, resubscribing...")
      Process.send_after(self(), :retry_subscribe, 5_000)
      {:noreply, [], %{state | subscription: nil, sub_ref: nil}}
    end

    def handle_info(:retry_subscribe, state) do
      {:noreply, [], state, {:continue, :subscribe}}
    end

    def handle_info({:kubemq_event, event}, state) do
      if state.buffer_size >= state.max_buffer_size do
        Logger.warning("[Broadway.Events] Buffer full (#{state.max_buffer_size}), dropping event")
        {:noreply, [], state}
      else
        new_buffer = :queue.in(event, state.buffer)

        {messages, new_state} =
          drain_buffer(%{state | buffer: new_buffer, buffer_size: state.buffer_size + 1})

        {:noreply, messages, new_state}
      end
    end

    def handle_info(_msg, state) do
      {:noreply, [], state}
    end

    @impl Broadway.Producer
    def prepare_for_draining(state) do
      if state.subscription && KubeMQ.Subscription.active?(state.subscription) do
        KubeMQ.Subscription.cancel(state.subscription)
      end

      {:noreply, [], %{state | subscription: nil, sub_ref: nil}}
    end

    defp drain_buffer(%{demand: 0} = state), do: {[], state}
    defp drain_buffer(%{buffer_size: 0} = state), do: {[], state}

    defp drain_buffer(state) do
      take = min(state.demand, state.buffer_size)
      {items, remaining} = dequeue_n(state.buffer, take, [])

      messages =
        Enum.map(items, fn event ->
          %Broadway.Message{
            data: event,
            acknowledger: {__MODULE__, :ack_id, :ack_data}
          }
        end)

      {messages,
       %{
         state
         | buffer: remaining,
           buffer_size: state.buffer_size - take,
           demand: state.demand - take
       }}
    end

    @doc false
    @spec ack(term(), [Broadway.Message.t()], [Broadway.Message.t()]) :: :ok
    def ack(_ack_ref, _successful, _failed), do: :ok

    defp dequeue_n(queue, 0, acc), do: {Enum.reverse(acc), queue}

    defp dequeue_n(queue, n, acc) do
      case :queue.out(queue) do
        {{:value, item}, rest} -> dequeue_n(rest, n - 1, [item | acc])
        {:empty, rest} -> {Enum.reverse(acc), rest}
      end
    end
  end
end
