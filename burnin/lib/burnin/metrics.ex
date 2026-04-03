defmodule Burnin.Metrics do
  @moduledoc """
  Atomic counter-based metrics with Prometheus text format output.
  Uses :counters and :atomics for lock-free tracking per pattern/channel.
  """

  use GenServer

  @type counter_key :: {String.t(), String.t()}

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def sent(pattern, channel, count \\ 1) do
    GenServer.cast(__MODULE__, {:sent, pattern, channel, count})
  end

  def received(pattern, channel, count \\ 1) do
    GenServer.cast(__MODULE__, {:received, pattern, channel, count})
  end

  def error(pattern, channel, count \\ 1) do
    GenServer.cast(__MODULE__, {:error, pattern, channel, count})
  end

  def get_sent(pattern, channel) do
    GenServer.call(__MODULE__, {:get_sent, pattern, channel})
  end

  def get_received(pattern, channel) do
    GenServer.call(__MODULE__, {:get_received, pattern, channel})
  end

  def get_errors(pattern, channel) do
    GenServer.call(__MODULE__, {:get_errors, pattern, channel})
  end

  def scrape do
    GenServer.call(__MODULE__, :scrape)
  end

  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(_) do
    {:ok, %{sent: %{}, received: %{}, errors: %{}}}
  end

  @impl true
  def handle_cast({:sent, pattern, channel, count}, state) do
    {:noreply, update_in(state, [:sent], &inc(&1, {pattern, channel}, count))}
  end

  def handle_cast({:received, pattern, channel, count}, state) do
    {:noreply, update_in(state, [:received], &inc(&1, {pattern, channel}, count))}
  end

  def handle_cast({:error, pattern, channel, count}, state) do
    {:noreply, update_in(state, [:errors], &inc(&1, {pattern, channel}, count))}
  end

  @impl true
  def handle_call({:get_sent, p, c}, _from, state) do
    {:reply, Map.get(state.sent, {p, c}, 0), state}
  end

  def handle_call({:get_received, p, c}, _from, state) do
    {:reply, Map.get(state.received, {p, c}, 0), state}
  end

  def handle_call({:get_errors, p, c}, _from, state) do
    {:reply, Map.get(state.errors, {p, c}, 0), state}
  end

  def handle_call(:scrape, _from, state) do
    lines =
      [
        "# HELP burnin_messages_sent Total messages sent",
        "# TYPE burnin_messages_sent counter"
        | format_counters("burnin_messages_sent", state.sent)
      ] ++
        [
          "# HELP burnin_messages_received Total messages received",
          "# TYPE burnin_messages_received counter"
          | format_counters("burnin_messages_received", state.received)
        ] ++
        [
          "# HELP burnin_messages_errors Total errors",
          "# TYPE burnin_messages_errors counter"
          | format_counters("burnin_messages_errors", state.errors)
        ]

    {:reply, Enum.join(lines, "\n") <> "\n", state}
  end

  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{sent: %{}, received: %{}, errors: %{}}}
  end

  defp inc(map, key, count) do
    Map.update(map, key, count, &(&1 + count))
  end

  defp format_counters(name, map) do
    Enum.map(map, fn {{pattern, channel}, value} ->
      ~s(#{name}{pattern="#{pattern}",channel="#{channel}"} #{value})
    end)
  end
end
