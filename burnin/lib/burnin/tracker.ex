defmodule Burnin.Tracker do
  @moduledoc """
  Run progress tracking — aggregates per-worker stats into a unified view.
  """

  defstruct run_id: "",
            state: :idle,
            started_at: nil,
            workers: [],
            verdict: ""

  @type run_state :: :idle | :starting | :running | :stopping | :completed | :failed
  @type t :: %__MODULE__{
          run_id: String.t(),
          state: run_state(),
          started_at: DateTime.t() | nil,
          workers: [map()],
          verdict: String.t()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec start(t(), String.t()) :: t()
  def start(tracker, run_id) do
    %{tracker | run_id: run_id, state: :starting, started_at: DateTime.utc_now(), verdict: ""}
  end

  @spec running(t()) :: t()
  def running(tracker), do: %{tracker | state: :running}

  @spec stopping(t()) :: t()
  def stopping(tracker), do: %{tracker | state: :stopping}

  @spec complete(t(), String.t()) :: t()
  def complete(tracker, verdict) do
    %{tracker | state: :completed, verdict: verdict}
  end

  @spec fail(t(), String.t()) :: t()
  def fail(tracker, reason) do
    %{tracker | state: :failed, verdict: "FAILED: #{reason}"}
  end

  @spec set_workers(t(), [pid()]) :: t()
  def set_workers(tracker, worker_pids) do
    %{tracker | workers: Enum.map(worker_pids, &%{pid: &1})}
  end

  @spec elapsed_seconds(t()) :: non_neg_integer()
  def elapsed_seconds(%{started_at: nil}), do: 0

  def elapsed_seconds(%{started_at: started}) do
    DateTime.diff(DateTime.utc_now(), started, :second)
  end

  @spec to_status_map(t(), [map()]) :: map()
  def to_status_map(tracker, pattern_statuses) do
    %{
      state: Atom.to_string(tracker.state),
      run_id: tracker.run_id,
      started_at: format_time(tracker.started_at),
      elapsed_seconds: elapsed_seconds(tracker),
      patterns: pattern_statuses,
      verdict: tracker.verdict
    }
  end

  defp format_time(nil), do: ""
  defp format_time(dt), do: DateTime.to_iso8601(dt)
end
