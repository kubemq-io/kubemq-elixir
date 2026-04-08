defmodule Burnin.Report do
  @moduledoc """
  Report generation for completed burn-in runs.
  Computes loss/error percentages and renders a PASS/FAIL verdict.
  """

  @rpc_patterns ~w(commands queries)

  @spec generate_verdict([map()], Burnin.Config.t() | nil) :: String.t()
  def generate_verdict([], _config), do: "NO_PATTERNS"

  def generate_verdict(pattern_statuses, config) do
    {total_sent, total_recv, total_err, pattern_lines} =
      Enum.reduce(pattern_statuses, {0, 0, 0, []}, fn status, {ts, tr, te, lines} ->
        sent = status.sent
        recv = status.received
        errs = status.errors
        loss_pct = if sent > 0, do: (sent - recv) / sent * 100, else: 0.0
        err_pct = if sent > 0, do: errs / sent * 100, else: 0.0

        line =
          "  #{status.name}: sent=#{sent} recv=#{recv} err=#{errs} " <>
            "loss=#{:erlang.float_to_binary(loss_pct, decimals: 2)}% " <>
            "errRate=#{:erlang.float_to_binary(err_pct, decimals: 2)}%"

        {ts + sent, tr + recv, te + errs, [line | lines]}
      end)

    overall_loss = if total_sent > 0, do: (total_sent - total_recv) / total_sent * 100, else: 0.0
    overall_err = if total_sent > 0, do: total_err / total_sent * 100, else: 0.0

    max_loss = get_threshold(config, :max_loss_pct, 1.0)
    max_error = get_threshold(config, :max_error_rate_pct, 1.0)
    pass? = overall_loss <= max_loss and overall_err <= max_error

    header = if pass?, do: "PASS", else: "FAIL"

    total_line =
      "Total: sent=#{total_sent} recv=#{total_recv} err=#{total_err} " <>
        "loss=#{:erlang.float_to_binary(overall_loss, decimals: 2)}% " <>
        "errRate=#{:erlang.float_to_binary(overall_err, decimals: 2)}%"

    [header, total_line | Enum.reverse(pattern_lines)]
    |> Enum.join("\n")
  end

  @doc """
  Generates the full dashboard-compatible report with all required fields.
  Called at stop time and stored in engine state for later retrieval.
  """
  @spec generate_full_report([map()], Burnin.Config.t() | nil, non_neg_integer(), Burnin.Tracker.t(), String.t()) :: map()
  def generate_full_report(pattern_statuses, config, elapsed_seconds, tracker, broker) do
    {total_errors, patterns_map} = build_patterns_detail(pattern_statuses)

    verdict_result =
      cond do
        total_errors == 0 -> "PASSED"
        total_errors > 0 -> "PASSED_WITH_WARNINGS"
      end

    # Check against thresholds for FAILED verdict
    {total_sent, total_recv, _total_err} = aggregate_totals(pattern_statuses)
    overall_loss = if total_sent > 0, do: (total_sent - total_recv) / total_sent * 100, else: 0.0
    overall_err = if total_sent > 0, do: total_errors / total_sent * 100, else: 0.0
    max_loss = get_threshold(config, :max_loss_pct, 1.0)
    max_error = get_threshold(config, :max_error_rate_pct, 1.0)

    verdict_result =
      if overall_loss > max_loss or overall_err > max_error do
        "FAILED"
      else
        verdict_result
      end

    memory_mb = div(:erlang.memory(:total), 1_024 * 1_024)

    ended_at = DateTime.utc_now()
    started_at = tracker.started_at || ended_at

    %{
      run_id: tracker.run_id,
      sdk: Burnin.sdk(),
      sdk_version: Burnin.version(),
      mode: if(config, do: config.mode, else: "soak"),
      broker_address: broker,
      started_at: DateTime.to_iso8601(started_at),
      ended_at: DateTime.to_iso8601(ended_at),
      duration_seconds: elapsed_seconds,
      all_patterns_enabled: true,
      warmup_active: false,
      patterns: patterns_map,
      resources: %{
        peak_rss_mb: memory_mb,
        baseline_rss_mb: 0,
        memory_growth_factor: 1.0,
        peak_workers: length(pattern_statuses)
      },
      verdict: %{
        result: verdict_result,
        warnings: [],
        checks: %{}
      }
    }
  end

  @doc """
  Generates the legacy simple report (kept for backward compatibility).
  """
  @spec generate_report([map()], Burnin.Config.t() | nil, non_neg_integer()) :: map()
  def generate_report(pattern_statuses, config, elapsed_seconds) do
    %{
      verdict: generate_verdict(pattern_statuses, config),
      elapsed_seconds: elapsed_seconds,
      patterns:
        Enum.map(pattern_statuses, fn s ->
          %{
            name: s.name,
            sent: s.sent,
            received: s.received,
            errors: s.errors,
            channels: s.channels
          }
        end)
    }
  end

  # Builds the patterns detail map keyed by pattern name, matching dashboard format.
  # Returns {total_errors, patterns_map}.
  defp build_patterns_detail(pattern_statuses) do
    # Build map from actual worker statuses
    status_by_name =
      Map.new(pattern_statuses, fn s -> {s.name, s} end)

    all_pattern_names = ~w(events events_store queue_stream queue_simple commands queries)

    {total_errors, patterns_list} =
      Enum.reduce(all_pattern_names, {0, []}, fn name, {err_acc, list_acc} ->
        case Map.get(status_by_name, name) do
          nil ->
            {err_acc, [{name, %{enabled: false}} | list_acc]}

          s ->
            detail =
              if name in @rpc_patterns do
                %{
                  enabled: true,
                  state: "stopped",
                  channels: s.channels,
                  sent: s.sent,
                  responses_success: s.received,
                  responses_timeout: 0,
                  responses_error: 0,
                  errors: s.errors,
                  reconnections: 0,
                  target_rate: 0,
                  actual_rate: 0,
                  avg_rate: 0,
                  peak_rate: 0,
                  bytes_sent: 0,
                  bytes_received: 0,
                  latency: %{p50_ms: 0, p95_ms: 0, p99_ms: 0, p999_ms: 0},
                  senders: [],
                  responders: []
                }
              else
                %{
                  enabled: true,
                  state: "stopped",
                  channels: s.channels,
                  sent: s.sent,
                  received: s.received,
                  lost: 0,
                  duplicated: 0,
                  corrupted: 0,
                  out_of_order: 0,
                  errors: s.errors,
                  reconnections: 0,
                  loss_pct: 0,
                  target_rate: 0,
                  actual_rate: 0,
                  avg_rate: 0,
                  peak_rate: 0,
                  bytes_sent: 0,
                  bytes_received: 0,
                  latency: %{p50_ms: 0, p95_ms: 0, p99_ms: 0, p999_ms: 0},
                  producers: [],
                  consumers: []
                }
              end

            {err_acc + s.errors, [{name, detail} | list_acc]}
        end
      end)

    patterns_map = Map.new(patterns_list)
    {total_errors, patterns_map}
  end

  defp aggregate_totals(pattern_statuses) do
    Enum.reduce(pattern_statuses, {0, 0, 0}, fn s, {ts, tr, te} ->
      {ts + s.sent, tr + s.received, te + s.errors}
    end)
  end

  defp get_threshold(nil, _key, default), do: default

  defp get_threshold(%{thresholds: t}, key, default) do
    Map.get(t, key, default)
  end
end
