defmodule Burnin.Report do
  @moduledoc """
  Report generation for completed burn-in runs.
  Computes loss/error percentages and renders a PASS/FAIL verdict.
  """

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

  defp get_threshold(nil, _key, default), do: default

  defp get_threshold(%{thresholds: t}, key, default) do
    Map.get(t, key, default)
  end
end
