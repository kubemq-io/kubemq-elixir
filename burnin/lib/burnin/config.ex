defmodule Burnin.Config do
  @moduledoc """
  YAML configuration loading and validation matching Go burn-in structure.
  """

  @all_patterns ~w(events events_store queue_stream queue_simple commands queries)

  defstruct mode: "soak",
            duration: "15m",
            run_id: "",
            broker: %{address: ""},
            patterns: %{},
            queue: %{poll_max_messages: 10, poll_wait_timeout_seconds: 5, auto_ack: true},
            rpc: %{timeout_ms: 10_000},
            message: %{size_bytes: 256},
            thresholds: %{max_duplication_pct: 1.0, max_error_rate_pct: 1.0, max_loss_pct: 1.0},
            shutdown: %{drain_timeout_seconds: 10, cleanup_channels: true}

  @type t :: %__MODULE__{}

  @spec from_yaml(String.t()) :: {:ok, t()} | {:error, String.t()}
  def from_yaml(path) do
    case YamlElixir.read_from_file(path) do
      {:ok, data} -> {:ok, from_map(data)}
      {:error, reason} -> {:error, "YAML parse error: #{inspect(reason)}"}
    end
  end

  @spec from_json(String.t()) :: {:ok, t()} | {:error, String.t()}
  def from_json(json) do
    case Jason.decode(json) do
      {:ok, data} -> {:ok, from_map(data)}
      {:error, reason} -> {:error, "JSON parse error: #{inspect(reason)}"}
    end
  end

  @spec from_map(map()) :: t()
  def from_map(data) when is_map(data) do
    %__MODULE__{
      mode: Map.get(data, "mode", "soak"),
      duration: Map.get(data, "duration", "15m"),
      run_id: Map.get(data, "run_id", ""),
      broker: parse_broker(Map.get(data, "broker", %{})),
      patterns: parse_patterns(Map.get(data, "patterns", %{})),
      queue: parse_queue(Map.get(data, "queue", %{})),
      rpc: parse_rpc(Map.get(data, "rpc", %{})),
      message: parse_message(Map.get(data, "message", %{})),
      thresholds: parse_thresholds(Map.get(data, "thresholds", %{})),
      shutdown: parse_shutdown(Map.get(data, "shutdown", %{}))
    }
  end

  @spec validate(t()) :: :ok | {:error, [String.t()]}
  def validate(%__MODULE__{} = cfg) do
    errors =
      []
      |> validate_mode(cfg)
      |> validate_duration(cfg)
      |> validate_patterns(cfg)

    case errors do
      [] -> :ok
      errs -> {:error, errs}
    end
  end

  def all_patterns, do: @all_patterns

  def pattern_enabled?(%__MODULE__{patterns: pats}, name) do
    case Map.get(pats, name) do
      nil -> true
      %{enabled: enabled} -> enabled
    end
  end

  def pattern_config(%__MODULE__{patterns: pats}, name) do
    Map.get(pats, name, default_pattern())
  end

  def effective_broker(%__MODULE__{broker: %{address: addr}}, startup) do
    if addr != "" and addr != nil, do: addr, else: startup
  end

  @spec parse_duration(String.t()) :: non_neg_integer()
  def parse_duration(s) when is_binary(s) do
    case Regex.run(~r/^(\d+)([smhd])$/, s) do
      [_, val, "s"] -> String.to_integer(val) * 1_000
      [_, val, "m"] -> String.to_integer(val) * 60_000
      [_, val, "h"] -> String.to_integer(val) * 3_600_000
      [_, val, "d"] -> String.to_integer(val) * 86_400_000
      _ -> 0
    end
  end

  def parse_duration(_), do: 0

  defp parse_broker(nil), do: %{address: ""}
  defp parse_broker(m), do: %{address: Map.get(m, "address", "")}

  defp parse_patterns(nil), do: %{}

  defp parse_patterns(m) when is_map(m) do
    Map.new(m, fn {name, cfg} ->
      {name,
       %{
         enabled: Map.get(cfg, "enabled", true),
         channels: Map.get(cfg, "channels", 1),
         producers_per_channel: Map.get(cfg, "producers_per_channel", 1),
         consumers_per_channel: Map.get(cfg, "consumers_per_channel", 1),
         senders_per_channel: Map.get(cfg, "senders_per_channel", 1),
         responders_per_channel: Map.get(cfg, "responders_per_channel", 1),
         rate: Map.get(cfg, "rate", 10)
       }}
    end)
  end

  defp parse_queue(nil), do: %{poll_max_messages: 10, poll_wait_timeout_seconds: 5, auto_ack: true}

  defp parse_queue(m) do
    %{
      poll_max_messages: Map.get(m, "poll_max_messages", 10),
      poll_wait_timeout_seconds: Map.get(m, "poll_wait_timeout_seconds", 5),
      auto_ack: Map.get(m, "auto_ack", true)
    }
  end

  defp parse_rpc(nil), do: %{timeout_ms: 10_000}
  defp parse_rpc(m), do: %{timeout_ms: Map.get(m, "timeout_ms", 10_000)}

  defp parse_message(nil), do: %{size_bytes: 256}
  defp parse_message(m), do: %{size_bytes: Map.get(m, "size_bytes", 256)}

  defp parse_thresholds(nil), do: %{max_duplication_pct: 1.0, max_error_rate_pct: 1.0, max_loss_pct: 1.0}

  defp parse_thresholds(m) do
    %{
      max_duplication_pct: Map.get(m, "max_duplication_pct", 1.0),
      max_error_rate_pct: Map.get(m, "max_error_rate_pct", 1.0),
      max_loss_pct: Map.get(m, "max_loss_pct", 1.0)
    }
  end

  defp parse_shutdown(nil), do: %{drain_timeout_seconds: 10, cleanup_channels: true}

  defp parse_shutdown(m) do
    %{
      drain_timeout_seconds: Map.get(m, "drain_timeout_seconds", 10),
      cleanup_channels: Map.get(m, "cleanup_channels", true)
    }
  end

  defp default_pattern do
    %{enabled: true, channels: 1, producers_per_channel: 1, consumers_per_channel: 1,
      senders_per_channel: 1, responders_per_channel: 1, rate: 10}
  end

  defp validate_mode(errs, %{mode: m}) when m in ["soak", "benchmark"], do: errs
  defp validate_mode(errs, %{mode: m}), do: ["mode: must be 'soak' or 'benchmark', got '#{m}'" | errs]

  defp validate_duration(errs, %{duration: d}) do
    if Regex.match?(~r/^\d+[smhd]$/, d), do: errs, else: ["duration: invalid format '#{d}'" | errs]
  end

  defp validate_patterns(errs, %{patterns: pats}) do
    enabled = Enum.count(@all_patterns, fn n -> pattern_enabled_in_map?(pats, n) end)
    if enabled == 0, do: ["no patterns enabled" | errs], else: errs
  end

  defp pattern_enabled_in_map?(pats, name) do
    case Map.get(pats, name) do
      nil -> true
      %{enabled: e} -> e
    end
  end
end
