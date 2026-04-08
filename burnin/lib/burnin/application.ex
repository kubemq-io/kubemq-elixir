defmodule Burnin.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    config_path = parse_config_arg(System.argv())
    port = load_metrics_port(config_path)
    broker = System.get_env("KUBEMQ_BROKER_ADDRESS", "localhost:50000")

    children = [
      {Burnin.Metrics, []},
      {Burnin.Engine, broker: broker},
      {Bandit, plug: Burnin.Server, port: port, scheme: :http}
    ]

    opts = [strategy: :one_for_one, name: Burnin.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp parse_config_arg(argv) do
    case Enum.find_index(argv, &(&1 == "--config")) do
      nil -> nil
      idx -> Enum.at(argv, idx + 1)
    end
  end

  defp load_metrics_port(nil) do
    System.get_env("BURNIN_PORT", "8888") |> String.to_integer()
  end

  defp load_metrics_port(path) do
    case YamlElixir.read_from_file(path) do
      {:ok, data} ->
        get_in(data, ["metrics", "port"]) ||
          System.get_env("BURNIN_PORT", "8888") |> String.to_integer()

      _ ->
        System.get_env("BURNIN_PORT", "8888") |> String.to_integer()
    end
  end
end
