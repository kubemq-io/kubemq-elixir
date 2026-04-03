defmodule Burnin.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    port = System.get_env("BURNIN_PORT", "8888") |> String.to_integer()
    broker = System.get_env("KUBEMQ_BROKER_ADDRESS", "localhost:50000")

    children = [
      {Burnin.Metrics, []},
      {Burnin.Engine, broker: broker},
      {Bandit, plug: Burnin.Server, port: port, scheme: :http}
    ]

    opts = [strategy: :one_for_one, name: Burnin.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
