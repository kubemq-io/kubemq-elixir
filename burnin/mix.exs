defmodule Burnin.MixProject do
  use Mix.Project

  def project do
    [
      app: :burnin,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Burnin.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:kubemq, path: "../"},
      {:plug, "~> 1.16"},
      {:bandit, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:yaml_elixir, "~> 2.11"}
    ]
  end
end
