defmodule Burnin do
  @moduledoc """
  KubeMQ Elixir SDK burn-in soak-test application.

  Provides an HTTP control plane (Plug + Bandit) for starting/stopping
  burn-in runs across all 6 KubeMQ messaging patterns.
  """

  @version "0.1.0"
  @sdk "kubemq-elixir"

  def version, do: @version
  def sdk, do: @sdk
end
