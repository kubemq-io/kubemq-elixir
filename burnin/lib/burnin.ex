defmodule Burnin do
  @moduledoc """
  KubeMQ Elixir SDK burn-in soak-test application.

  Provides an HTTP control plane (Plug + Bandit) for starting/stopping
  burn-in runs across all 6 KubeMQ messaging patterns.
  """

  @version "0.1.0"
  @sdk "kubemq-elixir"
  @burnin_version "2.0.0"
  @burnin_spec_version "2"

  def version, do: @version
  def sdk, do: @sdk
  def burnin_version, do: @burnin_version
  def burnin_spec_version, do: @burnin_spec_version
end
