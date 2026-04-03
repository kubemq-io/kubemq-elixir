defmodule KubeMQ.Retry.ReconnectPolicyTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.ReconnectPolicy

  alias KubeMQ.ReconnectPolicy

  describe "new/0" do
    test "creates policy with default values" do
      policy = ReconnectPolicy.new()

      assert %ReconnectPolicy{} = policy
      assert policy.enabled == true
      assert policy.initial_delay == 1_000
      assert policy.max_delay == 30_000
      assert policy.max_attempts == 0
      assert policy.multiplier == 2.0
    end
  end

  describe "new/1" do
    test "creates policy with custom values" do
      policy =
        ReconnectPolicy.new(
          enabled: false,
          initial_delay: 500,
          max_delay: 10_000,
          max_attempts: 5,
          multiplier: 1.5
        )

      assert policy.enabled == false
      assert policy.initial_delay == 500
      assert policy.max_delay == 10_000
      assert policy.max_attempts == 5
      assert policy.multiplier == 1.5
    end
  end

  describe "from_config/1" do
    test "builds policy from keyword list" do
      config = [
        enabled: false,
        initial_delay: 2_000,
        max_delay: 60_000,
        max_attempts: 10,
        multiplier: 3.0
      ]

      policy = ReconnectPolicy.from_config(config)

      assert %ReconnectPolicy{} = policy
      assert policy.enabled == false
      assert policy.initial_delay == 2_000
      assert policy.max_delay == 60_000
      assert policy.max_attempts == 10
      assert policy.multiplier == 3.0
    end
  end
end
