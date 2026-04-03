defmodule KubeMQ.RetryPolicyTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.RetryPolicy

  alias KubeMQ.RetryPolicy

  describe "should_retry?/2" do
    test "returns true within limit" do
      policy = RetryPolicy.new(max_retries: 3)
      assert RetryPolicy.should_retry?(policy, 0)
      assert RetryPolicy.should_retry?(policy, 1)
      assert RetryPolicy.should_retry?(policy, 2)
    end

    test "returns false at limit" do
      policy = RetryPolicy.new(max_retries: 3)
      refute RetryPolicy.should_retry?(policy, 3)
    end

    test "returns false beyond limit" do
      policy = RetryPolicy.new(max_retries: 2)
      refute RetryPolicy.should_retry?(policy, 5)
    end

    test "zero max_retries means never retry" do
      policy = RetryPolicy.new(max_retries: 0)
      refute RetryPolicy.should_retry?(policy, 0)
    end
  end

  describe "delay_for_attempt/2" do
    test "exponential backoff" do
      policy = RetryPolicy.new(initial_delay: 100, multiplier: 2.0, max_delay: 10_000)

      assert RetryPolicy.delay_for_attempt(policy, 0) == 100
      assert RetryPolicy.delay_for_attempt(policy, 1) == 200
      assert RetryPolicy.delay_for_attempt(policy, 2) == 400
      assert RetryPolicy.delay_for_attempt(policy, 3) == 800
    end

    test "max_delay caps at 1_000 with default policy" do
      policy = RetryPolicy.new()

      # Default: initial_delay=100, multiplier=2.0, max_delay=1_000
      # Attempt 0: 100, 1: 200, 2: 400, 3: 800, 4: 1_000 (capped)
      assert RetryPolicy.delay_for_attempt(policy, 0) == 100
      assert RetryPolicy.delay_for_attempt(policy, 4) == 1_000
      assert RetryPolicy.delay_for_attempt(policy, 10) == 1_000
    end
  end

  describe "from_config/1" do
    test "builds policy from keyword list" do
      config = [max_retries: 5, initial_delay: 200, max_delay: 2_000, multiplier: 3.0]
      policy = RetryPolicy.from_config(config)

      assert %RetryPolicy{} = policy
      assert policy.max_retries == 5
      assert policy.initial_delay == 200
      assert policy.max_delay == 2_000
      assert policy.multiplier == 3.0
    end

    test "uses defaults for missing keys" do
      policy = RetryPolicy.from_config([])

      assert policy.max_retries == 3
      assert policy.initial_delay == 100
      assert policy.max_delay == 1_000
      assert policy.multiplier == 2.0
    end
  end

  describe "new/1" do
    test "creates policy with defaults" do
      policy = RetryPolicy.new()
      assert policy.max_retries == 3
      assert policy.initial_delay == 100
      assert policy.max_delay == 1_000
      assert policy.multiplier == 2.0
    end

    test "creates policy with custom values" do
      policy = RetryPolicy.new(max_retries: 10, initial_delay: 50)
      assert policy.max_retries == 10
      assert policy.initial_delay == 50
    end

    test "creates policy with all custom values" do
      policy =
        RetryPolicy.new(max_retries: 5, initial_delay: 200, max_delay: 5_000, multiplier: 3.0)

      assert policy.max_retries == 5
      assert policy.initial_delay == 200
      assert policy.max_delay == 5_000
      assert policy.multiplier == 3.0
    end

    test "partial overrides keep defaults for unspecified keys" do
      policy = RetryPolicy.new(max_retries: 7)
      assert policy.max_retries == 7
      assert policy.initial_delay == 100
      assert policy.max_delay == 1_000
      assert policy.multiplier == 2.0
    end
  end

  describe "delay_for_attempt/2 edge cases" do
    test "attempt 0 returns initial_delay" do
      policy = RetryPolicy.new(initial_delay: 50, multiplier: 2.0, max_delay: 10_000)
      assert RetryPolicy.delay_for_attempt(policy, 0) == 50
    end

    test "caps at max_delay with custom multiplier" do
      policy = RetryPolicy.new(initial_delay: 100, multiplier: 10.0, max_delay: 500)
      # Attempt 0: 100, Attempt 1: 1000 -> capped to 500
      assert RetryPolicy.delay_for_attempt(policy, 0) == 100
      assert RetryPolicy.delay_for_attempt(policy, 1) == 500
    end

    test "large attempt number stays capped" do
      policy = RetryPolicy.new(initial_delay: 100, multiplier: 2.0, max_delay: 1_000)
      assert RetryPolicy.delay_for_attempt(policy, 100) == 1_000
    end
  end

  describe "should_retry?/2 edge cases" do
    test "max_retries 1 allows only attempt 0" do
      policy = RetryPolicy.new(max_retries: 1)
      assert RetryPolicy.should_retry?(policy, 0)
      refute RetryPolicy.should_retry?(policy, 1)
    end
  end
end
