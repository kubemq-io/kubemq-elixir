defmodule KubeMQ.QueuePolicyTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueuePolicy

  describe "new/0" do
    test "returns struct with zero/empty defaults" do
      policy = QueuePolicy.new()
      assert %QueuePolicy{} = policy
      assert policy.expiration_seconds == 0
      assert policy.delay_seconds == 0
      assert policy.max_receive_count == 0
      assert policy.max_receive_queue == ""
    end
  end

  describe "new/1" do
    test "with custom values" do
      policy =
        QueuePolicy.new(
          expiration_seconds: 300,
          delay_seconds: 60,
          max_receive_count: 5,
          max_receive_queue: "dead-letter"
        )

      assert policy.expiration_seconds == 300
      assert policy.delay_seconds == 60
      assert policy.max_receive_count == 5
      assert policy.max_receive_queue == "dead-letter"
    end
  end
end
