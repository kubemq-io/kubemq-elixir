defmodule KubeMQ.PollRequestTest do
  use ExUnit.Case, async: true

  alias KubeMQ.PollRequest

  describe "new/0" do
    test "returns struct with defaults" do
      req = PollRequest.new()
      assert %PollRequest{} = req
      assert req.channel == nil
      assert req.max_items == 1
      assert req.wait_timeout == 5_000
      assert req.auto_ack == false
    end
  end

  describe "new/1" do
    test "with custom values" do
      req =
        PollRequest.new(
          channel: "orders",
          max_items: 10,
          wait_timeout: 30_000,
          auto_ack: true
        )

      assert req.channel == "orders"
      assert req.max_items == 10
      assert req.wait_timeout == 30_000
      assert req.auto_ack == true
    end
  end
end
