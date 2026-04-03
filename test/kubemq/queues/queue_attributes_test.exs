defmodule KubeMQ.QueueAttributesTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueAttributes

  describe "new/0" do
    test "returns struct with zero/false/empty defaults" do
      attrs = QueueAttributes.new()
      assert %QueueAttributes{} = attrs
      assert attrs.timestamp == 0
      assert attrs.sequence == 0
      assert attrs.md5_of_body == ""
      assert attrs.receive_count == 0
      assert attrs.re_routed == false
      assert attrs.re_routed_from_queue == ""
      assert attrs.expiration_at == 0
      assert attrs.delayed_to == 0
    end
  end

  describe "new/1" do
    test "with custom values" do
      attrs =
        QueueAttributes.new(
          timestamp: 1_000_000,
          sequence: 10,
          md5_of_body: "hash123",
          receive_count: 3,
          re_routed: true,
          re_routed_from_queue: "source-q",
          expiration_at: 2_000_000,
          delayed_to: 1_500_000
        )

      assert attrs.timestamp == 1_000_000
      assert attrs.sequence == 10
      assert attrs.md5_of_body == "hash123"
      assert attrs.receive_count == 3
      assert attrs.re_routed == true
      assert attrs.re_routed_from_queue == "source-q"
      assert attrs.expiration_at == 2_000_000
      assert attrs.delayed_to == 1_500_000
    end
  end
end
