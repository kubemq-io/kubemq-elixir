defmodule KubeMQ.QueueBatchResultTest do
  use ExUnit.Case, async: true

  alias KubeMQ.QueueBatchResult
  alias KubeMQ.QueueSendResult

  describe "new/0" do
    test "returns struct with defaults" do
      result = QueueBatchResult.new()
      assert %QueueBatchResult{} = result
      assert result.batch_id == ""
      assert result.results == []
      assert result.have_errors == false
    end
  end

  describe "from_transport/1" do
    test "nested results are converted to QueueSendResult structs" do
      transport = %{
        batch_id: "batch-1",
        have_errors: true,
        results: [
          %{
            message_id: "m1",
            sent_at: 100,
            expiration_at: 200,
            delayed_to: 0,
            is_error: false,
            error: nil
          },
          %{
            message_id: "m2",
            sent_at: 0,
            expiration_at: 0,
            delayed_to: 0,
            is_error: true,
            error: "failed"
          }
        ]
      }

      result = QueueBatchResult.from_transport(transport)
      assert result.batch_id == "batch-1"
      assert result.have_errors == true
      assert length(result.results) == 2

      [r1, r2] = result.results
      assert %QueueSendResult{} = r1
      assert r1.message_id == "m1"
      assert r1.error == nil

      assert %QueueSendResult{} = r2
      assert r2.message_id == "m2"
      assert r2.is_error == true
      assert r2.error == "failed"
    end
  end
end
