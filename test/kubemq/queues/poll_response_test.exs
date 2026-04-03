defmodule KubeMQ.PollResponseTest do
  use ExUnit.Case, async: true

  alias KubeMQ.{Error, PollResponse}

  describe "safe_call handles noproc" do
    test "ack_all on dead downstream returns error" do
      # Start and immediately kill a process to get a dead pid
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(20)
      refute Process.alive?(dead_pid)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: dead_pid
      }

      result = PollResponse.ack_all(poll)
      assert {:error, %Error{code: :stream_broken}} = result
    end

    test "nack_all on dead downstream returns error" do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(20)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: dead_pid
      }

      result = PollResponse.nack_all(poll)
      assert {:error, %Error{code: :stream_broken}} = result
    end

    test "requeue_all on dead downstream returns error" do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(20)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: dead_pid
      }

      result = PollResponse.requeue_all(poll, "requeue-channel")
      assert {:error, %Error{code: :stream_broken}} = result
    end
  end

  describe "state tracking" do
    test "ack_all returns {:ok, %PollResponse{state: :acked}} on success" do
      # We need a live GenServer that responds to {:ack_all, txn_id}
      {:ok, mock_downstream} =
        Agent.start_link(fn -> :ok end)

      # Replace the Agent with a process that handles GenServer.call
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:ack_all, _txn_id}} ->
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      result = PollResponse.ack_all(poll)
      assert {:ok, %PollResponse{state: :acked}} = result

      # Clean up
      Agent.stop(mock_downstream)
    end

    test "nack_all returns {:ok, %PollResponse{state: :nacked}} on success" do
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:nack_all, _txn_id}} ->
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      result = PollResponse.nack_all(poll)
      assert {:ok, %PollResponse{state: :nacked}} = result
    end

    test "requeue_all returns {:ok, %PollResponse{state: :acked}} on success" do
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:requeue_all, _txn_id, _channel}} ->
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      result = PollResponse.requeue_all(poll, "requeue-ch")
      assert {:ok, %PollResponse{state: :acked}} = result
    end

    test "ack_all rejects already-acked poll response" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :acked,
        downstream_pid: self()
      }

      result = PollResponse.ack_all(poll)
      assert {:error, %Error{code: :validation}} = result
    end

    test "nack_all rejects already-nacked poll response" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :nacked,
        downstream_pid: self()
      }

      result = PollResponse.nack_all(poll)
      assert {:error, %Error{code: :validation}} = result
    end

    test "ack_all rejects expired poll response" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :expired,
        downstream_pid: self()
      }

      result = PollResponse.ack_all(poll)
      assert {:error, %Error{code: :validation}} = result
    end
  end

  describe "new/1" do
    test "creates poll response with defaults" do
      poll = PollResponse.new()
      assert poll.transaction_id == ""
      assert poll.messages == []
      assert poll.is_error == false
      assert poll.error == nil
      assert poll.state == :pending
      assert poll.downstream_pid == nil
    end

    test "creates poll response with options" do
      poll =
        PollResponse.new(
          transaction_id: "txn-1",
          messages: [:msg1],
          is_error: true,
          error: "something failed",
          state: :acked,
          downstream_pid: self()
        )

      assert poll.transaction_id == "txn-1"
      assert poll.messages == [:msg1]
      assert poll.is_error == true
      assert poll.error == "something failed"
      assert poll.state == :acked
      assert poll.downstream_pid == self()
    end
  end

  # --- EC9: duplicate ack/nack on already-settled transaction ---

  describe "ack_all/nack_all on already settled (EC9)" do
    test "ack_all on already-nacked returns validation error" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :nacked,
        downstream_pid: self()
      }

      assert {:error, %Error{code: :validation}} = PollResponse.ack_all(poll)
    end

    test "nack_all on already-acked returns validation error" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :acked,
        downstream_pid: self()
      }

      assert {:error, %Error{code: :validation}} = PollResponse.nack_all(poll)
    end
  end

  describe "requeue_all validation" do
    test "requeue_all rejects empty requeue channel" do
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, _msg} -> GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      assert {:error, %Error{code: :validation}} = PollResponse.requeue_all(poll, "")
    end

    test "requeue_all rejects nil requeue channel" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: self()
      }

      assert {:error, %Error{code: :validation}} = PollResponse.requeue_all(poll, nil)
    end
  end

  describe "ack_range/nack_range/requeue_range" do
    test "ack_range on pending delegates to downstream" do
      test_pid = self()

      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:ack_range, "txn-1", [1, 2, 3]}} ->
              send(test_pid, :ack_range_called)
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      assert :ok = PollResponse.ack_range(poll, [1, 2, 3])
      assert_receive :ack_range_called
    end

    test "nack_range on pending delegates to downstream" do
      test_pid = self()

      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:nack_range, "txn-1", [4, 5]}} ->
              send(test_pid, :nack_range_called)
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      assert :ok = PollResponse.nack_range(poll, [4, 5])
      assert_receive :nack_range_called
    end

    test "requeue_range on pending delegates to downstream" do
      test_pid = self()

      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:requeue_range, "txn-1", [1], "other-ch"}} ->
              send(test_pid, :requeue_range_called)
              GenServer.reply(from, :ok)
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      assert :ok = PollResponse.requeue_range(poll, [1], "other-ch")
      assert_receive :requeue_range_called
    end

    test "ack_range rejects already-settled transaction" do
      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :acked,
        downstream_pid: self()
      }

      assert {:error, %Error{code: :validation}} = PollResponse.ack_range(poll, [1])
    end
  end

  describe "active_offsets" do
    test "active_offsets on pending delegates to downstream" do
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:active_offsets, "txn-1"}} ->
              GenServer.reply(from, {:ok, [1, 2, 3]})
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: downstream_pid
      }

      assert {:ok, [1, 2, 3]} = PollResponse.active_offsets(poll)
    end
  end

  describe "transaction_status" do
    test "transaction_status delegates to downstream (does not check pending)" do
      downstream_pid =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:transaction_status, "txn-1"}} ->
              GenServer.reply(from, {:ok, true})
          end
        end)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :acked,
        downstream_pid: downstream_pid
      }

      assert {:ok, true} = PollResponse.transaction_status(poll)
    end
  end

  describe "safe_call dead process edge cases" do
    test "ack_range on dead downstream returns stream_broken error" do
      dead_pid = spawn(fn -> :ok end)
      Process.sleep(20)
      refute Process.alive?(dead_pid)

      poll = %PollResponse{
        transaction_id: "txn-1",
        messages: [],
        is_error: false,
        error: nil,
        state: :pending,
        downstream_pid: dead_pid
      }

      assert {:error, %Error{code: :stream_broken}} = PollResponse.ack_range(poll, [1])
    end
  end
end
