defmodule KubeMQ.MockHelpersTest do
  use ExUnit.Case, async: false

  import Mox
  alias KubeMQ.MockHelpers

  setup :verify_on_exit!

  describe "expect_ping_success/1" do
    test "sets up mock transport to return successful ping" do
      MockHelpers.expect_ping_success()
      {:ok, result} = KubeMQ.MockTransport.ping(:fake_channel)
      assert is_map(result)
      assert Map.has_key?(result, :Host)
      assert Map.get(result, :Host) == "localhost"
      assert Map.get(result, :Version) == "2.0.0"
    end
  end

  describe "expect_ping_error/2" do
    test "sets up mock transport to return ping error with default" do
      MockHelpers.expect_ping_error()
      assert {:error, :unavailable} = KubeMQ.MockTransport.ping(:fake_channel)
    end

    test "sets up mock transport to return ping error with custom error" do
      MockHelpers.expect_ping_error(KubeMQ.MockTransport, :timeout)
      assert {:error, :timeout} = KubeMQ.MockTransport.ping(:fake_channel)
    end
  end

  describe "expect_send_event_success/1" do
    test "sets up mock transport to return :ok for send_event" do
      MockHelpers.expect_send_event_success()
      assert :ok = KubeMQ.MockTransport.send_event(:fake_channel, %{})
    end
  end

  describe "expect_send_request_success/2" do
    test "sets up mock transport to return successful request response" do
      MockHelpers.expect_send_request_success()
      {:ok, result} = KubeMQ.MockTransport.send_request(:fake_channel, %{})
      assert Map.get(result, :Executed) == true
      assert Map.get(result, :CacheHit) == false
    end

    test "respects executed option" do
      MockHelpers.expect_send_request_success(KubeMQ.MockTransport, executed: false)
      {:ok, result} = KubeMQ.MockTransport.send_request(:fake_channel, %{})
      assert Map.get(result, :Executed) == false
    end

    test "respects cache_hit option" do
      MockHelpers.expect_send_request_success(KubeMQ.MockTransport, cache_hit: true)
      {:ok, result} = KubeMQ.MockTransport.send_request(:fake_channel, %{})
      assert Map.get(result, :CacheHit) == true
    end
  end

  describe "expect_send_queue_message_success/1" do
    test "sets up mock transport to return successful queue send" do
      MockHelpers.expect_send_queue_message_success()
      {:ok, result} = KubeMQ.MockTransport.send_queue_message(:fake_channel, %{})
      assert is_binary(Map.get(result, :MessageID))
      assert Map.get(result, :IsError) == false
    end
  end

  describe "expect_receive_queue_messages_success/2" do
    test "sets up mock transport to return queue messages with default count" do
      MockHelpers.expect_receive_queue_messages_success()
      {:ok, result} = KubeMQ.MockTransport.receive_queue_messages(:fake_channel, %{})
      assert Map.get(result, :MessagesReceived) == 1
      assert length(Map.get(result, :Messages)) == 1
    end

    test "sets up mock transport to return multiple queue messages" do
      MockHelpers.expect_receive_queue_messages_success(KubeMQ.MockTransport, 5)
      {:ok, result} = KubeMQ.MockTransport.receive_queue_messages(:fake_channel, %{})
      assert Map.get(result, :MessagesReceived) == 5
      assert length(Map.get(result, :Messages)) == 5
    end
  end

  describe "expect_ack_all_success/1" do
    test "sets up mock transport to return successful ack_all" do
      MockHelpers.expect_ack_all_success()
      {:ok, result} = KubeMQ.MockTransport.ack_all_queue_messages(:fake_channel, %{})
      assert Map.get(result, :AffectedMessages) == 5
      assert Map.get(result, :IsError) == false
    end
  end
end
