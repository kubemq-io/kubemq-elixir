defmodule KubeMQ.MockHelpers do
  @moduledoc false

  import Mox

  @doc """
  Sets up the mock transport to return a successful ping response.
  """
  def expect_ping_success(mock \\ KubeMQ.MockTransport) do
    expect(mock, :ping, fn _channel ->
      {:ok,
       %{
         Host: "localhost",
         Version: "2.0.0",
         ServerStartTime: System.system_time(:nanosecond),
         ServerUpTimeSeconds: 120
       }}
    end)
  end

  @doc """
  Sets up the mock transport to return a ping error.
  """
  def expect_ping_error(mock \\ KubeMQ.MockTransport, error \\ :unavailable) do
    expect(mock, :ping, fn _channel ->
      {:error, error}
    end)
  end

  @doc """
  Sets up the mock transport to accept a send_event and return success.
  """
  def expect_send_event_success(mock \\ KubeMQ.MockTransport) do
    expect(mock, :send_event, fn _channel, _request ->
      :ok
    end)
  end

  @doc """
  Sets up the mock transport to accept a send_request and return a successful
  command/query response.
  """
  def expect_send_request_success(mock \\ KubeMQ.MockTransport, opts \\ []) do
    executed = Keyword.get(opts, :executed, true)
    cache_hit = Keyword.get(opts, :cache_hit, false)

    expect(mock, :send_request, fn _channel, _request ->
      {:ok,
       %{
         RequestID: KubeMQ.UUID.generate(),
         Executed: executed,
         Timestamp: System.system_time(:nanosecond),
         Error: "",
         CacheHit: cache_hit,
         Metadata: "",
         Body: "",
         Tags: %{}
       }}
    end)
  end

  @doc """
  Sets up the mock transport to accept a send_queue_message and return success.
  """
  def expect_send_queue_message_success(mock \\ KubeMQ.MockTransport) do
    expect(mock, :send_queue_message, fn _channel, _msg ->
      {:ok,
       %{
         MessageID: KubeMQ.UUID.generate(),
         SentAt: System.system_time(:nanosecond),
         ExpirationAt: 0,
         DelayedTo: 0,
         IsError: false,
         Error: ""
       }}
    end)
  end

  @doc """
  Sets up the mock transport to accept receive_queue_messages and return messages.
  """
  def expect_receive_queue_messages_success(mock \\ KubeMQ.MockTransport, count \\ 1) do
    expect(mock, :receive_queue_messages, fn _channel, _request ->
      messages =
        Enum.map(1..count, fn i ->
          %{
            id: KubeMQ.UUID.generate(),
            channel: "test-queue",
            metadata: "",
            body: "message-#{i}",
            client_id: "test",
            tags: %{},
            policy: nil,
            attributes: nil
          }
        end)

      {:ok,
       %{
         RequestID: KubeMQ.UUID.generate(),
         Messages: messages,
         MessagesReceived: count,
         MessagesExpired: 0,
         IsPeak: false,
         IsError: false,
         Error: ""
       }}
    end)
  end

  @doc """
  Sets up the mock transport to accept ack_all_queue_messages and return success.
  """
  def expect_ack_all_success(mock \\ KubeMQ.MockTransport) do
    expect(mock, :ack_all_queue_messages, fn _channel, _request ->
      {:ok,
       %{
         RequestID: KubeMQ.UUID.generate(),
         AffectedMessages: 5,
         IsError: false,
         Error: ""
       }}
    end)
  end
end
