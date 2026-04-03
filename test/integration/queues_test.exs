defmodule KubeMQ.Integration.QueuesTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-queues-#{System.unique_integer([:positive])}"

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-queues-test"
      )

    on_exit(fn ->
      if Process.alive?(client), do: Client.close(client)
    end)

    %{client: client}
  end

  describe "queues send/poll/ack" do
    test "send -> poll -> ack -> verify no redelivery", %{client: client} do
      channel = unique_channel()

      # Send a queue message
      msg = %KubeMQ.QueueMessage{
        channel: channel,
        body: "queue message 1",
        client_id: "integration-queues-test"
      }

      {:ok, send_result} = Client.send_queue_message(client, msg)
      refute send_result.is_error

      # Poll for the message
      {:ok, poll} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 5_000,
          auto_ack: false
        )

      refute poll.is_error
      assert length(poll.messages) == 1
      [received_msg] = poll.messages
      assert received_msg.body == "queue message 1"

      # Ack the message
      {:ok, _acked_poll} = KubeMQ.PollResponse.ack_all(poll)

      # Verify no redelivery -- poll again with short timeout
      {:ok, poll2} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 2_000,
          auto_ack: false
        )

      # Should have no messages or be an error (timeout with no messages)
      assert poll2.messages == [] or poll2.is_error
    end

    test "send batch -> poll multiple", %{client: client} do
      channel = unique_channel()

      # Send a batch of messages
      msgs =
        for i <- 1..3 do
          %KubeMQ.QueueMessage{
            channel: channel,
            body: "batch-#{i}",
            client_id: "integration-queues-test"
          }
        end

      {:ok, batch_result} = Client.send_queue_messages(client, msgs)
      refute batch_result.have_errors

      # Poll for all messages
      {:ok, poll} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 10,
          wait_timeout: 5_000,
          auto_ack: true
        )

      refute poll.is_error
      assert length(poll.messages) >= 3
    end

    test "nack requeues message for redelivery", %{client: client} do
      channel = unique_channel()

      msg = %KubeMQ.QueueMessage{
        channel: channel,
        body: "nack me",
        client_id: "integration-queues-test"
      }

      {:ok, _send_result} = Client.send_queue_message(client, msg)

      # Poll and nack
      {:ok, poll} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 5_000,
          auto_ack: false
        )

      refute poll.is_error
      assert length(poll.messages) == 1

      {:ok, _nacked_poll} = KubeMQ.PollResponse.nack_all(poll)

      # Poll again -- message should be redelivered
      {:ok, poll2} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 5_000,
          auto_ack: true
        )

      refute poll2.is_error
      assert length(poll2.messages) == 1
      [redelivered] = poll2.messages
      assert redelivered.body == "nack me"
    end

    test "auto_ack consumes without explicit ack", %{client: client} do
      channel = unique_channel()

      msg = %KubeMQ.QueueMessage{
        channel: channel,
        body: "auto ack me",
        client_id: "integration-queues-test"
      }

      {:ok, _send_result} = Client.send_queue_message(client, msg)

      # Poll with auto_ack
      {:ok, poll} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 5_000,
          auto_ack: true
        )

      refute poll.is_error
      assert length(poll.messages) == 1

      # Should not be redelivered
      {:ok, poll2} =
        Client.poll_queue(client,
          channel: channel,
          max_items: 1,
          wait_timeout: 2_000,
          auto_ack: false
        )

      assert poll2.messages == [] or poll2.is_error
    end
  end
end
