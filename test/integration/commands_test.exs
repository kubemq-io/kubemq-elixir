defmodule KubeMQ.Integration.CommandsTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-commands-#{System.unique_integer([:positive])}"

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-commands-test"
      )

    on_exit(fn ->
      if Process.alive?(client), do: Client.close(client)
    end)

    %{client: client}
  end

  describe "commands request/response" do
    test "subscribe -> send -> receive response", %{client: client} do
      channel = unique_channel()

      # Subscribe to commands and reply with executed: true
      {:ok, _sub} =
        Client.subscribe_to_commands(client, channel,
          on_command: fn cmd_receive ->
            KubeMQ.CommandReply.new(
              request_id: cmd_receive.id,
              response_to: cmd_receive.reply_channel,
              executed: true
            )
          end
        )

      Process.sleep(500)

      # Send a command
      command = %KubeMQ.Command{
        channel: channel,
        body: "do something",
        timeout: 10_000,
        client_id: "integration-commands-test"
      }

      {:ok, response} = Client.send_command(client, command)
      assert response.executed == true
    end

    test "command with execution failure returns error", %{client: client} do
      channel = unique_channel()

      {:ok, _sub} =
        Client.subscribe_to_commands(client, channel,
          on_command: fn cmd_receive ->
            KubeMQ.CommandReply.new(
              request_id: cmd_receive.id,
              response_to: cmd_receive.reply_channel,
              executed: false,
              error: "operation failed"
            )
          end
        )

      Process.sleep(500)

      command = %KubeMQ.Command{
        channel: channel,
        body: "fail this",
        timeout: 10_000,
        client_id: "integration-commands-test"
      }

      {:ok, response} = Client.send_command(client, command)
      assert response.executed == false
      assert response.error == "operation failed"
    end

    test "command with tags round-trips metadata", %{client: client} do
      channel = unique_channel()

      {:ok, _sub} =
        Client.subscribe_to_commands(client, channel,
          on_command: fn cmd_receive ->
            # Echo the tags back
            KubeMQ.CommandReply.new(
              request_id: cmd_receive.id,
              response_to: cmd_receive.reply_channel,
              executed: true,
              tags: cmd_receive.tags
            )
          end
        )

      Process.sleep(500)

      command = %KubeMQ.Command{
        channel: channel,
        body: "tagged command",
        timeout: 10_000,
        client_id: "integration-commands-test",
        tags: %{"key" => "value"}
      }

      {:ok, response} = Client.send_command(client, command)
      assert response.executed == true
    end
  end
end
