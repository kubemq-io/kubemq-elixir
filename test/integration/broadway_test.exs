defmodule KubeMQ.Integration.BroadwayTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias KubeMQ.Client

  @broker_address "localhost:50000"

  defp unique_channel, do: "test-broadway-#{System.unique_integer([:positive])}"

  # A test Broadway pipeline module
  defmodule TestPipeline do
    use Broadway

    def start_link(opts) do
      channel = Keyword.fetch!(opts, :channel)
      client = Keyword.fetch!(opts, :client)
      test_pid = Keyword.fetch!(opts, :test_pid)

      Broadway.start_link(__MODULE__,
        name: :"test_pipeline_#{System.unique_integer([:positive])}",
        context: %{test_pid: test_pid},
        producer: [
          module:
            {KubeMQ.Broadway.Queues,
             [
               client: client,
               channel: channel,
               max_items: 10,
               wait_timeout: 2_000,
               auto_ack: false
             ]}
        ],
        processors: [
          default: [concurrency: 1]
        ]
      )
    end

    @impl true
    def handle_message(_processor, message, %{test_pid: test_pid}) do
      send(test_pid, {:broadway_message, message.data})
      message
    end
  end

  setup do
    {:ok, client} =
      Client.start_link(
        address: @broker_address,
        client_id: "integration-broadway-test"
      )

    on_exit(fn ->
      try do
        if Process.alive?(client), do: Client.close(client)
      catch
        :exit, _ -> :ok
      end
    end)

    %{client: client}
  end

  describe "broadway queue pipeline" do
    test "messages flow through broadway pipeline with ack", %{client: client} do
      channel = unique_channel()

      # Start the Broadway pipeline
      {:ok, pipeline} =
        TestPipeline.start_link(
          channel: channel,
          client: client,
          test_pid: self()
        )

      on_exit(fn ->
        try do
          if Process.alive?(pipeline), do: Broadway.stop(pipeline)
        catch
          :exit, _ -> :ok
        end
      end)

      # Send queue messages
      for i <- 1..3 do
        msg = %KubeMQ.QueueMessage{
          channel: channel,
          body: "broadway-msg-#{i}",
          client_id: "integration-broadway-test"
        }

        {:ok, _result} = Client.send_queue_message(client, msg)
      end

      # Verify messages flow through the pipeline
      for _i <- 1..3 do
        assert_receive {:broadway_message, _msg}, 10_000
      end
    end

    test "pipeline handles empty queue gracefully", %{client: client} do
      channel = unique_channel()

      {:ok, pipeline} =
        TestPipeline.start_link(
          channel: channel,
          client: client,
          test_pid: self()
        )

      on_exit(fn ->
        try do
          if Process.alive?(pipeline), do: Broadway.stop(pipeline)
        catch
          :exit, _ -> :ok
        end
      end)

      # With no messages, the pipeline should stay alive without errors
      Process.sleep(3_000)
      assert Process.alive?(pipeline)
    end
  end
end
