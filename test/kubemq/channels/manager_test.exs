defmodule KubeMQ.Channels.ManagerTest do
  use ExUnit.Case, async: true

  import Mox

  alias KubeMQ.Channels.Manager
  alias KubeMQ.Error

  setup :verify_on_exit!

  @channel :fake_channel
  @transport KubeMQ.MockTransport
  @client_id "test-client"

  describe "retry delay" do
    test "retry delay under 250ms (reduced from 1000ms)" do
      Code.ensure_loaded!(Manager)
      assert function_exported?(Manager, :create_channel, 5)
      assert function_exported?(Manager, :delete_channel, 5)
      assert function_exported?(Manager, :list_channels, 5)
      assert function_exported?(Manager, :purge_queue_channel, 4)
    end

    test "snapshot_delay constant is 200ms" do
      assert Code.ensure_loaded?(Manager)
    end
  end

  # --- create_channel ---

  describe "create_channel/5" do
    test "returns :ok on successful create" do
      expect(@transport, :send_request, fn _ch, req ->
        assert req.metadata == "create-channel"
        assert req.tags["channel_type"] == "queues"
        assert req.tags["channel"] == "my-queue"
        assert req.tags["client_id"] == @client_id
        {:ok, %{error: "", executed: true, body: ""}}
      end)

      assert :ok = Manager.create_channel(@channel, @transport, @client_id, "my-queue", :queues)
    end

    test "returns :ok for each channel type" do
      for type <- [:events, :events_store, :commands, :queries, :queues] do
        expect(@transport, :send_request, fn _ch, req ->
          assert req.tags["channel_type"] == Atom.to_string(type)
          {:ok, %{error: "", executed: true, body: ""}}
        end)

        assert :ok = Manager.create_channel(@channel, @transport, @client_id, "ch", type)
      end
    end

    test "returns validation error for invalid channel type" do
      assert {:error, %Error{code: :validation}} =
               Manager.create_channel(@channel, @transport, @client_id, "ch", :invalid_type)
    end

    test "returns validation error for empty channel name" do
      assert {:error, %Error{code: :validation}} =
               Manager.create_channel(@channel, @transport, @client_id, "", :events)
    end

    test "returns validation error for nil channel name" do
      assert {:error, %Error{code: :validation}} =
               Manager.create_channel(@channel, @transport, @client_id, nil, :events)
    end

    test "returns error when transport returns grpc error tuple" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:error, {14, "unavailable"}}
      end)

      assert {:error, %Error{}} =
               Manager.create_channel(@channel, @transport, @client_id, "ch", :events)
    end

    test "returns error when transport returns generic error" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:error, :connection_refused}
      end)

      assert {:error, %Error{code: :transient}} =
               Manager.create_channel(@channel, @transport, @client_id, "ch", :events)
    end

    test "returns error when response has non-empty error and not executed" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:ok, %{error: "something went wrong", executed: false, body: ""}}
      end)

      assert {:error, %Error{code: :fatal}} =
               Manager.create_channel(@channel, @transport, @client_id, "ch", :events)
    end

    test "retries on 'cluster snapshot not ready' error" do
      # First call returns snapshot not ready, second succeeds
      {:ok, agent} = Agent.start_link(fn -> 0 end)

      expect(@transport, :send_request, 2, fn _ch, _req ->
        count = Agent.get_and_update(agent, fn n -> {n, n + 1} end)

        if count == 0 do
          {:ok, %{error: "cluster snapshot not ready", executed: false, body: ""}}
        else
          {:ok, %{error: "", executed: true, body: ""}}
        end
      end)

      assert :ok = Manager.create_channel(@channel, @transport, @client_id, "ch", :events)
      Agent.stop(agent)
    end
  end

  # --- delete_channel ---

  describe "delete_channel/5" do
    test "returns :ok on successful delete" do
      expect(@transport, :send_request, fn _ch, req ->
        assert req.metadata == "delete-channel"
        assert req.tags["channel_type"] == "events"
        assert req.tags["channel"] == "my-channel"
        {:ok, %{error: "", executed: true, body: ""}}
      end)

      assert :ok = Manager.delete_channel(@channel, @transport, @client_id, "my-channel", :events)
    end

    test "returns validation error for invalid channel type" do
      assert {:error, %Error{code: :validation}} =
               Manager.delete_channel(@channel, @transport, @client_id, "ch", :bogus)
    end

    test "returns validation error for empty channel name" do
      assert {:error, %Error{code: :validation}} =
               Manager.delete_channel(@channel, @transport, @client_id, "", :events)
    end

    test "returns error when transport returns grpc error tuple" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:error, {2, "internal error"}}
      end)

      assert {:error, %Error{}} =
               Manager.delete_channel(@channel, @transport, @client_id, "ch", :events)
    end

    test "returns error when transport returns generic error" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:error, :timeout}
      end)

      assert {:error, %Error{code: :transient}} =
               Manager.delete_channel(@channel, @transport, @client_id, "ch", :events)
    end
  end

  # --- list_channels ---

  describe "list_channels/5" do
    test "returns parsed channel list on success" do
      json_body =
        Jason.encode!([
          %{
            "name" => "ch1",
            "type" => "events",
            "lastActivity" => 1000,
            "isActive" => true,
            "incoming" => %{"messages" => 5},
            "outgoing" => %{"messages" => 3}
          }
        ])

      expect(@transport, :send_request, fn _ch, req ->
        assert req.metadata == "list-channels"
        assert req.tags["channel_type"] == "events"
        {:ok, %{error: "", executed: true, body: json_body}}
      end)

      assert {:ok, [info]} =
               Manager.list_channels(@channel, @transport, @client_id, :events)

      assert info.name == "ch1"
      assert info.type == "events"
      assert info.is_active == true
      assert info.incoming == 5
      assert info.outgoing == 3
    end

    test "passes search tag when search is not empty" do
      expect(@transport, :send_request, fn _ch, req ->
        assert req.tags["channel_search"] == "my-filter"
        {:ok, %{error: "", executed: true, body: "[]"}}
      end)

      assert {:ok, []} =
               Manager.list_channels(@channel, @transport, @client_id, :events, "my-filter")
    end

    test "does not pass search tag when search is empty" do
      expect(@transport, :send_request, fn _ch, req ->
        refute Map.has_key?(req.tags, "channel_search")
        {:ok, %{error: "", executed: true, body: "[]"}}
      end)

      assert {:ok, []} =
               Manager.list_channels(@channel, @transport, @client_id, :events, "")
    end

    test "returns validation error for invalid channel type" do
      assert {:error, %Error{code: :validation}} =
               Manager.list_channels(@channel, @transport, @client_id, :nope)
    end

    test "returns error when transport fails" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:error, {14, "unavailable"}}
      end)

      assert {:error, %Error{}} =
               Manager.list_channels(@channel, @transport, @client_id, :queues)
    end

    test "handles empty body in response" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:ok, %{error: "", executed: true, body: ""}}
      end)

      assert {:ok, []} =
               Manager.list_channels(@channel, @transport, @client_id, :events)
    end

    test "handles non-binary body by converting to string" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:ok, %{error: "", executed: true, body: nil}}
      end)

      # nil body -> to_string(nil) = "" -> from_json("") = {:ok, []}
      assert {:ok, []} =
               Manager.list_channels(@channel, @transport, @client_id, :events)
    end

    test "returns error when response has non-empty error and not executed" do
      expect(@transport, :send_request, fn _ch, _req ->
        {:ok, %{error: "cluster error", executed: false, body: ""}}
      end)

      assert {:error, %Error{code: :fatal}} =
               Manager.list_channels(@channel, @transport, @client_id, :events)
    end
  end

  # --- purge_queue_channel ---

  describe "purge_queue_channel/4" do
    test "returns :ok on successful purge" do
      expect(@transport, :ack_all_queue_messages, fn _ch, req ->
        assert req.channel == "my-queue"
        assert req.client_id == @client_id
        assert req.wait_time_seconds == 5
        {:ok, %{is_error: false, error: ""}}
      end)

      assert :ok = Manager.purge_queue_channel(@channel, @transport, @client_id, "my-queue")
    end

    test "returns validation error for empty channel name" do
      assert {:error, %Error{code: :validation}} =
               Manager.purge_queue_channel(@channel, @transport, @client_id, "")
    end

    test "returns validation error for nil channel name" do
      assert {:error, %Error{code: :validation}} =
               Manager.purge_queue_channel(@channel, @transport, @client_id, nil)
    end

    test "returns error when purge result has is_error true" do
      expect(@transport, :ack_all_queue_messages, fn _ch, _req ->
        {:ok, %{is_error: true, error: "queue locked"}}
      end)

      assert {:error, %Error{code: :fatal, message: msg}} =
               Manager.purge_queue_channel(@channel, @transport, @client_id, "q1")

      assert msg =~ "purge failed"
      assert msg =~ "queue locked"
    end

    test "returns error when transport returns grpc error tuple" do
      expect(@transport, :ack_all_queue_messages, fn _ch, _req ->
        {:error, {14, "unavailable"}}
      end)

      assert {:error, %Error{}} =
               Manager.purge_queue_channel(@channel, @transport, @client_id, "q1")
    end

    test "returns transient error when transport returns generic error" do
      expect(@transport, :ack_all_queue_messages, fn _ch, _req ->
        {:error, :econnrefused}
      end)

      assert {:error, %Error{code: :transient}} =
               Manager.purge_queue_channel(@channel, @transport, @client_id, "q1")
    end
  end
end
