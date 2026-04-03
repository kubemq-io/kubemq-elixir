defmodule KubeMQ.TypesTest do
  use ExUnit.Case, async: true

  describe "KubeMQ.SubscribeType" do
    test "returns correct integer values for all 5 subscribe types" do
      assert KubeMQ.SubscribeType.undefined() == 0
      assert KubeMQ.SubscribeType.events() == 1
      assert KubeMQ.SubscribeType.events_store() == 2
      assert KubeMQ.SubscribeType.commands() == 3
      assert KubeMQ.SubscribeType.queries() == 4
    end
  end

  describe "KubeMQ.RequestType" do
    test "returns correct integer values for all 3 request types" do
      assert KubeMQ.RequestType.unknown() == 0
      assert KubeMQ.RequestType.command() == 1
      assert KubeMQ.RequestType.query() == 2
    end
  end

  describe "KubeMQ.DownstreamRequestType" do
    test "returns correct integer values for all 12 downstream request types" do
      assert KubeMQ.DownstreamRequestType.unknown() == 0
      assert KubeMQ.DownstreamRequestType.get() == 1
      assert KubeMQ.DownstreamRequestType.ack_all() == 2
      assert KubeMQ.DownstreamRequestType.ack_range() == 3
      assert KubeMQ.DownstreamRequestType.nack_all() == 4
      assert KubeMQ.DownstreamRequestType.nack_range() == 5
      assert KubeMQ.DownstreamRequestType.requeue_all() == 6
      assert KubeMQ.DownstreamRequestType.requeue_range() == 7
      assert KubeMQ.DownstreamRequestType.active_offsets() == 8
      assert KubeMQ.DownstreamRequestType.transaction_status() == 9
      assert KubeMQ.DownstreamRequestType.close_by_client() == 10
      assert KubeMQ.DownstreamRequestType.close_by_server() == 11
    end
  end

  describe "KubeMQ.EventsStoreType" do
    test "returns correct integer values for all 6 events store types" do
      assert KubeMQ.EventsStoreType.start_new_only() == 1
      assert KubeMQ.EventsStoreType.start_from_first() == 2
      assert KubeMQ.EventsStoreType.start_from_last() == 3
      assert KubeMQ.EventsStoreType.start_at_sequence() == 4
      assert KubeMQ.EventsStoreType.start_at_time() == 5
      assert KubeMQ.EventsStoreType.start_at_time_delta() == 6
    end
  end
end
