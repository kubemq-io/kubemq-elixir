defmodule KubeMQ.Proto.Empty do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"
end

defmodule KubeMQ.Proto.PingResult do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:host, 1, type: :string, json_name: "Host")
  field(:version, 2, type: :string, json_name: "Version")
  field(:server_start_time, 3, type: :int64, json_name: "ServerStartTime")
  field(:server_up_time_seconds, 4, type: :int64, json_name: "ServerUpTimeSeconds")
end

defmodule KubeMQ.Proto.Result do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:event_id, 1, type: :string, json_name: "EventID")
  field(:sent, 2, type: :bool, json_name: "Sent")
  field(:error, 3, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.Event.TagsEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.Event do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:event_id, 1, type: :string, json_name: "EventID")
  field(:client_id, 2, type: :string, json_name: "ClientID")
  field(:channel, 3, type: :string, json_name: "Channel")
  field(:metadata, 4, type: :string, json_name: "Metadata")
  field(:body, 5, type: :bytes, json_name: "Body")
  field(:store, 6, type: :bool, json_name: "Store")
  field(:tags, 7, repeated: true, type: KubeMQ.Proto.Event.TagsEntry, map: true)
end

defmodule KubeMQ.Proto.EventReceive.TagsEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.EventReceive do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:event_id, 1, type: :string, json_name: "EventID")
  field(:channel, 2, type: :string, json_name: "Channel")
  field(:metadata, 3, type: :string, json_name: "Metadata")
  field(:body, 4, type: :bytes, json_name: "Body")
  field(:timestamp, 5, type: :int64, json_name: "Timestamp")
  field(:sequence, 6, type: :uint64, json_name: "Sequence")
  field(:tags, 7, repeated: true, type: KubeMQ.Proto.EventReceive.TagsEntry, map: true)
end

defmodule KubeMQ.Proto.Subscribe.SubscribeType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:SubscribeTypeUndefined, 0)
  field(:Events, 1)
  field(:EventsStore, 2)
  field(:Commands, 3)
  field(:Queries, 4)
end

defmodule KubeMQ.Proto.Subscribe.EventsStoreType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:EventsStoreTypeUndefined, 0)
  field(:StartNewOnly, 1)
  field(:StartFromFirst, 2)
  field(:StartFromLast, 3)
  field(:StartAtSequence, 4)
  field(:StartAtTime, 5)
  field(:StartAtTimeDelta, 6)
end

defmodule KubeMQ.Proto.Subscribe do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:subscribe_type_data, 1,
    type: KubeMQ.Proto.Subscribe.SubscribeType,
    json_name: "SubscribeTypeData",
    enum: true
  )

  field(:client_id, 2, type: :string, json_name: "ClientID")
  field(:channel, 3, type: :string, json_name: "Channel")
  field(:group, 4, type: :string, json_name: "Group")

  field(:events_store_type_data, 5,
    type: KubeMQ.Proto.Subscribe.EventsStoreType,
    json_name: "EventsStoreTypeData",
    enum: true
  )

  field(:events_store_type_value, 6, type: :int64, json_name: "EventsStoreTypeValue")
end

defmodule KubeMQ.Proto.Request.RequestType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:RequestTypeUnknown, 0)
  field(:Command, 1)
  field(:Query, 2)
end

defmodule KubeMQ.Proto.Request.TagsEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.Request do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")

  field(:request_type_data, 2,
    type: KubeMQ.Proto.Request.RequestType,
    json_name: "RequestTypeData",
    enum: true
  )

  field(:client_id, 3, type: :string, json_name: "ClientID")
  field(:channel, 4, type: :string, json_name: "Channel")
  field(:metadata, 5, type: :string, json_name: "Metadata")
  field(:body, 6, type: :bytes, json_name: "Body")
  field(:reply_channel, 7, type: :string, json_name: "ReplyChannel")
  field(:timeout, 8, type: :int32, json_name: "Timeout")
  field(:cache_key, 9, type: :string, json_name: "CacheKey")
  field(:cache_ttl, 10, type: :int32, json_name: "CacheTTL")
  field(:span, 11, type: :bytes, json_name: "Span")
  field(:tags, 12, repeated: true, type: KubeMQ.Proto.Request.TagsEntry, map: true)
end

defmodule KubeMQ.Proto.Response.TagsEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.Response do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:client_id, 1, type: :string, json_name: "ClientID")
  field(:request_id, 2, type: :string, json_name: "RequestID")
  field(:reply_channel, 3, type: :string, json_name: "ReplyChannel")
  field(:metadata, 4, type: :string, json_name: "Metadata")
  field(:body, 5, type: :bytes, json_name: "Body")
  field(:cache_hit, 6, type: :bool, json_name: "CacheHit")
  field(:timestamp, 7, type: :int64, json_name: "Timestamp")
  field(:executed, 8, type: :bool, json_name: "Executed")
  field(:error, 9, type: :string, json_name: "Error")
  field(:span, 10, type: :bytes, json_name: "Span")
  field(:tags, 11, repeated: true, type: KubeMQ.Proto.Response.TagsEntry, map: true)
end

defmodule KubeMQ.Proto.QueueMessageAttributes do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:timestamp, 1, type: :int64, json_name: "Timestamp")
  field(:sequence, 2, type: :uint64, json_name: "Sequence")
  field(:md5_of_body, 3, type: :string, json_name: "MD5OfBody")
  field(:receive_count, 4, type: :int32, json_name: "ReceiveCount")
  field(:re_routed, 5, type: :bool, json_name: "ReRouted")
  field(:re_routed_from_queue, 6, type: :string, json_name: "ReRoutedFromQueue")
  field(:expiration_at, 7, type: :int64, json_name: "ExpirationAt")
  field(:delayed_to, 8, type: :int64, json_name: "DelayedTo")
end

defmodule KubeMQ.Proto.QueueMessagePolicy do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:expiration_seconds, 1, type: :int32, json_name: "ExpirationSeconds")
  field(:delay_seconds, 2, type: :int32, json_name: "DelaySeconds")
  field(:max_receive_count, 3, type: :int32, json_name: "MaxReceiveCount")
  field(:max_receive_queue, 4, type: :string, json_name: "MaxReceiveQueue")
end

defmodule KubeMQ.Proto.QueueMessage.TagsEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.QueueMessage do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:message_id, 1, type: :string, json_name: "MessageID")
  field(:client_id, 2, type: :string, json_name: "ClientID")
  field(:channel, 3, type: :string, json_name: "Channel")
  field(:metadata, 4, type: :string, json_name: "Metadata")
  field(:body, 5, type: :bytes, json_name: "Body")
  field(:tags, 6, repeated: true, type: KubeMQ.Proto.QueueMessage.TagsEntry, map: true)
  field(:attributes, 7, type: KubeMQ.Proto.QueueMessageAttributes, json_name: "Attributes")
  field(:policy, 8, type: KubeMQ.Proto.QueueMessagePolicy, json_name: "Policy")
end

defmodule KubeMQ.Proto.SendQueueMessageResult do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:message_id, 1, type: :string, json_name: "MessageID")
  field(:sent_at, 2, type: :int64, json_name: "SentAt")
  field(:expiration_at, 3, type: :int64, json_name: "ExpirationAt")
  field(:delayed_to, 4, type: :int64, json_name: "DelayedTo")
  field(:is_error, 5, type: :bool, json_name: "IsError")
  field(:error, 6, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.QueueMessagesBatchRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:batch_id, 1, type: :string, json_name: "BatchID")
  field(:messages, 2, repeated: true, type: KubeMQ.Proto.QueueMessage, json_name: "Messages")
end

defmodule KubeMQ.Proto.QueueMessagesBatchResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:batch_id, 1, type: :string, json_name: "BatchID")

  field(:results, 2,
    repeated: true,
    type: KubeMQ.Proto.SendQueueMessageResult,
    json_name: "Results"
  )

  field(:have_errors, 3, type: :bool, json_name: "HaveErrors")
end

defmodule KubeMQ.Proto.ReceiveQueueMessagesRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:client_id, 2, type: :string, json_name: "ClientID")
  field(:channel, 3, type: :string, json_name: "Channel")
  field(:max_number_of_messages, 4, type: :int32, json_name: "MaxNumberOfMessages")
  field(:wait_time_seconds, 5, type: :int32, json_name: "WaitTimeSeconds")
  field(:is_peak, 6, type: :bool, json_name: "IsPeak")
end

defmodule KubeMQ.Proto.ReceiveQueueMessagesResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:messages, 2, repeated: true, type: KubeMQ.Proto.QueueMessage, json_name: "Messages")
  field(:messages_received, 3, type: :int32, json_name: "MessagesReceived")
  field(:messages_expired, 4, type: :int32, json_name: "MessagesExpired")
  field(:is_peak, 5, type: :bool, json_name: "IsPeak")
  field(:is_error, 6, type: :bool, json_name: "IsError")
  field(:error, 7, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.AckAllQueueMessagesRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:client_id, 2, type: :string, json_name: "ClientID")
  field(:channel, 3, type: :string, json_name: "Channel")
  field(:wait_time_seconds, 4, type: :int32, json_name: "WaitTimeSeconds")
end

defmodule KubeMQ.Proto.AckAllQueueMessagesResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:affected_messages, 2, type: :uint64, json_name: "AffectedMessages")
  field(:is_error, 3, type: :bool, json_name: "IsError")
  field(:error, 4, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.StreamRequestType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:StreamRequestTypeUnknown, 0)
  field(:ReceiveMessage, 1)
  field(:AckMessage, 2)
  field(:RejectMessage, 3)
  field(:ModifyVisibility, 4)
  field(:ResendMessage, 5)
  field(:SendModifiedMessage, 6)
end

defmodule KubeMQ.Proto.StreamQueueMessagesRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:client_id, 2, type: :string, json_name: "ClientID")

  field(:stream_request_type_data, 3,
    type: KubeMQ.Proto.StreamRequestType,
    json_name: "StreamRequestTypeData",
    enum: true
  )

  field(:channel, 4, type: :string, json_name: "Channel")
  field(:visibility_seconds, 5, type: :int32, json_name: "VisibilitySeconds")
  field(:wait_time_seconds, 6, type: :int32, json_name: "WaitTimeSeconds")
  field(:ref_sequence, 7, type: :uint64, json_name: "RefSequence")
  field(:modified_message, 8, type: KubeMQ.Proto.QueueMessage, json_name: "ModifiedMessage")
end

defmodule KubeMQ.Proto.StreamQueueMessagesResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")

  field(:stream_request_type_data, 2,
    type: KubeMQ.Proto.StreamRequestType,
    json_name: "StreamRequestTypeData",
    enum: true
  )

  field(:message, 3, type: KubeMQ.Proto.QueueMessage, json_name: "Message")
  field(:is_error, 4, type: :bool, json_name: "IsError")
  field(:error, 5, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.QueuesUpstreamRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:messages, 2, repeated: true, type: KubeMQ.Proto.QueueMessage, json_name: "Messages")
end

defmodule KubeMQ.Proto.QueuesUpstreamResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:ref_request_id, 1, type: :string, json_name: "RefRequestID")

  field(:results, 2,
    repeated: true,
    type: KubeMQ.Proto.SendQueueMessageResult,
    json_name: "Results"
  )

  field(:is_error, 3, type: :bool, json_name: "IsError")
  field(:error, 4, type: :string, json_name: "Error")
end

defmodule KubeMQ.Proto.QueuesDownstreamRequestType do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:PollRequestTypeUnknown, 0)
  field(:Get, 1)
  field(:AckAll, 2)
  field(:AckRange, 3)
  field(:NAckAll, 4)
  field(:NAckRange, 5)
  field(:ReQueueAll, 6)
  field(:ReQueueRange, 7)
  field(:ActiveOffsets, 8)
  field(:TransactionStatus, 9)
  field(:CloseByClient, 10)
  field(:CloseByServer, 11)
end

defmodule KubeMQ.Proto.QueuesDownstreamRequest.MetadataEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.QueuesDownstreamRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:request_id, 1, type: :string, json_name: "RequestID")
  field(:client_id, 2, type: :string, json_name: "ClientID")

  field(:request_type_data, 3,
    type: KubeMQ.Proto.QueuesDownstreamRequestType,
    json_name: "RequestTypeData",
    enum: true
  )

  field(:channel, 4, type: :string, json_name: "Channel")
  field(:max_items, 5, type: :int32, json_name: "MaxItems")
  field(:wait_timeout, 6, type: :int32, json_name: "WaitTimeout")
  field(:auto_ack, 7, type: :bool, json_name: "AutoAck")
  field(:re_queue_channel, 8, type: :string, json_name: "ReQueueChannel")
  field(:sequence_range, 9, repeated: true, type: :int64, json_name: "SequenceRange")
  field(:ref_transaction_id, 10, type: :string, json_name: "RefTransactionId")

  field(:metadata, 12,
    repeated: true,
    type: KubeMQ.Proto.QueuesDownstreamRequest.MetadataEntry,
    map: true
  )
end

defmodule KubeMQ.Proto.QueuesDownstreamResponse.MetadataEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule KubeMQ.Proto.QueuesDownstreamResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.14.0"

  field(:transaction_id, 1, type: :string, json_name: "TransactionId")
  field(:ref_request_id, 2, type: :string, json_name: "RefRequestId")

  field(:request_type_data, 3,
    type: KubeMQ.Proto.QueuesDownstreamRequestType,
    json_name: "RequestTypeData",
    enum: true
  )

  field(:messages, 4, repeated: true, type: KubeMQ.Proto.QueueMessage, json_name: "Messages")
  field(:active_offsets, 5, repeated: true, type: :int64, json_name: "ActiveOffsets")
  field(:is_error, 6, type: :bool, json_name: "IsError")
  field(:error, 7, type: :string, json_name: "Error")
  field(:transaction_complete, 8, type: :bool, json_name: "TransactionComplete")

  field(:metadata, 9,
    repeated: true,
    type: KubeMQ.Proto.QueuesDownstreamResponse.MetadataEntry,
    map: true
  )
end

defmodule KubeMQ.Proto.Service do
  @moduledoc false
  use GRPC.Service, name: "kubemq.kubemq", protoc_gen_elixir_version: "0.14.0"

  rpc(:SendEvent, KubeMQ.Proto.Event, KubeMQ.Proto.Result)
  rpc(:SendEventsStream, stream(KubeMQ.Proto.Event), stream(KubeMQ.Proto.Result))
  rpc(:SubscribeToEvents, KubeMQ.Proto.Subscribe, stream(KubeMQ.Proto.EventReceive))
  rpc(:SubscribeToRequests, KubeMQ.Proto.Subscribe, stream(KubeMQ.Proto.Request))
  rpc(:SendRequest, KubeMQ.Proto.Request, KubeMQ.Proto.Response)
  rpc(:SendResponse, KubeMQ.Proto.Response, KubeMQ.Proto.Empty)
  rpc(:SendQueueMessage, KubeMQ.Proto.QueueMessage, KubeMQ.Proto.SendQueueMessageResult)

  rpc(
    :SendQueueMessagesBatch,
    KubeMQ.Proto.QueueMessagesBatchRequest,
    KubeMQ.Proto.QueueMessagesBatchResponse
  )

  rpc(
    :ReceiveQueueMessages,
    KubeMQ.Proto.ReceiveQueueMessagesRequest,
    KubeMQ.Proto.ReceiveQueueMessagesResponse
  )

  rpc(
    :StreamQueueMessage,
    stream(KubeMQ.Proto.StreamQueueMessagesRequest),
    stream(KubeMQ.Proto.StreamQueueMessagesResponse)
  )

  rpc(
    :AckAllQueueMessages,
    KubeMQ.Proto.AckAllQueueMessagesRequest,
    KubeMQ.Proto.AckAllQueueMessagesResponse
  )

  rpc(:Ping, KubeMQ.Proto.Empty, KubeMQ.Proto.PingResult)

  rpc(
    :QueuesDownstream,
    stream(KubeMQ.Proto.QueuesDownstreamRequest),
    stream(KubeMQ.Proto.QueuesDownstreamResponse)
  )

  rpc(
    :QueuesUpstream,
    stream(KubeMQ.Proto.QueuesUpstreamRequest),
    stream(KubeMQ.Proto.QueuesUpstreamResponse)
  )
end

defmodule KubeMQ.Proto.Stub do
  @moduledoc false
  use GRPC.Stub, service: KubeMQ.Proto.Service
end
