# Messaging Concepts

This guide explains the messaging patterns supported by KubeMQ and how they map to the Elixir SDK.

## Overview

KubeMQ is a Kubernetes-native message queue broker that supports 5 messaging patterns:

| Pattern | Type | Delivery | Persistence | Use Case |
|---------|------|----------|:-----------:|----------|
| Events | Pub/Sub | At-most-once | No | Real-time notifications, log streaming |
| Events Store | Pub/Sub | At-least-once | Yes | Audit logs, event sourcing, replay |
| Commands | RPC | Exactly-once | No | Service-to-service commands |
| Queries | RPC | Exactly-once | Cached | Data retrieval with optional caching |
| Queues | Queue | At-least-once | Yes | Task distribution, work queues |

## Events (Pub/Sub)

Events implement the publish-subscribe pattern. Publishers send messages to a channel, and all active subscribers on that channel receive a copy.

**Key characteristics**:
- Fire-and-forget — no delivery confirmation
- Messages are NOT persisted — subscribers only receive messages sent while they are connected
- Supports wildcard subscriptions (e.g., `events.>` matches all `events.*` channels)
- Consumer groups allow load balancing across subscribers

```elixir
# Publisher
:ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{
  channel: "events.orders.created",
  body: payload
})

# Subscriber
{:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "events.orders.>",
  group: "processors",
  on_event: fn event -> handle_event(event) end
)
```

## Events Store (Persistent Pub/Sub)

Events Store extends the Events pattern with message persistence and replay capabilities. Every message is stored and assigned a monotonically increasing sequence number.

**Key characteristics**:
- Messages are persisted to disk
- Subscribers can replay from any position
- Sequence tracking for reliable resume after disconnection
- Consumer groups with shared cursor

### Start Positions

| Position | Description |
|----------|-------------|
| `:start_new_only` | Only receive messages published after subscribing |
| `:start_from_first` | Replay from the very first stored message |
| `:start_from_last` | Start from the most recent message |
| `{:start_at_sequence, n}` | Replay from sequence number `n` |
| `{:start_at_time, nanos}` | Replay from a specific Unix timestamp (nanoseconds) |
| `{:start_at_time_delta, ms}` | Replay from `ms` milliseconds ago |

```elixir
# Replay all events from the beginning
{:ok, sub} = KubeMQ.Client.subscribe_to_events_store(client, "audit.logs",
  start_at: :start_from_first,
  on_event: fn event ->
    IO.puts("Seq #{event.sequence}: #{event.body}")
  end
)

# Resume from where we left off
{:ok, sub} = KubeMQ.Client.subscribe_to_events_store(client, "audit.logs",
  start_at: {:start_at_sequence, last_processed_seq + 1}
)
```

## Commands (RPC)

Commands implement the request-response pattern for executing operations on remote services. The caller blocks until the command is executed or times out.

**Key characteristics**:
- Synchronous request-response
- Mandatory timeout (prevents indefinite blocking)
- The subscriber's callback returns a `CommandReply` that is automatically sent back
- No caching — every command is executed

```elixir
# Sender
{:ok, response} = KubeMQ.Client.send_command(client, %KubeMQ.Command{
  channel: "commands.users.create",
  body: Jason.encode!(%{name: "Alice"}),
  timeout: 10_000
})
# response.executed => true/false

# Responder
{:ok, sub} = KubeMQ.Client.subscribe_to_commands(client, "commands.users.create",
  on_command: fn cmd ->
    user = create_user(cmd.body)
    %KubeMQ.CommandReply{
      request_id: cmd.id,
      response_to: cmd.reply_channel,
      executed: true
    }
  end
)
```

## Queries (RPC with Cache)

Queries extend Commands with optional server-side result caching. The first call executes the query; subsequent calls with the same cache key return the cached result until the TTL expires.

**Key characteristics**:
- Synchronous request-response (like Commands)
- Optional `cache_key` + `cache_ttl` for server-side caching
- `response.cache_hit` indicates whether the result came from cache
- Subscriber callback returns a `QueryReply` with response data

```elixir
# Sender with caching
{:ok, response} = KubeMQ.Client.send_query(client, %KubeMQ.Query{
  channel: "queries.products",
  body: Jason.encode!(%{id: 42}),
  timeout: 10_000,
  cache_key: "product:42",
  cache_ttl: 60_000  # Cache for 1 minute
})
# response.cache_hit => true if served from cache

# Responder
{:ok, sub} = KubeMQ.Client.subscribe_to_queries(client, "queries.products",
  on_query: fn query ->
    product = find_product(query.body)
    %KubeMQ.QueryReply{
      request_id: query.id,
      response_to: query.reply_channel,
      executed: true,
      body: Jason.encode!(product)
    }
  end
)
```

## Queues

Queues provide reliable message delivery with at-least-once semantics. Messages are persisted and delivered to exactly one consumer. Two APIs are available:

### Stream API (Primary)

The Stream API uses bidirectional gRPC streams for high-throughput scenarios.

**Sending** via `queue_upstream/1`:

```elixir
{:ok, upstream} = KubeMQ.Client.queue_upstream(client)
{:ok, results} = KubeMQ.QueueUpstreamHandle.send(upstream, [
  %KubeMQ.QueueMessage{channel: "tasks", body: "job-1"},
  %KubeMQ.QueueMessage{channel: "tasks", body: "job-2"}
])
```

**Receiving** via `poll_queue/2`:

```elixir
{:ok, poll} = KubeMQ.Client.poll_queue(client,
  channel: "tasks",
  max_items: 10,
  wait_timeout: 5_000
)

# Process messages, then acknowledge
:ok = KubeMQ.PollResponse.ack_all(poll)

# Or selectively acknowledge
:ok = KubeMQ.PollResponse.ack_range(poll, [1, 2, 3])
:ok = KubeMQ.PollResponse.nack_range(poll, [4, 5])
:ok = KubeMQ.PollResponse.requeue_range(poll, [6], "other-queue")
```

### Simple API (Secondary)

The Simple API uses unary gRPC calls — simpler but lower throughput.

```elixir
# Send
{:ok, result} = KubeMQ.Client.send_queue_message(client, %KubeMQ.QueueMessage{
  channel: "tasks",
  body: "simple-job"
})

# Receive
{:ok, result} = KubeMQ.Client.receive_queue_messages(client, "tasks",
  max_messages: 5,
  wait_timeout: 3_000
)
```

### Delivery Policies

Queue messages support delivery policies via `KubeMQ.QueuePolicy`:

| Policy | Description |
|--------|-------------|
| `expiration_seconds` | Message expires after N seconds (0 = no expiry) |
| `delay_seconds` | Delay delivery by N seconds |
| `max_receive_count` | Max delivery attempts before dead-letter |
| `max_receive_queue` | Dead-letter queue name |

```elixir
msg = %KubeMQ.QueueMessage{
  channel: "tasks",
  body: payload,
  policy: %KubeMQ.QueuePolicy{
    delay_seconds: 60,
    expiration_seconds: 3600,
    max_receive_count: 3,
    max_receive_queue: "tasks.dlq"
  }
}
```

## Channel Management

Channels are created automatically when you first publish or subscribe. You can also manage them explicitly:

```elixir
# Create
:ok = KubeMQ.Client.create_events_channel(client, "notifications")

# List
{:ok, channels} = KubeMQ.Client.list_events_channels(client, "notif")

# Delete
:ok = KubeMQ.Client.delete_events_channel(client, "notifications")

# Purge all messages from a queue
:ok = KubeMQ.Client.purge_queue_channel(client, "tasks")
```

## Consumer Groups

All subscription-based patterns (Events, Events Store, Commands, Queries) support consumer groups. When multiple subscribers join the same group on the same channel, each message is delivered to only one member of the group — enabling load balancing.

```elixir
# Three instances share the "workers" group — each message goes to one instance
{:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "tasks",
  group: "workers",
  on_event: fn event -> process(event) end
)
```

## Choosing a Pattern

| Need | Pattern |
|------|---------|
| Real-time notifications, no persistence needed | Events |
| Event sourcing, audit trail, replay | Events Store |
| Execute a remote operation, need confirmation | Commands |
| Query remote data, cacheable results | Queries |
| Reliable task processing, at-least-once | Queues |
