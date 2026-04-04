# KubeMQ Elixir SDK

[![Hex.pm](https://img.shields.io/hexpm/v/kubemq.svg)](https://hex.pm/packages/kubemq)
[![CI](https://github.com/kubemq/kubemq-elixir/actions/workflows/ci.yml/badge.svg)](https://github.com/kubemq/kubemq-elixir/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Features](#features)
  - [Events (Pub/Sub)](#events-pubsub)
  - [Events Store (Persistent Pub/Sub)](#events-store-persistent-pubsub)
  - [Commands (RPC)](#commands-rpc)
  - [Queries (RPC with Cache)](#queries-rpc-with-cache)
  - [Queues](#queues)
  - [Channel Management](#channel-management)
  - [Broadway Producers](#broadway-producers)
- [Configuration](#configuration)
- [Supervision](#supervision)
- [Error Handling](#error-handling)
- [TLS / mTLS](#tls--mtls)
- [Examples](#examples)
- [Documentation](#documentation)
- [Requirements](#requirements)
- [License](#license)

Elixir client SDK for [KubeMQ](https://kubemq.io) — a Kubernetes-native message queue broker.

## Installation

Add `kubemq` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kubemq, "~> 1.0"}
  ]
end
```

For Broadway integration, also add:

```elixir
{:broadway, "~> 1.0"}
```

## Quick Start

```elixir
# Start a client
{:ok, client} = KubeMQ.Client.start_link(
  address: "localhost:50000",
  client_id: "my-app"
)

# Send an event
:ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{
  channel: "notifications",
  body: "Hello KubeMQ!",
  metadata: "greeting"
})

# Subscribe to events
{:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "notifications",
  on_event: fn event ->
    IO.puts("Received: #{event.body}")
  end
)

# Clean up
KubeMQ.Subscription.cancel(sub)
KubeMQ.Client.close(client)
```

## Features

### Events (Pub/Sub)

Fire-and-forget messaging with optional consumer groups and wildcard subscriptions.

```elixir
# Publish
:ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{
  channel: "events.orders",
  body: Jason.encode!(%{order_id: 123}),
  tags: %{"source" => "api"}
})

# Stream publish (high throughput)
{:ok, stream} = KubeMQ.Client.send_event_stream(client)
:ok = KubeMQ.EventStreamHandle.send(stream, %KubeMQ.Event{
  channel: "events.orders",
  body: "batch-item"
})
KubeMQ.EventStreamHandle.close(stream)

# Subscribe with consumer group
{:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "events.>",
  group: "order-processors",
  on_event: fn event -> process_event(event) end,
  on_error: fn error -> Logger.error("Sub error: #{error.message}") end
)
```

### Events Store (Persistent Pub/Sub)

Persistent events with replay from any position.

```elixir
# Publish (returns confirmation)
{:ok, result} = KubeMQ.Client.send_event_store(client, %KubeMQ.EventStore{
  channel: "audit.logs",
  body: Jason.encode!(%{action: "login", user: "alice"})
})

# Subscribe from the beginning
{:ok, sub} = KubeMQ.Client.subscribe_to_events_store(client, "audit.logs",
  start_at: :start_from_first,
  on_event: fn event ->
    IO.puts("Seq #{event.sequence}: #{event.body}")
  end
)

# Replay from a specific sequence
{:ok, sub} = KubeMQ.Client.subscribe_to_events_store(client, "audit.logs",
  start_at: {:start_at_sequence, 100}
)

# Replay events from the last 5 minutes
{:ok, sub} = KubeMQ.Client.subscribe_to_events_store(client, "audit.logs",
  start_at: {:start_at_time_delta, 300_000}
)
```

### Commands (RPC)

Request-response with timeout.

```elixir
# Send a command
{:ok, response} = KubeMQ.Client.send_command(client, %KubeMQ.Command{
  channel: "commands.users",
  body: Jason.encode!(%{action: "create", name: "Bob"}),
  timeout: 10_000
})

if response.executed do
  IO.puts("Command executed at #{response.executed_at}")
end

# Handle commands
{:ok, sub} = KubeMQ.Client.subscribe_to_commands(client, "commands.users",
  on_command: fn cmd ->
    %KubeMQ.CommandReply{
      request_id: cmd.id,
      response_to: cmd.reply_channel,
      executed: true
    }
  end
)
```

### Queries (RPC with Cache)

Request-response with optional server-side caching.

```elixir
# Send a query with caching
{:ok, response} = KubeMQ.Client.send_query(client, %KubeMQ.Query{
  channel: "queries.products",
  body: Jason.encode!(%{id: 42}),
  timeout: 10_000,
  cache_key: "product:42",
  cache_ttl: 60_000
})

IO.puts("Cache hit: #{response.cache_hit}")

# Handle queries
{:ok, sub} = KubeMQ.Client.subscribe_to_queries(client, "queries.products",
  on_query: fn query ->
    product = load_product(query.body)
    %KubeMQ.QueryReply{
      request_id: query.id,
      response_to: query.reply_channel,
      executed: true,
      body: Jason.encode!(product)
    }
  end
)
```

### Queues

Reliable message queues with at-least-once delivery.

```elixir
# Send a message
{:ok, result} = KubeMQ.Client.send_queue_message(client, %KubeMQ.QueueMessage{
  channel: "queue.tasks",
  body: "process-this"
})

# Send with delivery policy
{:ok, result} = KubeMQ.Client.send_queue_message(client, %KubeMQ.QueueMessage{
  channel: "queue.tasks",
  body: "delayed-task",
  policy: %KubeMQ.QueuePolicy{
    delay_seconds: 30,
    expiration_seconds: 3600,
    max_receive_count: 3,
    max_receive_queue: "queue.dlq"
  }
})

# Poll and acknowledge (Stream API)
{:ok, poll} = KubeMQ.Client.poll_queue(client,
  channel: "queue.tasks",
  max_items: 10,
  wait_timeout: 5_000
)

Enum.each(poll.messages, &process_message/1)
:ok = KubeMQ.PollResponse.ack_all(poll)

# Simple receive (Simple API)
{:ok, result} = KubeMQ.Client.receive_queue_messages(client, "queue.tasks",
  max_messages: 5,
  wait_timeout: 3_000
)
```

### Channel Management

```elixir
# Create channels
:ok = KubeMQ.Client.create_events_channel(client, "notifications")
:ok = KubeMQ.Client.create_queues_channel(client, "tasks")

# List channels
{:ok, channels} = KubeMQ.Client.list_queues_channels(client, "")
Enum.each(channels, fn ch ->
  IO.puts("#{ch.name} - active: #{ch.is_active}")
end)

# Delete and purge
:ok = KubeMQ.Client.purge_queue_channel(client, "tasks")
:ok = KubeMQ.Client.delete_queues_channel(client, "tasks")
```

### Broadway Producers

Process KubeMQ messages using Broadway data pipelines.

```elixir
defmodule MyEventPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {KubeMQ.Broadway.Events, [
          client: MyApp.KubeMQ,
          channel: "events.>",
          group: "broadway-consumer"
        ]}
      ],
      processors: [default: [concurrency: 4]],
      batchers: [default: [batch_size: 100, batch_timeout: 1_000]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    event = message.data
    IO.puts("Processing event on #{event.channel}")
    message
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    IO.puts("Batch of #{length(messages)} processed")
    messages
  end
end
```

Available producers: `KubeMQ.Broadway.Events`, `KubeMQ.Broadway.EventsStore`, `KubeMQ.Broadway.Queues`.

## Configuration

All options are passed to `KubeMQ.Client.start_link/1`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `address` | `String.t()` | `"localhost:50000"` | KubeMQ server address (host:port) |
| `client_id` | `String.t()` | *required* | Unique client identifier |
| `auth_token` | `String.t()` | `nil` | Static JWT/OIDC auth token |
| `credential_provider` | `module()` | `nil` | Module implementing `KubeMQ.CredentialProvider` |
| `tls` | `keyword()` | `nil` | TLS/mTLS options (`cacertfile`, `certfile`, `keyfile`) |
| `connection_timeout` | `pos_integer()` | `10_000` | Connection timeout (ms) |
| `keepalive_time` | `pos_integer()` | `10_000` | Keepalive interval (ms, min 5000) |
| `keepalive_timeout` | `pos_integer()` | `5_000` | Keepalive timeout (ms) |
| `max_receive_size` | `pos_integer()` | `4_194_304` | Max receive message size (bytes) |
| `max_send_size` | `pos_integer()` | `104_857_600` | Max send message size (bytes) |
| `default_channel` | `String.t()` | `nil` | Default channel for operations |
| `default_cache_ttl` | `pos_integer()` | `900_000` | Default query cache TTL (ms) |
| `receive_buffer_size` | `pos_integer()` | `10` | Subscription receive buffer size |
| `reconnect_buffer_size` | `pos_integer()` | `1_000` | Max buffered ops during reconnect |
| `reconnect_policy` | `keyword()` | see below | Reconnection policy |
| `retry_policy` | `keyword()` | see below | Retry policy for transient errors |
| `name` | `atom()` / `{:via, ...}` | `nil` | OTP process name |
| `on_connected` | `(-> :ok)` | `nil` | Connection established callback |
| `on_disconnected` | `(-> :ok)` | `nil` | Connection lost callback |
| `on_reconnecting` | `(-> :ok)` | `nil` | Reconnection attempt callback |
| `on_reconnected` | `(-> :ok)` | `nil` | Reconnection success callback |
| `on_closed` | `(-> :ok)` | `nil` | Connection closed callback |

### Reconnect Policy Defaults

```elixir
reconnect_policy: [
  enabled: true,
  initial_delay: 1_000,
  max_delay: 30_000,
  max_attempts: 0,      # 0 = unlimited
  multiplier: 2.0
]
```

### Retry Policy Defaults

```elixir
retry_policy: [
  max_retries: 3,
  initial_delay: 100,
  max_delay: 5_000,
  multiplier: 2.0
]
```

## Supervision

Add the client to your application supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {KubeMQ.Client,
        address: "localhost:50000",
        client_id: "my-app",
        name: MyApp.KubeMQ}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

## Error Handling

All public functions return `{:ok, result} | {:error, %KubeMQ.Error{}}`. Bang variants (suffixed with `!`) raise `KubeMQ.Error` on failure.

```elixir
case KubeMQ.Client.send_event(client, event) do
  :ok -> :ok
  {:error, %KubeMQ.Error{code: :validation} = err} ->
    Logger.warning("Validation: #{err.message}")
  {:error, %KubeMQ.Error{code: :transient, retryable?: true} = err} ->
    Logger.error("Transient error (will retry): #{err.message}")
  {:error, %KubeMQ.Error{} = err} ->
    Logger.error("#{err.code}: #{err.message}")
end

# Or use bang variants for scripts
KubeMQ.Client.send_event!(client, event)
```

### Error Codes

| Code | Retryable | Description |
|------|:---------:|-------------|
| `:transient` | Yes | Temporary network/server issue |
| `:timeout` | Yes | Operation timed out |
| `:throttling` | Yes | Rate limited |
| `:authentication` | No | Invalid credentials |
| `:authorization` | No | Insufficient permissions |
| `:validation` | No | Invalid input |
| `:not_found` | No | Resource not found |
| `:fatal` | No | Unrecoverable error |
| `:client_closed` | No | Client has been closed |
| `:buffer_full` | No | Reconnect buffer overflow |
| `:stream_broken` | No | Stream connection lost |

## TLS / mTLS

```elixir
# TLS (server verification only)
{:ok, client} = KubeMQ.Client.start_link(
  address: "broker:50000",
  client_id: "secure-app",
  tls: [cacertfile: "/path/to/ca.pem"]
)

# mTLS (mutual TLS)
{:ok, client} = KubeMQ.Client.start_link(
  address: "broker:50000",
  client_id: "mtls-app",
  tls: [
    cacertfile: "/path/to/ca.pem",
    certfile: "/path/to/client.pem",
    keyfile: "/path/to/client-key.pem",
    verify: :verify_peer
  ]
)
```

## Examples

See the [`examples/`](examples/) directory for 59 runnable `.exs` scripts covering all messaging patterns. Run any example with:

```bash
cd examples/events
elixir basic_pubsub.exs
```

## Documentation

Full API documentation is available on [HexDocs](https://hexdocs.pm/kubemq).

Additional guides:

- [Concepts](CONCEPTS.md) — Messaging pattern overview
- [Troubleshooting](TROUBLESHOOTING.md) — Common issues and solutions
- [Compatibility](COMPATIBILITY.md) — Version support matrix
- [Contributing](CONTRIBUTING.md) — Development setup and PR process
- [Changelog](CHANGELOG.md) — Version history

## Requirements

- Elixir 1.15+
- OTP 26+
- KubeMQ Server v2.0.0+

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.
