# Troubleshooting

Common issues and solutions for the KubeMQ Elixir SDK.

## Connection Issues

### Connection Refused

**Symptom**: `{:error, %KubeMQ.Error{code: :transient, message: "...connection refused..."}}`

**Causes & Solutions**:

1. **Broker not running**: Verify the KubeMQ broker is running:
   ```bash
   curl http://localhost:8080/health
   ```

2. **Wrong address**: Check the `address` option matches the broker's gRPC port (default 50000):
   ```elixir
   KubeMQ.Client.start_link(address: "localhost:50000", client_id: "app")
   ```

3. **Firewall/network**: Ensure the gRPC port is accessible from your Elixir application.

### Connection Timeout

**Symptom**: Client hangs or returns timeout error during `start_link`.

**Solutions**:

- Increase `connection_timeout`:
  ```elixir
  KubeMQ.Client.start_link(
    address: "remote-broker:50000",
    client_id: "app",
    connection_timeout: 30_000
  )
  ```

- Check DNS resolution for the broker hostname.

### Frequent Reconnections

**Symptom**: Repeated `on_disconnected` / `on_reconnecting` callbacks.

**Solutions**:

- Tune keepalive settings:
  ```elixir
  KubeMQ.Client.start_link(
    address: "broker:50000",
    client_id: "app",
    keepalive_time: 15_000,
    keepalive_timeout: 10_000
  )
  ```

- Check broker logs for keepalive-related disconnections.
- The minimum `keepalive_time` is 5000ms (enforced by the SDK).

## TLS Errors

### Certificate Verification Failed

**Symptom**: `{:error, %KubeMQ.Error{code: :transient, message: "...certificate verify failed..."}}`

**Solutions**:

1. Verify the CA certificate path is correct:
   ```elixir
   tls: [cacertfile: "/absolute/path/to/ca.pem"]
   ```

2. Ensure the CA certificate matches the broker's server certificate.

3. For development/testing, you can use system CA certificates (OTP 26+):
   ```elixir
   tls: [cacerts: :public_key.cacerts_get()]
   ```

### mTLS Handshake Failure

**Symptom**: Connection fails with mTLS configuration.

**Solutions**:

1. Verify all three files exist and are readable:
   ```elixir
   tls: [
     cacertfile: "/path/to/ca.pem",
     certfile: "/path/to/client.pem",
     keyfile: "/path/to/client-key.pem",
     verify: :verify_peer
   ]
   ```

2. Ensure the client certificate is signed by a CA trusted by the broker.

3. Check file permissions — the BEAM process must be able to read the key file.

## Timeout Tuning

### Command/Query Timeouts

**Symptom**: Commands or queries return `{:error, %KubeMQ.Error{code: :timeout}}`.

**Solutions**:

- Increase the operation timeout:
  ```elixir
  KubeMQ.Client.send_command(client, %KubeMQ.Command{
    channel: "commands.slow",
    body: payload,
    timeout: 30_000   # 30 seconds
  })
  ```

- Ensure a subscriber is connected and processing commands on the target channel.

### Queue Poll Timeouts

**Symptom**: `poll_queue` returns empty or times out.

**Solutions**:

- Adjust `wait_timeout` to allow time for messages to arrive:
  ```elixir
  KubeMQ.Client.poll_queue(client,
    channel: "queue.tasks",
    max_items: 10,
    wait_timeout: 30_000
  )
  ```

- Verify messages are being sent to the correct channel name.

## Gun vs Mint HTTP/2 Adapter

The SDK defaults to the Gun HTTP/2 adapter. In some cases you may want to switch to Mint.

### Gun Process Affinity

**Issue**: Gun creates a process per connection. All gRPC calls must be routed through the same process that created the connection.

**Resolution**: The SDK handles this automatically — all gRPC operations are routed through the `KubeMQ.Connection` GenServer, which owns the Gun process.

### Mint Known Limitations

**Issue**: Mint has known issues with mTLS certificate handling in some configurations.

**Resolution**: Use Gun (default) if you need mTLS. Mint works for TLS-only and plain connections.

To use Mint:

```elixir
# In your mix.exs, add:
{:mint, "~> 1.6"}

# Then configure:
KubeMQ.Client.start_link(
  address: "broker:50000",
  client_id: "app",
  # Mint adapter is configured at the grpc library level
)
```

## Buffer Full Errors

**Symptom**: `{:error, %KubeMQ.Error{code: :buffer_full}}` during reconnection.

**Cause**: Too many operations queued while the client is reconnecting.

**Solutions**:

- Increase the buffer size:
  ```elixir
  KubeMQ.Client.start_link(
    address: "broker:50000",
    client_id: "app",
    reconnect_buffer_size: 5_000
  )
  ```

- Implement backpressure in your application — check `KubeMQ.Client.connected?/1` before sending.
- Handle `:buffer_full` errors by retrying after a delay.

## Subscription Issues

### Subscription Not Receiving Messages

**Causes**:

1. **Consumer group conflict**: Only one member per group receives each message. Check group names.
2. **Channel mismatch**: Ensure publisher and subscriber use the exact same channel name.
3. **Events Store start position**: Verify the `start_at` option is correct — `:start_new_only` ignores historical messages.

### Double Processing

**Cause**: Multiple subscription processes running for the same channel/group.

**Solution**: Use `KubeMQ.Subscription.active?/1` to check before creating new subscriptions, or use the supervision tree to manage subscription lifecycle.

## Debug Logging

### Enabling SDK Debug Logs

The SDK uses Elixir's standard `Logger` for internal logging. To see SDK activity:

```elixir
# In config/config.exs or config/dev.exs
config :logger, level: :debug
```

### Log Levels Used by the SDK

| Level     | What is logged                                        |
|-----------|-------------------------------------------------------|
| `:debug`  | gRPC channel creation, subscription setup, proto mapping |
| `:info`   | Client start/stop, connection state transitions       |
| `:warning`| Retry attempts, keepalive mismatches, reconnection    |
| `:error`  | Connection failures, stream errors, unrecoverable errors |

### Filtering SDK Logs

To see only KubeMQ SDK logs, use module-based filtering:

```elixir
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:module]

# Then filter in your log backend for modules starting with KubeMQ
```

### Telemetry Events

The SDK emits `:telemetry` events for every client operation. Attach handlers
for operational monitoring:

```elixir
:telemetry.attach_many("kubemq-logger", [
  [:kubemq, :client, :send_event, :start],
  [:kubemq, :client, :send_event, :stop],
  [:kubemq, :client, :send_event, :exception]
], &handle_event/4, nil)
```

All client-level events follow the pattern `[:kubemq, :client, <operation>, <phase>]` where:
- `<operation>` is the function atom (e.g., `:send_event`, `:poll_queue`, `:send_command`)
- `<phase>` is `:start`, `:stop`, or `:exception`

Measurements include `:system_time` (start) and `:duration` (stop/exception).
Metadata includes `:operation`, `:channel`, and `:client_id`.

The internal connection layer also emits `[:kubemq, :connection, :connect, <phase>]` events
for connection lifecycle operations. These are lower-level and primarily useful for
debugging connection issues. Metadata includes `:address` and `:client_id`.

## Getting Help

If this guide doesn't resolve your issue:

1. Check the [examples](examples/) for working code
2. Review the [API documentation](https://hexdocs.pm/kubemq)
3. Open an issue on [GitHub](https://github.com/kubemq/kubemq-elixir/issues)
