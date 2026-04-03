defmodule KubeMQ.Config do
  @moduledoc """
  Configuration for KubeMQ client connections via NimbleOptions.

  All timeouts are in milliseconds, matching Elixir/Erlang convention.
  The SDK converts internally for proto fields that use different units.

  ## Unit Conversions

  | User-Facing Field       | User Unit | Proto Field            | Proto Unit | Conversion      |
  |-------------------------|-----------|------------------------|------------|-----------------|
  | `default_cache_ttl`     | ms        | `CacheTTL`             | seconds    | `div(ms, 1000)` |
  | `cache_ttl` (Query)     | ms        | `CacheTTL`             | seconds    | `div(ms, 1000)` |
  | `wait_timeout` (Simple) | ms        | `WaitTimeSeconds`      | seconds    | `div(ms, 1000)` |
  | `wait_timeout` (Stream) | ms        | `WaitTimeout`          | ms         | pass-through    |
  | `start_at_time_delta`   | ms        | `EventsStoreTypeValue` | seconds    | `div(ms, 1000)` |
  | `timeout` (CQ)          | ms        | `Timeout`              | ms         | pass-through    |
  """

  @schema NimbleOptions.new!(
            address: [
              type: :string,
              default: "localhost:50000",
              doc: "KubeMQ server address (host:port)"
            ],
            client_id: [
              type: :string,
              required: true,
              doc: "Client identifier"
            ],
            auth_token: [
              type: :string,
              doc: "Static JWT/OIDC token"
            ],
            credential_provider: [
              type: :atom,
              doc: "Module implementing KubeMQ.CredentialProvider"
            ],
            tls: [
              type: :keyword_list,
              doc: "TLS options: [cacertfile: path] for TLS, add certfile/keyfile for mTLS"
            ],
            connection_timeout: [
              type: :pos_integer,
              default: 10_000,
              doc: "Connection timeout in milliseconds"
            ],
            reconnect_policy: [
              type: :keyword_list,
              default: [
                enabled: true,
                initial_delay: 1_000,
                max_delay: 30_000,
                max_attempts: 0,
                multiplier: 2.0
              ],
              doc: "Reconnection policy options",
              keys: [
                enabled: [type: :boolean, default: true],
                initial_delay: [type: :pos_integer, default: 1_000],
                max_delay: [type: :pos_integer, default: 30_000],
                max_attempts: [type: :non_neg_integer, default: 0],
                multiplier: [type: :float, default: 2.0]
              ]
            ],
            retry_policy: [
              type: :keyword_list,
              default: [max_retries: 3, initial_delay: 100, max_delay: 1_000, multiplier: 2.0],
              doc:
                "Retry policy for transient errors. Note: max total block time is ~3s (3 retries x 1s max each).",
              keys: [
                max_retries: [type: :non_neg_integer, default: 3],
                initial_delay: [type: :pos_integer, default: 100],
                max_delay: [type: :pos_integer, default: 1_000],
                multiplier: [type: :float, default: 2.0]
              ]
            ],
            send_timeout: [
              type: :pos_integer,
              default: 5_000,
              doc: "Default timeout for send operations in milliseconds"
            ],
            rpc_timeout: [
              type: :pos_integer,
              default: 10_000,
              doc: "Default timeout for RPC operations in milliseconds"
            ],
            keepalive_time: [
              type: :pos_integer,
              default: 10_000,
              doc: "Keepalive interval in milliseconds (minimum 5000ms enforced)"
            ],
            keepalive_timeout: [
              type: :pos_integer,
              default: 5_000,
              doc: "Keepalive timeout in milliseconds"
            ],
            max_receive_size: [
              type: :pos_integer,
              default: 4_194_304,
              doc: "Max receive message size in bytes (default 4MB)"
            ],
            max_send_size: [
              type: :pos_integer,
              default: 104_857_600,
              doc: "Max send message size in bytes (default 100MB)"
            ],
            default_channel: [
              type: :string,
              doc: "Default channel for operations"
            ],
            default_cache_ttl: [
              type: :pos_integer,
              default: 900_000,
              doc: "Default cache TTL in ms (900_000ms = 900s, proto CacheTTL is int32 seconds)"
            ],
            receive_buffer_size: [
              type: :pos_integer,
              default: 10,
              doc: "Subscription receive buffer size"
            ],
            reconnect_buffer_size: [
              type: :pos_integer,
              default: 1_000,
              doc: "Max operations buffered during reconnection"
            ],
            on_connected: [
              type: {:fun, 0},
              doc: "Callback invoked when connected"
            ],
            on_disconnected: [
              type: {:fun, 0},
              doc: "Callback invoked when disconnected"
            ],
            on_reconnecting: [
              type: {:fun, 0},
              doc: "Callback invoked when reconnecting"
            ],
            on_reconnected: [
              type: {:fun, 0},
              doc: "Callback invoked when reconnected"
            ],
            on_closed: [
              type: {:fun, 0},
              doc: "Callback invoked when closed"
            ],
            name: [
              type: :any,
              doc: "OTP process name: atom, {:global, term}, or {:via, module, term}"
            ]
          )

  @spec schema() :: keyword()
  def schema, do: @schema

  @spec validate!(keyword()) :: keyword()
  def validate!(opts) do
    opts
    |> NimbleOptions.validate!(@schema)
    |> enforce_keepalive_minimum()
  end

  @spec validate(keyword()) :: {:ok, keyword()} | {:error, NimbleOptions.ValidationError.t()}
  def validate(opts) do
    case NimbleOptions.validate(opts, @schema) do
      {:ok, validated} -> {:ok, enforce_keepalive_minimum(validated)}
      error -> error
    end
  end

  defp enforce_keepalive_minimum(opts) do
    keepalive = Keyword.get(opts, :keepalive_time, 10_000)
    Keyword.put(opts, :keepalive_time, max(keepalive, 5_000))
  end
end
