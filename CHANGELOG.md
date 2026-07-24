# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2026-07-24

### Security
- Update `protobuf` 0.16.0 → 0.17.0 — fixes CVE-2026-54451 (unbounded recursion when decoding embedded messages, DoS). Reachable on the inbound broker-message decode path.
- Update `mint` 1.7.1 → 1.9.3 — fixes CVE-2026-48862 and CVE-2026-49754 (HTTP/2 memory-exhaustion DoS via the gRPC transport). Pulled transitively through `grpc`; `hpax` also updated 1.0.3 → 1.0.4.

## [1.0.1] - 2026-05-31

### Improvements
- Update `cowlib` 2.16.0 → 2.16.1 (clears 4 advisories) and `cowboy` 2.14.2 → 2.15.0, plus jason/telemetry currency updates
- Add `run_status` endpoint and improved reporting for full dashboard compatibility; enhanced metrics/runtime introspection configuration

## [1.0.0] - 2026-04-04

### Added

- Initial release of the KubeMQ Elixir SDK
- Full support for all 5 KubeMQ messaging patterns:
  - Events (Pub/Sub) — fire-and-forget with optional consumer groups
  - Events Store (Persistent Pub/Sub) — replay from any position
  - Commands (RPC) — request-response with timeout
  - Queries (RPC with Cache) — request-response with server-side caching
  - Queues — reliable message queues (Stream + Simple APIs)
- `KubeMQ.Client` GenServer as the main API surface
- `KubeMQ.Connection` GenServer with automatic reconnection (exponential backoff + jitter)
- Connection state machine: `:connecting -> :ready -> :reconnecting -> :closed`
- Bounded operation buffer during reconnection
- `KubeMQ.Transport` behaviour for Mox-based unit testing
- `KubeMQ.Error` struct implementing `Exception` behaviour with 13 error codes
- Client-side validation (19 rules) before gRPC calls
- Channel management (create, delete, list, purge) for all 5 channel types
- NimbleOptions-validated configuration with 22 client options
- TLS and mTLS support via `GRPC.Credential`
- `KubeMQ.CredentialProvider` behaviour for pluggable authentication
- Broadway producers for Events, Events Store, and Queues patterns
- `:telemetry` integration with 18 event types
- OTP supervision tree integration via `child_spec/1`
- DynamicSupervisor for subscription lifecycle management
- Registry-based multi-client naming
- Bang variants (`!`) for all public API functions
- UUID auto-generation for all message IDs
- 59 runnable example scripts matching Go/C++ SDK structure 1:1
- Full ExDoc documentation with module groups
- GitHub Actions CI with Elixir 1.15-1.17 x OTP 26-27 matrix
- Credo strict, Dialyxir, and ExCoveralls integration
