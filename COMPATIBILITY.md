# Compatibility

## Elixir / OTP Version Matrix

| Elixir | OTP 26 | OTP 27 |
|--------|:------:|:------:|
| 1.15   | Yes    | Yes    |
| 1.16   | Yes    | Yes    |
| 1.17   | Yes    | Yes    |

**Recommended**: Elixir 1.17 + OTP 27

**Minimum**: Elixir 1.15 + OTP 26

All combinations above are tested in CI via GitHub Actions matrix builds.

## KubeMQ Server Compatibility

| KubeMQ Server | SDK v0.1.x |
|---------------|:----------:|
| v2.0.0+       | Yes        |

The SDK uses proto version v1.4.0 and covers all 13 non-deprecated gRPC RPCs.

## gRPC Library

| Library     | Version    | Status     |
|-------------|------------|------------|
| `grpc`      | `~> 0.11` | Pre-1.0, actively maintained |
| `protobuf`  | `~> 0.14` | Stable, core team maintained |

The SDK pins to `grpc ~> 0.11`. The `grpc` library is pre-1.0 — version upgrades should be tested carefully. The CI matrix ensures compatibility across supported Elixir/OTP versions.

## HTTP/2 Adapters

| Adapter | Status  | mTLS | Notes |
|---------|---------|:----:|-------|
| Gun     | Default | Yes  | Process affinity handled by SDK |
| Mint    | Alternative | Limited | Known mTLS issues in some configs |

## Runtime Dependencies

| Package          | Version    | License     |
|------------------|------------|-------------|
| `grpc`           | `~> 0.11` | Apache-2.0  |
| `protobuf`       | `~> 0.14` | MIT         |
| `nimble_options`  | `~> 1.1`  | Apache-2.0  |
| `telemetry`      | `~> 1.0`  | Apache-2.0  |
| `jason`          | `~> 1.4`  | Apache-2.0  |

## Optional Dependencies

| Package   | Version   | License    | Purpose |
|-----------|-----------|------------|---------|
| `broadway` | `~> 1.0` | Apache-2.0 | Broadway producers for Events, Events Store, Queues |

## Operating System Support

| OS    | Architecture | Status |
|-------|:------------:|:------:|
| Linux | amd64        | Yes    |
| Linux | arm64        | Yes    |
| macOS | amd64        | Yes    |
| macOS | arm64        | Yes    |

## Feature Parity

The Elixir SDK has 1:1 feature parity with the Go v2.0.0 SDK across all 5 messaging patterns, including:

- 177 compliance checklist items (176 base + 1 Elixir-specific)
- 153 feature parity items
- 59 example scripts matching Go/C++ structure
