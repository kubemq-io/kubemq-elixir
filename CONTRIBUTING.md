# Contributing to KubeMQ Elixir SDK

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

### Prerequisites

- Elixir 1.15+ (1.17 recommended)
- OTP 26+ (27 recommended)
- KubeMQ broker running on `localhost:50000` (for integration tests)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/kubemq/kubemq-elixir.git
cd kubemq-elixir

# Install dependencies
mix deps.get

# Compile
mix compile

# Run unit tests (no broker required)
mix test

# Run all tests (requires live broker)
mix test.all
```

### Using the Makefile

```bash
make deps          # Install dependencies
make compile       # Compile the project
make test          # Run unit tests
make test.all      # Run all tests (unit + integration)
make lint          # Format check + Credo strict + Dialyzer
make format        # Auto-format code
make dialyzer      # Run Dialyzer type checker
make docs          # Generate documentation
make coverage      # Run tests with coverage report
make clean         # Remove build artifacts
```

## Code Quality

Before submitting a PR, ensure all checks pass:

```bash
# Format code
mix format

# Lint (format check + Credo + Dialyzer)
mix lint

# Run tests with coverage
mix coveralls
```

### Quality Gates

- **Format**: `mix format --check-formatted` must pass with zero diffs
- **Credo**: `mix credo --strict` must pass with zero warnings
- **Dialyzer**: `mix dialyzer` must pass with zero warnings
- **Coverage**: `mix coveralls` must report >= 95% line coverage
- **Tests**: All unit tests must pass

## Pull Request Process

1. Fork the repository and create a feature branch from `main`
2. Write tests for any new functionality
3. Ensure all quality gates pass
4. Update documentation if the public API changes
5. Add an entry to `CHANGELOG.md` under `## [Unreleased]`
6. Submit a PR with a clear description of the changes

### PR Checklist

- [ ] Tests added/updated
- [ ] `mix format` applied
- [ ] `mix credo --strict` passes
- [ ] `mix dialyzer` passes
- [ ] Documentation updated (if public API changed)
- [ ] CHANGELOG.md updated
- [ ] Coverage remains >= 95%

## Coding Standards

### Elixir Conventions

- Functions use `snake_case`
- Modules use `CamelCase`
- Predicates end with `?` (e.g., `connected?/1`)
- Bang variants end with `!` (e.g., `send_event!/2`)
- All public functions have `@spec` annotations
- All public structs have `@type t()`

### Error Handling

- Public functions return `{:ok, result} | {:error, %KubeMQ.Error{}}`
- Never raise exceptions for expected errors (network, timeout, validation)
- Use `with` blocks for multi-step error propagation

### Testing

- Unit tests use Mox via the `KubeMQ.Transport` behaviour
- Integration tests are tagged `@tag :integration`
- Unit tests can run `async: true`
- Integration tests must run `async: false`

## Project Structure

```
lib/
  kubemq.ex                 # Top-level module
  kubemq/
    client.ex               # Client GenServer (main API)
    connection.ex            # gRPC connection management
    error.ex                 # Error types
    config.ex                # NimbleOptions configuration
    validation.ex            # Client-side validation
    transport/               # Transport behaviour + gRPC impl
    events/                  # Events pattern modules
    events_store/            # Events Store pattern modules
    commands/                # Commands pattern modules
    queries/                 # Queries pattern modules
    queues/                  # Queues pattern modules
    channels/                # Channel management
    broadway/                # Broadway producers
    auth/                    # Authentication
    tls/                     # TLS configuration
    retry/                   # Retry/reconnect policies
test/
  kubemq/                   # Unit tests (Mox-based)
  integration/              # Integration tests (live broker)
  support/                  # Test helpers and mocks
examples/                   # Runnable .exs example scripts
```

## Reporting Issues

Use [GitHub Issues](https://github.com/kubemq/kubemq-elixir/issues) to report bugs or request features. Include:

- Elixir/OTP version (`elixir --version`)
- KubeMQ server version
- Minimal reproduction case
- Expected vs actual behavior

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
