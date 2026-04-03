defmodule KubeMQ.Error do
  @moduledoc """
  KubeMQ error type with error codes, retryability, and gRPC status mapping.

  All public SDK functions return `{:ok, result} | {:error, %KubeMQ.Error{}}`.
  Bang variants raise `KubeMQ.Error` on failure.

  ## Fields

    * `code` — Error code atom (see Error Code Reference below)
    * `message` — Human-readable error description
    * `operation` — SDK operation that triggered the error (e.g., `"send_event"`)
    * `channel` — Channel name involved, if applicable
    * `cause` — Underlying error term (e.g., gRPC error struct)
    * `retryable?` — Whether the operation can be retried
    * `request_id` — Correlation ID for tracing

  ## Error Code Reference

  | Code              | Retryable | Description                              | Common Triggers                        |
  |-------------------|:---------:|------------------------------------------|----------------------------------------|
  | `:transient`      | Yes       | Temporary network or server issue        | Network blip, broker restart           |
  | `:timeout`        | Yes       | Operation timed out                      | Slow responder, network congestion     |
  | `:throttling`     | Yes       | Rate limited by server                   | Exceeding server rate limits           |
  | `:authentication` | No        | Invalid credentials                      | Expired/invalid auth token             |
  | `:authorization`  | No        | Insufficient permissions                 | Missing ACL permissions                |
  | `:validation`     | No        | Invalid input                            | Empty channel, missing body/metadata   |
  | `:not_found`      | No        | Resource not found                       | Channel doesn't exist                  |
  | `:fatal`          | No        | Unrecoverable error                      | Internal server error, protocol error  |
  | `:cancellation`   | No        | Operation was cancelled                  | Context/process cancellation           |
  | `:client_closed`  | No        | Client has been closed                   | Calling after `KubeMQ.Client.close/1`  |
  | `:buffer_full`    | No        | Reconnect buffer overflow                | Too many ops during reconnection       |
  | `:stream_broken`  | No        | Stream connection lost                   | Broker disconnect during stream op     |
  | `:backpressure`   | No        | Reserved — not currently produced by any SDK function | —                                      |

  ## gRPC Status Code Mapping

  | gRPC Code | gRPC Name           | SDK Error Code     | Retryable |
  |-----------|---------------------|-------------------:|:---------:|
  | 0         | OK                  | (success)          | —         |
  | 1         | CANCELLED           | `:cancellation`    | No        |
  | 2         | UNKNOWN             | `:fatal`           | No        |
  | 3         | INVALID_ARGUMENT    | `:validation`      | No        |
  | 4         | DEADLINE_EXCEEDED   | `:timeout`         | Yes       |
  | 5         | NOT_FOUND           | `:not_found`       | No        |
  | 6         | ALREADY_EXISTS      | `:validation`      | No        |
  | 7         | PERMISSION_DENIED   | `:authorization`   | No        |
  | 8         | RESOURCE_EXHAUSTED  | `:throttling`      | Yes       |
  | 9         | FAILED_PRECONDITION | `:validation`      | No        |
  | 10        | ABORTED             | `:transient`       | Yes       |
  | 11        | OUT_OF_RANGE        | `:validation`      | No        |
  | 12        | UNIMPLEMENTED       | `:fatal`           | No        |
  | 13        | INTERNAL            | `:fatal`           | No        |
  | 14        | UNAVAILABLE         | `:transient`       | Yes       |
  | 15        | DATA_LOSS           | `:fatal`           | No        |
  | 16        | UNAUTHENTICATED     | `:authentication`  | No        |

  ## Pattern Matching

  Match on the error code atom for control flow:

      case KubeMQ.Client.send_event(client, event) do
        :ok -> :sent
        {:error, %KubeMQ.Error{code: :validation} = e} -> {:bad_input, e.message}
        {:error, %KubeMQ.Error{retryable?: true} = e} -> retry(e)
        {:error, %KubeMQ.Error{} = e} -> {:fatal, e}
      end

  With `with` expressions:

      with :ok <- KubeMQ.Client.send_event(client, event) do
        :ok
      else
        {:error, %KubeMQ.Error{code: :client_closed}} -> restart_client()
        {:error, %KubeMQ.Error{retryable?: true}} -> :retry
        {:error, error} -> raise error
      end

  ## Recovery Guidance

    * **Retryable errors** (`:transient`, `:timeout`, `:throttling`) — The SDK's built-in
      `KubeMQ.RetryPolicy` handles retries automatically. If you receive these after retries
      are exhausted, implement application-level backoff or circuit breaking.

    * **`:authentication` / `:authorization`** — Check token validity and permissions.
      These will not resolve with retries.

    * **`:validation`** — Fix the input (empty channel, missing required fields).

    * **`:client_closed`** — The client was explicitly closed via `KubeMQ.Client.close/1`.
      Create a new client instance.

    * **`:buffer_full`** — Reduce send rate during reconnection, or increase
      `:reconnect_buffer_size` in client config.

    * **`:stream_broken`** — The stream handle is no longer valid. Obtain a new one
      via the corresponding `KubeMQ.Client` function.
  """

  @type error_code ::
          :transient
          | :timeout
          | :throttling
          | :authentication
          | :authorization
          | :validation
          | :not_found
          | :fatal
          | :cancellation
          | :backpressure
          | :client_closed
          | :buffer_full
          | :stream_broken

  @type t :: %__MODULE__{
          code: error_code(),
          message: String.t(),
          operation: String.t() | nil,
          channel: String.t() | nil,
          cause: term() | nil,
          retryable?: boolean(),
          request_id: String.t() | nil
        }

  defexception [
    :code,
    :message,
    :operation,
    :channel,
    :cause,
    :request_id,
    retryable?: false
  ]

  @impl true
  def message(%__MODULE__{message: msg}), do: msg

  @doc """
  Create a transient (retryable) error for temporary network or server issues.

  ## Options

    * `:operation` — Operation name string
    * `:channel` — Channel name string
    * `:cause` — Underlying error term
    * `:request_id` — Correlation ID

  ## Examples

      iex> err = KubeMQ.Error.transient("connection lost")
      iex> err.code
      :transient
      iex> err.retryable?
      true
  """
  @spec transient(String.t(), keyword()) :: t()
  def transient(message, opts \\ []) do
    new(:transient, message, true, opts)
  end

  @doc """
  Create a timeout (retryable) error for operations that exceeded their deadline.

  ## Examples

      iex> err = KubeMQ.Error.timeout("operation timed out after 10s")
      iex> err.code
      :timeout
      iex> err.retryable?
      true
  """
  @spec timeout(String.t(), keyword()) :: t()
  def timeout(message, opts \\ []) do
    new(:timeout, message, true, opts)
  end

  @doc """
  Create a throttling (retryable) error when the server rate-limits the client.

  ## Examples

      iex> err = KubeMQ.Error.throttling("rate limit exceeded")
      iex> err.code
      :throttling
      iex> err.retryable?
      true
  """
  @spec throttling(String.t(), keyword()) :: t()
  def throttling(message, opts \\ []) do
    new(:throttling, message, true, opts)
  end

  @doc """
  Create a validation (non-retryable) error for invalid input.

  ## Examples

      iex> err = KubeMQ.Error.validation("channel cannot be empty")
      iex> err.code
      :validation
      iex> err.retryable?
      false
  """
  @spec validation(String.t(), keyword()) :: t()
  def validation(message, opts \\ []) do
    new(:validation, message, false, opts)
  end

  @doc """
  Create an authentication (non-retryable) error for invalid credentials.

  ## Examples

      iex> err = KubeMQ.Error.authentication("invalid token")
      iex> err.code
      :authentication
      iex> err.retryable?
      false
  """
  @spec authentication(String.t(), keyword()) :: t()
  def authentication(message, opts \\ []) do
    new(:authentication, message, false, opts)
  end

  @doc """
  Create an authorization (non-retryable) error for insufficient permissions.

  ## Examples

      iex> err = KubeMQ.Error.authorization("missing ACL")
      iex> err.code
      :authorization
      iex> err.retryable?
      false
  """
  @spec authorization(String.t(), keyword()) :: t()
  def authorization(message, opts \\ []) do
    new(:authorization, message, false, opts)
  end

  @doc """
  Create a not-found (non-retryable) error when a resource doesn't exist.

  ## Examples

      iex> err = KubeMQ.Error.not_found("channel does not exist")
      iex> err.code
      :not_found
      iex> err.retryable?
      false
  """
  @spec not_found(String.t(), keyword()) :: t()
  def not_found(message, opts \\ []) do
    new(:not_found, message, false, opts)
  end

  @doc """
  Create a fatal (non-retryable) error for unrecoverable failures.

  ## Examples

      iex> err = KubeMQ.Error.fatal("internal server error")
      iex> err.code
      :fatal
      iex> err.retryable?
      false
  """
  @spec fatal(String.t(), keyword()) :: t()
  def fatal(message, opts \\ []) do
    new(:fatal, message, false, opts)
  end

  @doc """
  Create a client-closed error. Produced when operations are attempted after
  `KubeMQ.Client.close/1` has been called.

  ## Examples

      iex> err = KubeMQ.Error.client_closed()
      iex> err.code
      :client_closed
      iex> err.message
      "client is closed"
  """
  @spec client_closed(keyword()) :: t()
  def client_closed(opts \\ []) do
    new(:client_closed, "client is closed", false, opts)
  end

  @doc """
  Create a buffer-full error. Produced when the reconnect operation buffer
  overflows during a reconnection cycle.

  ## Examples

      iex> err = KubeMQ.Error.buffer_full()
      iex> err.code
      :buffer_full
      iex> err.message
      "operation buffer full during reconnection"
  """
  @spec buffer_full(keyword()) :: t()
  def buffer_full(opts \\ []) do
    new(:buffer_full, "operation buffer full during reconnection", false, opts)
  end

  @doc """
  Create a stream-broken error. Produced when a stream handle's underlying
  gRPC stream is no longer connected.

  ## Examples

      iex> err = KubeMQ.Error.stream_broken("downstream process is not alive")
      iex> err.code
      :stream_broken
      iex> err.retryable?
      false
  """
  @spec stream_broken(String.t(), keyword()) :: t()
  def stream_broken(message, opts \\ []) do
    new(:stream_broken, message, false, opts)
  end

  @doc """
  Convert a gRPC status code or error to a `KubeMQ.Error`.

  Accepts a map with `:status` and `:message` keys, a bare integer status code,
  or an atom error code. Maps gRPC status codes to SDK error codes per the
  gRPC Status Code Mapping table above.

  ## Examples

      iex> err = KubeMQ.Error.from_grpc_status(%{status: 14, message: "unavailable"}, [])
      iex> err.code
      :transient

      iex> err = KubeMQ.Error.from_grpc_status(4, message: "deadline exceeded")
      iex> err.code
      :timeout
  """
  @spec from_grpc_status(map(), keyword()) :: t()
  def from_grpc_status(%{status: status, message: grpc_message}, opts) do
    {code, retryable} = classify_grpc_code(status)
    msg = if grpc_message in [nil, ""], do: "gRPC error (status #{status})", else: grpc_message
    new(code, msg, retryable, opts)
  end

  def from_grpc_status(status, opts) when is_integer(status) do
    {code, retryable} = classify_grpc_code(status)
    msg = Keyword.get(opts, :message, "gRPC error (status #{status})")
    new(code, msg, retryable, opts)
  end

  @atom_to_retryable %{
    transient: true,
    timeout: true,
    throttling: true,
    cancellation: false,
    fatal: false,
    validation: false,
    not_found: false,
    authorization: false,
    authentication: false
  }

  def from_grpc_status(code, opts) when is_atom(code) do
    retryable = Map.get(@atom_to_retryable, code, false)
    msg = Keyword.get(opts, :message, "gRPC error (#{code})")
    new(code, msg, retryable, opts)
  end

  defp classify_grpc_code(1), do: {:cancellation, false}
  defp classify_grpc_code(2), do: {:fatal, false}
  defp classify_grpc_code(3), do: {:validation, false}
  defp classify_grpc_code(4), do: {:timeout, true}
  defp classify_grpc_code(5), do: {:not_found, false}
  defp classify_grpc_code(6), do: {:validation, false}
  defp classify_grpc_code(7), do: {:authorization, false}
  defp classify_grpc_code(8), do: {:throttling, true}
  defp classify_grpc_code(9), do: {:validation, false}
  defp classify_grpc_code(10), do: {:transient, true}
  defp classify_grpc_code(11), do: {:validation, false}
  defp classify_grpc_code(12), do: {:fatal, false}
  defp classify_grpc_code(13), do: {:fatal, false}
  defp classify_grpc_code(14), do: {:transient, true}
  defp classify_grpc_code(15), do: {:fatal, false}
  defp classify_grpc_code(16), do: {:authentication, false}
  defp classify_grpc_code(_), do: {:fatal, false}

  defp new(code, message, retryable, opts) do
    %__MODULE__{
      code: code,
      message: message,
      retryable?: retryable,
      operation: Keyword.get(opts, :operation),
      channel: Keyword.get(opts, :channel),
      cause: Keyword.get(opts, :cause),
      request_id: Keyword.get(opts, :request_id)
    }
  end
end
