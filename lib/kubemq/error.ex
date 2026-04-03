defmodule KubeMQ.Error do
  @moduledoc """
  KubeMQ error type with error codes, retryability, and gRPC status mapping.

  All public SDK functions return `{:ok, result} | {:error, %KubeMQ.Error{}}`.
  Bang variants raise `KubeMQ.Error` on failure.
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

  @spec transient(String.t(), keyword()) :: t()
  def transient(message, opts \\ []) do
    new(:transient, message, true, opts)
  end

  @spec timeout(String.t(), keyword()) :: t()
  def timeout(message, opts \\ []) do
    new(:timeout, message, true, opts)
  end

  @spec throttling(String.t(), keyword()) :: t()
  def throttling(message, opts \\ []) do
    new(:throttling, message, true, opts)
  end

  @spec validation(String.t(), keyword()) :: t()
  def validation(message, opts \\ []) do
    new(:validation, message, false, opts)
  end

  @spec authentication(String.t(), keyword()) :: t()
  def authentication(message, opts \\ []) do
    new(:authentication, message, false, opts)
  end

  @spec authorization(String.t(), keyword()) :: t()
  def authorization(message, opts \\ []) do
    new(:authorization, message, false, opts)
  end

  @spec not_found(String.t(), keyword()) :: t()
  def not_found(message, opts \\ []) do
    new(:not_found, message, false, opts)
  end

  @spec fatal(String.t(), keyword()) :: t()
  def fatal(message, opts \\ []) do
    new(:fatal, message, false, opts)
  end

  @spec client_closed(keyword()) :: t()
  def client_closed(opts \\ []) do
    new(:client_closed, "client is closed", false, opts)
  end

  @spec buffer_full(keyword()) :: t()
  def buffer_full(opts \\ []) do
    new(:buffer_full, "operation buffer full during reconnection", false, opts)
  end

  @spec stream_broken(String.t(), keyword()) :: t()
  def stream_broken(message, opts \\ []) do
    new(:stream_broken, message, false, opts)
  end

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
