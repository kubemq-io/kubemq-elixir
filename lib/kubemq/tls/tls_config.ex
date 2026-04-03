defmodule KubeMQ.TLS do
  @moduledoc """
  TLS configuration for KubeMQ gRPC connections.

  Maps keyword options to `GRPC.Credential` SSL options.

  ## TLS (server authentication only)

      {:ok, client} = KubeMQ.Client.start_link(
        address: "kubemq.example.com:50000",
        client_id: "my-app",
        tls: [cacertfile: "/path/to/ca.pem"]
      )

  ## mTLS (mutual authentication)

      {:ok, client} = KubeMQ.Client.start_link(
        address: "kubemq.example.com:50000",
        client_id: "my-app",
        tls: [
          cacertfile: "/path/to/ca.pem",
          certfile: "/path/to/client.pem",
          keyfile: "/path/to/client-key.pem"
        ]
      )
  """

  @type tls_opts :: [
          cacertfile: String.t(),
          certfile: String.t(),
          keyfile: String.t(),
          verify: :verify_peer | :verify_none
        ]

  @doc """
  Build SSL options from the TLS keyword list for `GRPC.Credential.new/1`.

  Returns a keyword list suitable for `:ssl.connect/3`.
  """
  @spec to_ssl_opts(keyword(), String.t() | nil) :: keyword()
  def to_ssl_opts(tls_opts, hostname \\ nil) when is_list(tls_opts) do
    opts = []

    opts =
      case Keyword.get(tls_opts, :cacertfile) do
        nil ->
          # Default to system CA store (OTP 25+)
          case :public_key.cacerts_get() do
            [] -> opts
            certs -> Keyword.put(opts, :cacerts, certs)
          end

        path ->
          Keyword.put(opts, :cacertfile, to_charlist(path))
      end

    opts =
      case Keyword.get(tls_opts, :certfile) do
        nil -> opts
        path -> Keyword.put(opts, :certfile, to_charlist(path))
      end

    opts =
      case Keyword.get(tls_opts, :keyfile) do
        nil -> opts
        path -> Keyword.put(opts, :keyfile, to_charlist(path))
      end

    # Default to verify_peer (secure default)
    verify = Keyword.get(tls_opts, :verify, default_verify(opts))
    opts = Keyword.put(opts, :verify, verify)

    # Add SNI and hostname verification when hostname is available
    opts =
      if hostname do
        host_charlist = String.to_charlist(hostname)

        opts
        |> Keyword.put(:server_name_indication, host_charlist)
        |> Keyword.put(:customize_hostname_check,
          match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
        )
      else
        opts
      end

    opts
  end

  @doc """
  Build a `GRPC.Credential` from TLS keyword options.

  Returns `nil` if `tls_opts` is `nil` (plaintext connection).
  """
  # Explicit 1-arity overload to avoid default-argument conflict with nil/2 clause
  @spec to_credential(keyword()) :: GRPC.Credential.t() | nil
  def to_credential(tls_opts) when is_list(tls_opts), do: to_credential(tls_opts, nil)

  @spec to_credential(keyword() | nil, String.t() | nil) :: GRPC.Credential.t() | nil
  def to_credential(nil, _hostname), do: nil

  def to_credential(tls_opts, hostname) when is_list(tls_opts) do
    ssl_opts = to_ssl_opts(tls_opts, hostname)
    GRPC.Credential.new(ssl: ssl_opts)
  end

  @doc """
  Returns `true` if the TLS config includes client certificates (mTLS).
  """
  @spec mtls?(keyword() | nil) :: boolean()
  def mtls?(nil), do: false

  def mtls?(tls_opts) when is_list(tls_opts) do
    Keyword.has_key?(tls_opts, :certfile) and Keyword.has_key?(tls_opts, :keyfile)
  end

  defp default_verify(opts) do
    # Secure default: verify_peer if we have any CA source
    if Keyword.has_key?(opts, :cacertfile) or Keyword.has_key?(opts, :cacerts) do
      :verify_peer
    else
      :verify_none
    end
  end
end
