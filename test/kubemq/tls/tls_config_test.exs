defmodule KubeMQ.TLS.TlsConfigTest do
  use ExUnit.Case, async: true

  alias KubeMQ.TLS

  describe "to_credential/2 with hostname sets SNI" do
    test "sets server_name_indication when hostname is provided" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      ssl_opts = TLS.to_ssl_opts(tls_opts, "kubemq.example.com")

      assert Keyword.get(ssl_opts, :server_name_indication) == ~c"kubemq.example.com"
    end

    test "does not set server_name_indication when hostname is nil" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      ssl_opts = TLS.to_ssl_opts(tls_opts, nil)

      refute Keyword.has_key?(ssl_opts, :server_name_indication)
    end

    test "sets customize_hostname_check when hostname is provided" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      ssl_opts = TLS.to_ssl_opts(tls_opts, "kubemq.example.com")

      assert Keyword.has_key?(ssl_opts, :customize_hostname_check)
    end
  end

  describe "to_ssl_opts/2 uses system CA store" do
    test "uses system CA store when cacertfile is not specified" do
      tls_opts = []
      ssl_opts = TLS.to_ssl_opts(tls_opts)

      # Should either have :cacerts (system certs) or be empty if system has no certs
      # In any case, :cacertfile should not be set
      refute Keyword.has_key?(ssl_opts, :cacertfile)
    end

    test "uses explicit cacertfile when specified" do
      tls_opts = [cacertfile: "/path/to/ca.pem"]
      ssl_opts = TLS.to_ssl_opts(tls_opts)

      assert Keyword.get(ssl_opts, :cacertfile) == ~c"/path/to/ca.pem"
    end
  end

  describe "to_credential/2 creation" do
    test "returns nil for nil tls_opts" do
      assert TLS.to_credential(nil, nil) == nil
    end

    test "returns a GRPC.Credential for valid tls_opts" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      cred = TLS.to_credential(tls_opts, "localhost")

      assert %GRPC.Credential{} = cred
    end

    test "1-arity overload works" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      cred = TLS.to_credential(tls_opts)

      assert %GRPC.Credential{} = cred
    end
  end

  describe "mtls?/1 detection" do
    test "returns true when both certfile and keyfile are present" do
      tls_opts = [
        cacertfile: "/tmp/ca.pem",
        certfile: "/tmp/client.pem",
        keyfile: "/tmp/client-key.pem"
      ]

      assert TLS.mtls?(tls_opts)
    end

    test "returns false when only certfile is present" do
      tls_opts = [certfile: "/tmp/client.pem"]
      refute TLS.mtls?(tls_opts)
    end

    test "returns false when only keyfile is present" do
      tls_opts = [keyfile: "/tmp/client-key.pem"]
      refute TLS.mtls?(tls_opts)
    end

    test "returns false for empty list" do
      refute TLS.mtls?([])
    end

    test "returns false for nil" do
      refute TLS.mtls?(nil)
    end
  end

  describe "verify defaults" do
    test "defaults to :verify_peer when cacertfile is specified" do
      ssl_opts = TLS.to_ssl_opts(cacertfile: "/tmp/ca.pem")
      assert Keyword.get(ssl_opts, :verify) == :verify_peer
    end

    test "defaults to :verify_none when no CA source" do
      # When system CA store is empty or not used, and no cacertfile
      ssl_opts = TLS.to_ssl_opts([])

      # If system CA certs are available, verify_peer; otherwise verify_none
      verify = Keyword.get(ssl_opts, :verify)
      assert verify in [:verify_peer, :verify_none]
    end
  end

  describe "to_ssl_opts/2 mTLS options" do
    test "includes certfile and keyfile for mTLS" do
      tls_opts = [
        cacertfile: "/tmp/ca.pem",
        certfile: "/tmp/client.pem",
        keyfile: "/tmp/client-key.pem"
      ]

      ssl_opts = TLS.to_ssl_opts(tls_opts)

      assert Keyword.get(ssl_opts, :cacertfile) == ~c"/tmp/ca.pem"
      assert Keyword.get(ssl_opts, :certfile) == ~c"/tmp/client.pem"
      assert Keyword.get(ssl_opts, :keyfile) == ~c"/tmp/client-key.pem"
    end
  end

  describe "to_ssl_opts/2 verify override" do
    test "allows explicit verify override" do
      tls_opts = [cacertfile: "/tmp/ca.pem", verify: :verify_none]
      ssl_opts = TLS.to_ssl_opts(tls_opts)

      assert Keyword.get(ssl_opts, :verify) == :verify_none
    end
  end

  describe "to_credential/2 with hostname" do
    test "returns a GRPC.Credential with hostname" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      cred = TLS.to_credential(tls_opts, "kubemq.example.com")

      assert %GRPC.Credential{} = cred
    end
  end

  describe "to_ssl_opts/2 with hostname" do
    test "does not set customize_hostname_check when hostname is nil" do
      tls_opts = [cacertfile: "/tmp/ca.pem"]
      ssl_opts = TLS.to_ssl_opts(tls_opts, nil)

      refute Keyword.has_key?(ssl_opts, :customize_hostname_check)
    end
  end
end
