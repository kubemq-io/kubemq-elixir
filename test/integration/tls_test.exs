defmodule KubeMQ.Integration.TLSTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :tls

  alias KubeMQ.Client

  @tls_broker_address "localhost:50000"

  # Placeholder cert paths -- only work in a TLS-configured environment
  @ca_cert_path "/etc/kubemq/certs/ca.pem"
  @client_cert_path "/etc/kubemq/certs/client.pem"
  @client_key_path "/etc/kubemq/certs/client-key.pem"
  @bad_cert_path "/etc/kubemq/certs/bad-client.pem"

  describe "TLS connection" do
    test "connects with server CA certificate" do
      {:ok, client} =
        Client.start_link(
          address: @tls_broker_address,
          client_id: "integration-tls-test",
          tls: [cacertfile: @ca_cert_path]
        )

      on_exit(fn ->
        if Process.alive?(client), do: Client.close(client)
      end)

      assert Client.connected?(client)
      {:ok, info} = Client.ping(client)
      assert info.host != nil
    end

    test "connects with mTLS (mutual TLS)" do
      {:ok, client} =
        Client.start_link(
          address: @tls_broker_address,
          client_id: "integration-mtls-test",
          tls: [
            cacertfile: @ca_cert_path,
            certfile: @client_cert_path,
            keyfile: @client_key_path
          ]
        )

      on_exit(fn ->
        if Process.alive?(client), do: Client.close(client)
      end)

      assert Client.connected?(client)
      {:ok, info} = Client.ping(client)
      assert info.host != nil
    end

    test "connects with system CA certificates" do
      {:ok, client} =
        Client.start_link(
          address: @tls_broker_address,
          client_id: "integration-system-ca-test",
          tls: [use_system_ca: true]
        )

      on_exit(fn ->
        if Process.alive?(client), do: Client.close(client)
      end)

      assert Client.connected?(client)
    end

    test "rejects connection with bad certificate" do
      result =
        Client.start_link(
          address: @tls_broker_address,
          client_id: "integration-bad-cert-test",
          tls: [
            cacertfile: @ca_cert_path,
            certfile: @bad_cert_path,
            keyfile: @client_key_path
          ]
        )

      case result do
        {:ok, client} ->
          # Client may start but fail to connect
          on_exit(fn ->
            if Process.alive?(client), do: Client.close(client)
          end)

          # Connection should eventually fail or be in a non-ready state
          Process.sleep(2_000)
          refute Client.connected?(client)

        {:error, _reason} ->
          # Expected: connection refused with bad cert
          :ok
      end
    end

    test "TLS with verify_peer rejects mismatched hostname" do
      result =
        Client.start_link(
          address: "wrong-hostname:50000",
          client_id: "integration-hostname-test",
          tls: [
            cacertfile: @ca_cert_path,
            verify: :verify_peer
          ]
        )

      case result do
        {:ok, client} ->
          on_exit(fn ->
            if Process.alive?(client), do: Client.close(client)
          end)

          Process.sleep(2_000)
          refute Client.connected?(client)

        {:error, _reason} ->
          :ok
      end
    end
  end
end
