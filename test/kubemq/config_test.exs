defmodule KubeMQ.ConfigTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Config

  @valid_base [client_id: "test-client"]

  describe "validate/1 with valid configuration" do
    test "accepts minimal valid config" do
      assert {:ok, validated} = Config.validate(@valid_base)
      assert Keyword.get(validated, :client_id) == "test-client"
    end

    test "applies defaults for address" do
      assert {:ok, validated} = Config.validate(@valid_base)
      assert Keyword.get(validated, :address) == "localhost:50000"
    end

    test "applies defaults for connection_timeout" do
      assert {:ok, validated} = Config.validate(@valid_base)
      assert Keyword.get(validated, :connection_timeout) == 10_000
    end

    test "applies defaults for retry_policy sub-options" do
      assert {:ok, validated} = Config.validate(@valid_base)
      retry = Keyword.get(validated, :retry_policy)
      assert Keyword.get(retry, :max_retries) == 3
      assert Keyword.get(retry, :initial_delay) == 100
      assert Keyword.get(retry, :max_delay) == 1_000
      assert Keyword.get(retry, :multiplier) == 2.0
    end

    test "applies defaults for reconnect_policy sub-options" do
      assert {:ok, validated} = Config.validate(@valid_base)
      reconnect = Keyword.get(validated, :reconnect_policy)
      assert Keyword.get(reconnect, :enabled) == true
      assert Keyword.get(reconnect, :initial_delay) == 1_000
      assert Keyword.get(reconnect, :max_delay) == 30_000
      assert Keyword.get(reconnect, :max_attempts) == 0
      assert Keyword.get(reconnect, :multiplier) == 2.0
    end

    test "accepts custom reconnect_policy values" do
      opts =
        @valid_base ++
          [reconnect_policy: [enabled: false, initial_delay: 500, max_delay: 10_000]]

      assert {:ok, validated} = Config.validate(opts)
      reconnect = Keyword.get(validated, :reconnect_policy)
      assert Keyword.get(reconnect, :enabled) == false
      assert Keyword.get(reconnect, :initial_delay) == 500
      assert Keyword.get(reconnect, :max_delay) == 10_000
    end

    test "accepts custom retry_policy values" do
      opts = @valid_base ++ [retry_policy: [max_retries: 5, initial_delay: 200]]

      assert {:ok, validated} = Config.validate(opts)
      retry = Keyword.get(validated, :retry_policy)
      assert Keyword.get(retry, :max_retries) == 5
      assert Keyword.get(retry, :initial_delay) == 200
    end

    test "accepts send_timeout and rpc_timeout" do
      opts = @valid_base ++ [send_timeout: 3_000, rpc_timeout: 8_000]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :send_timeout) == 3_000
      assert Keyword.get(validated, :rpc_timeout) == 8_000
    end
  end

  describe "validate/1 with nested validation for reconnect_policy" do
    test "rejects non-boolean enabled" do
      opts = @valid_base ++ [reconnect_policy: [enabled: "yes"]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects non-integer initial_delay" do
      opts = @valid_base ++ [reconnect_policy: [initial_delay: "fast"]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects zero initial_delay (must be pos_integer)" do
      opts = @valid_base ++ [reconnect_policy: [initial_delay: 0]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects negative max_delay" do
      opts = @valid_base ++ [reconnect_policy: [max_delay: -1]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects non-float multiplier" do
      opts = @valid_base ++ [reconnect_policy: [multiplier: 2]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects negative max_attempts" do
      opts = @valid_base ++ [reconnect_policy: [max_attempts: -1]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects unknown sub-option" do
      opts = @valid_base ++ [reconnect_policy: [bogus_key: true]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end
  end

  describe "validate/1 with nested validation for retry_policy" do
    test "rejects non-integer max_retries" do
      opts = @valid_base ++ [retry_policy: [max_retries: "three"]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects zero initial_delay" do
      opts = @valid_base ++ [retry_policy: [initial_delay: 0]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects negative max_delay" do
      opts = @valid_base ++ [retry_policy: [max_delay: -5]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects non-float multiplier" do
      opts = @valid_base ++ [retry_policy: [multiplier: 3]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects unknown sub-option" do
      opts = @valid_base ++ [retry_policy: [jitter: true]]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end
  end

  describe "validate/1 rejects missing required fields" do
    test "rejects missing client_id" do
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate([])
    end
  end

  describe "validate/1 rejects invalid top-level types" do
    test "rejects non-string address" do
      opts = [client_id: "test", address: 12_345]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects non-integer connection_timeout" do
      opts = [client_id: "test", connection_timeout: "slow"]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end

    test "rejects unknown top-level option" do
      opts = [client_id: "test", unknown_opt: true]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end
  end

  describe "keepalive enforcement" do
    test "enforces minimum keepalive_time of 5000ms" do
      opts = @valid_base ++ [keepalive_time: 1_000]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :keepalive_time) == 5_000
    end

    test "preserves keepalive_time when above minimum" do
      opts = @valid_base ++ [keepalive_time: 15_000]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :keepalive_time) == 15_000
    end

    test "default keepalive_time is 10_000" do
      assert {:ok, validated} = Config.validate(@valid_base)
      assert Keyword.get(validated, :keepalive_time) == 10_000
    end
  end

  describe "validate!/1" do
    test "returns validated opts on valid input" do
      validated = Config.validate!(@valid_base)
      assert Keyword.get(validated, :client_id) == "test-client"
    end

    test "raises NimbleOptions.ValidationError on invalid input" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Config.validate!([])
      end
    end

    test "also enforces keepalive minimum" do
      validated = Config.validate!(@valid_base ++ [keepalive_time: 2_000])
      assert Keyword.get(validated, :keepalive_time) == 5_000
    end
  end

  describe "schema/0" do
    test "returns a NimbleOptions schema" do
      schema = Config.schema()
      assert is_list(schema) or is_struct(schema)
    end
  end

  describe "callback options" do
    test "accepts function callbacks" do
      opts =
        @valid_base ++
          [
            on_connected: fn -> :ok end,
            on_disconnected: fn -> :ok end,
            on_reconnecting: fn -> :ok end,
            on_reconnected: fn -> :ok end,
            on_closed: fn -> :ok end
          ]

      assert {:ok, _validated} = Config.validate(opts)
    end

    test "rejects non-function callback" do
      opts = @valid_base ++ [on_connected: "not_a_function"]
      assert {:error, %NimbleOptions.ValidationError{}} = Config.validate(opts)
    end
  end

  describe "validate/1 all defaults applied" do
    test "applies all default values for minimal config" do
      assert {:ok, validated} = Config.validate(@valid_base)

      assert Keyword.get(validated, :address) == "localhost:50000"
      assert Keyword.get(validated, :connection_timeout) == 10_000
      assert Keyword.get(validated, :send_timeout) == 5_000
      assert Keyword.get(validated, :rpc_timeout) == 10_000
      assert Keyword.get(validated, :keepalive_time) == 10_000
      assert Keyword.get(validated, :keepalive_timeout) == 5_000
      assert Keyword.get(validated, :max_receive_size) == 4_194_304
      assert Keyword.get(validated, :max_send_size) == 104_857_600
      assert Keyword.get(validated, :default_cache_ttl) == 900_000
      assert Keyword.get(validated, :receive_buffer_size) == 10
      assert Keyword.get(validated, :reconnect_buffer_size) == 1_000
    end
  end

  describe "validate/1 TLS options" do
    test "accepts tls keyword list" do
      opts = @valid_base ++ [tls: [cacertfile: "/tmp/ca.pem"]]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :tls) == [cacertfile: "/tmp/ca.pem"]
    end
  end

  describe "validate/1 credential_provider option" do
    test "accepts atom for credential_provider" do
      opts = @valid_base ++ [credential_provider: KubeMQ.StaticTokenProvider]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :credential_provider) == KubeMQ.StaticTokenProvider
    end
  end

  describe "validate/1 name option" do
    test "accepts atom name" do
      opts = @valid_base ++ [name: :my_client]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :name) == :my_client
    end

    test "accepts {:via, module, term} name" do
      opts = @valid_base ++ [name: {:via, Registry, {:kubemq, "client-1"}}]
      assert {:ok, validated} = Config.validate(opts)
      assert Keyword.get(validated, :name) == {:via, Registry, {:kubemq, "client-1"}}
    end
  end
end
