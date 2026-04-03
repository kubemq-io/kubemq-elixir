defmodule KubeMQ.ValidationTest do
  use ExUnit.Case, async: true

  alias KubeMQ.{Error, Validation}

  describe "validate_channel/2" do
    test "rejects nil channel" do
      assert {:error, %Error{code: :validation}} = Validation.validate_channel(nil)
    end

    test "rejects empty string channel" do
      assert {:error, %Error{code: :validation, message: msg}} = Validation.validate_channel("")
      assert msg =~ "cannot be empty"
    end

    test "accepts valid channel name" do
      assert :ok = Validation.validate_channel("my-channel")
    end

    test "accepts channel with dots" do
      assert :ok = Validation.validate_channel("events.orders.created")
    end

    test "rejects channel with trailing dot" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_channel("events.")

      assert msg =~ "trailing" or msg =~ "'.'"
    end

    test "rejects channel with whitespace" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_channel("my channel")

      assert msg =~ "whitespace"
    end

    test "rejects wildcards by default" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_channel("events.*")

      assert msg =~ "wildcard"
    end

    test "accepts wildcards when allow_wildcards: true" do
      assert :ok = Validation.validate_channel("events.*", allow_wildcards: true)
    end

    test "accepts > wildcard when allow_wildcards: true" do
      assert :ok = Validation.validate_channel("events.>", allow_wildcards: true)
    end

    test "rejects > wildcard when allow_wildcards: false" do
      assert {:error, %Error{code: :validation}} =
               Validation.validate_channel("events.>", allow_wildcards: false)
    end
  end

  describe "validate_max_messages/1" do
    test "accepts 1 (lower boundary)" do
      assert :ok = Validation.validate_max_messages(1)
    end

    test "accepts 1024 (upper boundary)" do
      assert :ok = Validation.validate_max_messages(1024)
    end

    test "accepts value in middle of range" do
      assert :ok = Validation.validate_max_messages(100)
    end

    test "rejects 0 (below lower boundary)" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_max_messages(0)

      assert msg =~ "max_messages"
    end

    test "rejects 1025 (above upper boundary)" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_max_messages(1025)

      assert msg =~ "max_messages"
    end

    test "rejects negative value" do
      assert {:error, %Error{code: :validation}} = Validation.validate_max_messages(-1)
    end

    test "rejects non-integer" do
      assert {:error, %Error{code: :validation}} = Validation.validate_max_messages("ten")
    end
  end

  describe "validate_wait_timeout/1" do
    test "accepts 0 (lower boundary)" do
      assert :ok = Validation.validate_wait_timeout(0)
    end

    test "accepts 3_600_000 (upper boundary, 1 hour)" do
      assert :ok = Validation.validate_wait_timeout(3_600_000)
    end

    test "accepts value in middle of range" do
      assert :ok = Validation.validate_wait_timeout(5_000)
    end

    test "rejects negative value" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_wait_timeout(-1)

      assert msg =~ "wait_timeout"
    end

    test "rejects value above 3_600_000" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_wait_timeout(3_600_001)

      assert msg =~ "wait_timeout"
    end

    test "rejects non-integer" do
      assert {:error, %Error{code: :validation}} = Validation.validate_wait_timeout("slow")
    end
  end

  describe "validate_client_id/1" do
    test "rejects nil" do
      assert {:error, %Error{code: :validation}} = Validation.validate_client_id(nil)
    end

    test "rejects empty string" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_client_id("")

      assert msg =~ "client_id"
    end

    test "accepts valid client_id" do
      assert :ok = Validation.validate_client_id("my-app")
    end
  end

  describe "validate_content/2" do
    test "rejects nil metadata and nil body" do
      assert {:error, %Error{code: :validation}} = Validation.validate_content(nil, nil)
    end

    test "rejects empty metadata and nil body" do
      assert {:error, %Error{code: :validation}} = Validation.validate_content("", nil)
    end

    test "rejects nil metadata and empty body" do
      assert {:error, %Error{code: :validation}} = Validation.validate_content(nil, "")
    end

    test "rejects empty metadata and empty body" do
      assert {:error, %Error{code: :validation}} = Validation.validate_content("", "")
    end

    test "accepts non-empty metadata with nil body" do
      assert :ok = Validation.validate_content("meta", nil)
    end

    test "accepts nil metadata with non-empty body" do
      assert :ok = Validation.validate_content(nil, "body")
    end
  end

  describe "validate_timeout/1" do
    test "accepts positive integer" do
      assert :ok = Validation.validate_timeout(5_000)
    end

    test "rejects zero" do
      assert {:error, %Error{code: :validation}} = Validation.validate_timeout(0)
    end

    test "rejects negative" do
      assert {:error, %Error{code: :validation}} = Validation.validate_timeout(-1)
    end
  end

  describe "validate_start_position/1" do
    test "rejects :undefined" do
      assert {:error, %Error{code: :validation}} = Validation.validate_start_position(:undefined)
    end

    test "rejects nil" do
      assert {:error, %Error{code: :validation}} = Validation.validate_start_position(nil)
    end

    test "accepts :start_new_only" do
      assert :ok = Validation.validate_start_position(:start_new_only)
    end

    test "accepts :start_from_first" do
      assert :ok = Validation.validate_start_position(:start_from_first)
    end

    test "accepts :start_from_last" do
      assert :ok = Validation.validate_start_position(:start_from_last)
    end

    test "accepts {:start_at_sequence, n} with positive n" do
      assert :ok = Validation.validate_start_position({:start_at_sequence, 42})
    end

    test "rejects {:start_at_sequence, 0}" do
      assert {:error, %Error{code: :validation}} =
               Validation.validate_start_position({:start_at_sequence, 0})
    end

    test "accepts {:start_at_time, t} with positive t" do
      assert :ok = Validation.validate_start_position({:start_at_time, 1_700_000_000})
    end

    test "accepts {:start_at_time_delta, d} with positive d" do
      assert :ok = Validation.validate_start_position({:start_at_time_delta, 60_000})
    end

    test "rejects invalid position" do
      assert {:error, %Error{code: :validation}} =
               Validation.validate_start_position(:invalid_pos)
    end
  end

  describe "validate_requeue_channel/1" do
    test "rejects nil" do
      assert {:error, %Error{code: :validation}} = Validation.validate_requeue_channel(nil)
    end

    test "rejects empty string" do
      assert {:error, %Error{code: :validation}} = Validation.validate_requeue_channel("")
    end

    test "accepts valid channel" do
      assert :ok = Validation.validate_requeue_channel("dead-letter-queue")
    end
  end

  describe "validate_cache/2" do
    test "accepts nil cache_key regardless of ttl" do
      assert :ok = Validation.validate_cache(nil, nil)
    end

    test "accepts empty cache_key regardless of ttl" do
      assert :ok = Validation.validate_cache("", nil)
    end

    test "accepts non-empty cache_key with positive ttl" do
      assert :ok = Validation.validate_cache("my-key", 60)
    end

    test "rejects non-empty cache_key with zero ttl" do
      assert {:error, %Error{code: :validation}} = Validation.validate_cache("my-key", 0)
    end

    test "rejects non-empty cache_key with negative ttl" do
      assert {:error, %Error{code: :validation}} = Validation.validate_cache("my-key", -1)
    end

    test "rejects non-empty cache_key with nil ttl" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_cache("my-key", nil)

      assert msg =~ "cache_ttl"
    end
  end

  describe "validate_response_fields/1" do
    test "rejects nil request_id" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_response_fields(%{request_id: nil, response_to: "reply-ch"})

      assert msg =~ "request_id"
    end

    test "rejects empty request_id" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_response_fields(%{request_id: "", response_to: "reply-ch"})

      assert msg =~ "request_id"
    end

    test "rejects nil response_to" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_response_fields(%{request_id: "req-1", response_to: nil})

      assert msg =~ "reply_channel"
    end

    test "rejects empty response_to" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_response_fields(%{request_id: "req-1", response_to: ""})

      assert msg =~ "reply_channel"
    end

    test "accepts valid request_id and response_to" do
      assert :ok =
               Validation.validate_response_fields(%{
                 request_id: "req-1",
                 response_to: "reply-ch"
               })
    end
  end

  describe "validate_channel/2 additional edge cases" do
    test "rejects channel with tab character" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_channel("my\tchannel")

      assert msg =~ "whitespace"
    end

    test "rejects channel with newline" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_channel("my\nchannel")

      assert msg =~ "whitespace"
    end

    test "rejects channel with both trailing dot and whitespace (whitespace checked first)" do
      assert {:error, %Error{code: :validation}} =
               Validation.validate_channel("my channel.")
    end

    test "accepts single-character channel" do
      assert :ok = Validation.validate_channel("a")
    end
  end

  describe "validate_content/2 additional combos" do
    test "accepts both metadata and body present" do
      assert :ok = Validation.validate_content("meta", "body")
    end
  end

  describe "validate_timeout/1 additional" do
    test "rejects non-integer value" do
      assert {:error, %Error{code: :validation}} = Validation.validate_timeout("fast")
    end

    test "accepts timeout of 1 (minimum positive)" do
      assert :ok = Validation.validate_timeout(1)
    end
  end

  describe "validate_start_position/1 additional" do
    test "accepts :start_from_new" do
      assert :ok = Validation.validate_start_position(:start_from_new)
    end

    test "rejects {:start_at_time, 0}" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_start_position({:start_at_time, 0})

      assert msg =~ "start_at_time"
    end

    test "rejects {:start_at_time_delta, 0}" do
      assert {:error, %Error{code: :validation, message: msg}} =
               Validation.validate_start_position({:start_at_time_delta, 0})

      assert msg =~ "start_at_time_delta"
    end

    test "rejects {:start_at_sequence, -1}" do
      assert {:error, %Error{code: :validation}} =
               Validation.validate_start_position({:start_at_sequence, -1})
    end
  end
end
