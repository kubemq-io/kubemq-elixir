defmodule KubeMQ.ErrorTest do
  use ExUnit.Case, async: true
  doctest KubeMQ.Error

  alias KubeMQ.Error

  describe "from_grpc_status/2 with atom codes" do
    test "transient atom is retryable" do
      error = Error.from_grpc_status(:transient, [])
      assert %Error{code: :transient, retryable?: true} = error
      assert error.message == "gRPC error (transient)"
    end

    test "timeout atom is retryable" do
      error = Error.from_grpc_status(:timeout, [])
      assert %Error{code: :timeout, retryable?: true} = error
    end

    test "throttling atom is retryable" do
      error = Error.from_grpc_status(:throttling, [])
      assert %Error{code: :throttling, retryable?: true} = error
    end

    test "fatal atom is non-retryable" do
      error = Error.from_grpc_status(:fatal, [])
      assert %Error{code: :fatal, retryable?: false} = error
    end

    test "validation atom is non-retryable" do
      error = Error.from_grpc_status(:validation, [])
      assert %Error{code: :validation, retryable?: false} = error
    end

    test "not_found atom is non-retryable" do
      error = Error.from_grpc_status(:not_found, [])
      assert %Error{code: :not_found, retryable?: false} = error
    end

    test "authorization atom is non-retryable" do
      error = Error.from_grpc_status(:authorization, [])
      assert %Error{code: :authorization, retryable?: false} = error
    end

    test "authentication atom is non-retryable" do
      error = Error.from_grpc_status(:authentication, [])
      assert %Error{code: :authentication, retryable?: false} = error
    end

    test "cancellation atom is non-retryable" do
      error = Error.from_grpc_status(:cancellation, [])
      assert %Error{code: :cancellation, retryable?: false} = error
    end

    test "unknown atom defaults to non-retryable (catch-all)" do
      error = Error.from_grpc_status(:some_unknown_code, [])
      assert %Error{retryable?: false} = error
      assert error.code == :some_unknown_code
    end

    test "custom message passed via opts overrides default" do
      error = Error.from_grpc_status(:transient, message: "custom msg")
      assert error.message == "custom msg"
    end

    test "operation and channel propagated from opts" do
      error =
        Error.from_grpc_status(:fatal,
          operation: "send_event",
          channel: "test-ch"
        )

      assert error.operation == "send_event"
      assert error.channel == "test-ch"
    end
  end

  describe "from_grpc_status/2 with integer codes" do
    test "code 1 maps to cancellation, non-retryable" do
      error = Error.from_grpc_status(1, [])
      assert %Error{code: :cancellation, retryable?: false} = error
    end

    test "code 2 maps to fatal, non-retryable" do
      error = Error.from_grpc_status(2, [])
      assert %Error{code: :fatal, retryable?: false} = error
    end

    test "code 4 maps to timeout, retryable" do
      error = Error.from_grpc_status(4, [])
      assert %Error{code: :timeout, retryable?: true} = error
    end

    test "code 8 maps to throttling, retryable" do
      error = Error.from_grpc_status(8, [])
      assert %Error{code: :throttling, retryable?: true} = error
    end

    test "code 10 maps to transient, retryable" do
      error = Error.from_grpc_status(10, [])
      assert %Error{code: :transient, retryable?: true} = error
    end

    test "code 14 maps to transient, retryable" do
      error = Error.from_grpc_status(14, [])
      assert %Error{code: :transient, retryable?: true} = error
    end

    test "code 16 maps to authentication, non-retryable" do
      error = Error.from_grpc_status(16, [])
      assert %Error{code: :authentication, retryable?: false} = error
    end

    test "unknown integer code defaults to fatal, non-retryable" do
      error = Error.from_grpc_status(999, [])
      assert %Error{code: :fatal, retryable?: false} = error
    end
  end

  describe "from_grpc_status/2 with map input" do
    test "extracts status and message from map" do
      error = Error.from_grpc_status(%{status: 14, message: "connection reset"}, [])
      assert %Error{code: :transient, retryable?: true} = error
      assert error.message == "connection reset"
    end

    test "uses default message when grpc_message is nil" do
      error = Error.from_grpc_status(%{status: 2, message: nil}, [])
      assert error.message == "gRPC error (status 2)"
    end

    test "uses default message when grpc_message is empty string" do
      error = Error.from_grpc_status(%{status: 2, message: ""}, [])
      assert error.message == "gRPC error (status 2)"
    end
  end

  describe "retryability classification completeness" do
    @retryable_codes [:transient, :timeout, :throttling]
    @non_retryable_codes [
      :cancellation,
      :fatal,
      :validation,
      :not_found,
      :authorization,
      :authentication
    ]

    test "all retryable atom codes produce retryable errors" do
      for code <- @retryable_codes do
        error = Error.from_grpc_status(code, [])
        assert error.retryable?, "Expected #{code} to be retryable"
      end
    end

    test "all non-retryable atom codes produce non-retryable errors" do
      for code <- @non_retryable_codes do
        error = Error.from_grpc_status(code, [])
        refute error.retryable?, "Expected #{code} to be non-retryable"
      end
    end

    @retryable_integers [4, 8, 10, 14]
    @non_retryable_integers [1, 2, 3, 5, 6, 7, 9, 11, 12, 13, 15, 16]

    test "all retryable integer codes produce retryable errors" do
      for code <- @retryable_integers do
        error = Error.from_grpc_status(code, [])
        assert error.retryable?, "Expected integer code #{code} to be retryable"
      end
    end

    test "all non-retryable integer codes produce non-retryable errors" do
      for code <- @non_retryable_integers do
        error = Error.from_grpc_status(code, [])
        refute error.retryable?, "Expected integer code #{code} to be non-retryable"
      end
    end
  end

  describe "named constructors" do
    test "transient/2 creates retryable error" do
      error = Error.transient("connection lost")
      assert %Error{code: :transient, retryable?: true} = error
      assert error.message == "connection lost"
    end

    test "fatal/2 creates non-retryable error" do
      error = Error.fatal("permanent failure")
      assert %Error{code: :fatal, retryable?: false} = error
    end

    test "validation/2 creates non-retryable error" do
      error = Error.validation("invalid input")
      assert %Error{code: :validation, retryable?: false} = error
    end

    test "buffer_full/1 creates non-retryable error" do
      error = Error.buffer_full()
      assert %Error{code: :buffer_full, retryable?: false} = error
      assert error.message == "operation buffer full during reconnection"
    end

    test "client_closed/1 creates non-retryable error" do
      error = Error.client_closed()
      assert %Error{code: :client_closed, retryable?: false} = error
      assert error.message == "client is closed"
    end

    test "stream_broken/2 creates non-retryable error" do
      error = Error.stream_broken("stream closed")
      assert %Error{code: :stream_broken, retryable?: false} = error
    end
  end

  describe "Error as exception" do
    test "message/1 returns the message field" do
      error = Error.fatal("something broke")
      assert Exception.message(error) == "something broke"
    end

    test "can be raised and rescued" do
      assert_raise Error, "boom", fn ->
        raise Error.fatal("boom")
      end
    end
  end

  describe "opts propagation" do
    test "request_id is stored" do
      error = Error.transient("retry me", request_id: "req-123")
      assert error.request_id == "req-123"
    end

    test "cause is stored" do
      cause = %RuntimeError{message: "root cause"}
      error = Error.fatal("wrapped", cause: cause)
      assert error.cause == cause
    end
  end

  # ===================================================================
  # Phase 10: Additional coverage gap tests for error.ex
  # ===================================================================

  describe "remaining named constructors" do
    test "timeout/2 creates retryable error" do
      error = Error.timeout("operation timed out")
      assert %Error{code: :timeout, retryable?: true} = error
      assert error.message == "operation timed out"
    end

    test "throttling/2 creates retryable error" do
      error = Error.throttling("rate limited")
      assert %Error{code: :throttling, retryable?: true} = error
      assert error.message == "rate limited"
    end

    test "authentication/2 creates non-retryable error" do
      error = Error.authentication("bad credentials")
      assert %Error{code: :authentication, retryable?: false} = error
      assert error.message == "bad credentials"
    end

    test "authorization/2 creates non-retryable error" do
      error = Error.authorization("forbidden")
      assert %Error{code: :authorization, retryable?: false} = error
      assert error.message == "forbidden"
    end

    test "not_found/2 creates non-retryable error" do
      error = Error.not_found("channel not found")
      assert %Error{code: :not_found, retryable?: false} = error
      assert error.message == "channel not found"
    end
  end

  describe "named constructors with opts" do
    test "timeout with opts propagates operation and channel" do
      error = Error.timeout("timed out", operation: "send", channel: "ch1")
      assert error.operation == "send"
      assert error.channel == "ch1"
    end

    test "throttling with opts propagates cause" do
      cause = %RuntimeError{message: "underlying"}
      error = Error.throttling("rate limited", cause: cause)
      assert error.cause == cause
    end

    test "authentication with request_id" do
      error = Error.authentication("bad creds", request_id: "req-999")
      assert error.request_id == "req-999"
    end

    test "buffer_full with opts" do
      error = Error.buffer_full(operation: "send_event")
      assert error.operation == "send_event"
    end

    test "client_closed with opts" do
      error = Error.client_closed(channel: "my-channel")
      assert error.channel == "my-channel"
    end
  end
end
