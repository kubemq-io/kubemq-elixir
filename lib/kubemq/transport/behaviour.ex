defmodule KubeMQ.Transport do
  @moduledoc false

  @type channel :: GRPC.Channel.t()

  @callback ping(channel :: channel()) :: {:ok, map()} | {:error, term()}

  @callback send_event(channel :: channel(), request :: map()) :: :ok | {:error, term()}

  @callback send_event_stream(channel :: channel()) :: {:ok, pid()} | {:error, term()}

  @callback subscribe(channel :: channel(), subscribe :: map()) :: {:ok, pid()} | {:error, term()}

  @callback send_request(channel :: channel(), request :: map()) ::
              {:ok, map()} | {:error, term()}

  @callback send_response(channel :: channel(), response :: map()) :: :ok | {:error, term()}

  @callback send_queue_message(channel :: channel(), msg :: map()) ::
              {:ok, map()} | {:error, term()}

  @callback send_queue_messages_batch(channel :: channel(), batch :: map()) ::
              {:ok, map()} | {:error, term()}

  @callback receive_queue_messages(channel :: channel(), request :: map()) ::
              {:ok, map()} | {:error, term()}

  @callback ack_all_queue_messages(channel :: channel(), request :: map()) ::
              {:ok, map()} | {:error, term()}

  @callback queue_upstream(channel :: channel()) :: {:ok, pid()} | {:error, term()}

  @callback queue_downstream(channel :: channel()) :: {:ok, pid()} | {:error, term()}
end
