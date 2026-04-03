defmodule KubeMQ.Validation do
  @moduledoc false

  alias KubeMQ.Error

  @wildcard_pattern ~r/[*>]/
  @whitespace_pattern ~r/\s/

  # Rule 10.1.1 & 10.4.3: client_id cannot be empty
  @spec validate_client_id(String.t() | nil) :: :ok | {:error, Error.t()}
  def validate_client_id(nil), do: {:error, Error.validation("client_id cannot be empty")}
  def validate_client_id(""), do: {:error, Error.validation("client_id cannot be empty")}
  def validate_client_id(id) when is_binary(id), do: :ok

  # Rules 10.1.2-10.1.5, 10.1.3, 10.3.5: channel validation
  # opts:
  #   :allow_wildcards - true only for Events subscribe (rule 10.1.3)
  @spec validate_channel(String.t() | nil, keyword()) :: :ok | {:error, Error.t()}
  def validate_channel(channel, opts \\ [])
  def validate_channel(nil, _opts), do: {:error, Error.validation("channel cannot be empty")}
  def validate_channel("", _opts), do: {:error, Error.validation("channel cannot be empty")}

  def validate_channel(channel, opts) when is_binary(channel) do
    allow_wildcards = Keyword.get(opts, :allow_wildcards, false)

    with :ok <- check_whitespace(channel),
         :ok <- check_trailing_dot(channel),
         :ok <- check_wildcards(channel, allow_wildcards) do
      :ok
    end
  end

  # Rule 10.1.6: at least one of metadata/body required
  @spec validate_content(String.t() | nil, binary() | nil) :: :ok | {:error, Error.t()}
  def validate_content(nil, nil),
    do: {:error, Error.validation("at least one of metadata or body is required")}

  def validate_content("", nil),
    do: {:error, Error.validation("at least one of metadata or body is required")}

  def validate_content(nil, ""),
    do: {:error, Error.validation("at least one of metadata or body is required")}

  def validate_content("", ""),
    do: {:error, Error.validation("at least one of metadata or body is required")}

  def validate_content(_metadata, _body), do: :ok

  # Rule 10.2.1: timeout must be > 0 for Commands and Queries
  @spec validate_timeout(integer()) :: :ok | {:error, Error.t()}
  def validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: :ok
  def validate_timeout(_), do: {:error, Error.validation("timeout must be greater than 0")}

  # Rule 10.2.2: cache_key + cache_ttl validation
  @spec validate_cache(String.t() | nil, integer() | nil) :: :ok | {:error, Error.t()}
  def validate_cache(nil, _ttl), do: :ok
  def validate_cache("", _ttl), do: :ok
  def validate_cache(_key, ttl) when is_integer(ttl) and ttl > 0, do: :ok

  def validate_cache(_key, _ttl),
    do: {:error, Error.validation("cache_ttl must be > 0 when cache_key is set")}

  # Rules 10.2.3 & 10.2.4: response fields validation
  @spec validate_response_fields(map()) :: :ok | {:error, Error.t()}
  def validate_response_fields(%{request_id: nil}),
    do: {:error, Error.validation("request_id cannot be empty")}

  def validate_response_fields(%{request_id: ""}),
    do: {:error, Error.validation("request_id cannot be empty")}

  def validate_response_fields(%{response_to: nil}),
    do: {:error, Error.validation("reply_channel cannot be empty")}

  def validate_response_fields(%{response_to: ""}),
    do: {:error, Error.validation("reply_channel cannot be empty")}

  def validate_response_fields(_), do: :ok

  # Rules 10.3.1-10.3.4: Events Store start position validation
  @spec validate_start_position(term()) :: :ok | {:error, Error.t()}
  def validate_start_position(:undefined),
    do: {:error, Error.validation("start_at position is required for Events Store subscriptions")}

  def validate_start_position(nil),
    do: {:error, Error.validation("start_at position is required for Events Store subscriptions")}

  def validate_start_position(:start_new_only), do: :ok
  def validate_start_position(:start_from_new), do: :ok
  def validate_start_position(:start_from_first), do: :ok
  def validate_start_position(:start_from_last), do: :ok

  def validate_start_position({:start_at_sequence, n})
      when is_integer(n) and n > 0,
      do: :ok

  def validate_start_position({:start_at_sequence, _}),
    do: {:error, Error.validation("start_at_sequence value must be > 0")}

  def validate_start_position({:start_at_time, t})
      when is_integer(t) and t > 0,
      do: :ok

  def validate_start_position({:start_at_time, _}),
    do: {:error, Error.validation("start_at_time value must be > 0")}

  def validate_start_position({:start_at_time_delta, d})
      when is_integer(d) and d > 0,
      do: :ok

  def validate_start_position({:start_at_time_delta, _}),
    do: {:error, Error.validation("start_at_time_delta value must be > 0")}

  def validate_start_position(_),
    do: {:error, Error.validation("invalid start_at position")}

  # Rule 10.4.1: max_messages >= 1 and <= 1024
  @spec validate_max_messages(integer()) :: :ok | {:error, Error.t()}
  def validate_max_messages(n) when is_integer(n) and n >= 1 and n <= 1024, do: :ok

  def validate_max_messages(_),
    do: {:error, Error.validation("max_messages must be between 1 and 1024")}

  # Rule 10.4.2: wait_timeout >= 0 and <= 3,600,000ms (1 hour)
  @spec validate_wait_timeout(integer()) :: :ok | {:error, Error.t()}
  def validate_wait_timeout(t) when is_integer(t) and t >= 0 and t <= 3_600_000, do: :ok

  def validate_wait_timeout(_),
    do: {:error, Error.validation("wait_timeout must be between 0 and 3600000ms")}

  # Rule 10.4.4: ReQueueChannel must be non-empty for requeue operations
  @spec validate_requeue_channel(String.t() | nil) :: :ok | {:error, Error.t()}
  def validate_requeue_channel(nil),
    do: {:error, Error.validation("requeue channel cannot be empty for requeue operations")}

  def validate_requeue_channel(""),
    do: {:error, Error.validation("requeue channel cannot be empty for requeue operations")}

  def validate_requeue_channel(ch) when is_binary(ch), do: :ok

  # -- Internal helpers --

  defp check_whitespace(channel) do
    if Regex.match?(@whitespace_pattern, channel),
      do: {:error, Error.validation("channel cannot contain whitespace")},
      else: :ok
  end

  defp check_trailing_dot(channel) do
    if String.ends_with?(channel, "."),
      do: {:error, Error.validation("channel cannot end with '.'")},
      else: :ok
  end

  defp check_wildcards(_channel, true), do: :ok

  defp check_wildcards(channel, false) do
    if Regex.match?(@wildcard_pattern, channel),
      do: {:error, Error.validation("wildcards not allowed for this operation")},
      else: :ok
  end
end
