defmodule KubeMQ.SubscribeType do
  @moduledoc false

  @spec undefined() :: 0
  def undefined, do: 0

  @spec events() :: 1
  def events, do: 1

  @spec events_store() :: 2
  def events_store, do: 2

  @spec commands() :: 3
  def commands, do: 3

  @spec queries() :: 4
  def queries, do: 4
end

defmodule KubeMQ.RequestType do
  @moduledoc false

  @spec unknown() :: 0
  def unknown, do: 0

  @spec command() :: 1
  def command, do: 1

  @spec query() :: 2
  def query, do: 2
end

defmodule KubeMQ.DownstreamRequestType do
  @moduledoc false

  @spec unknown() :: 0
  def unknown, do: 0

  @spec get() :: 1
  def get, do: 1

  @spec ack_all() :: 2
  def ack_all, do: 2

  @spec ack_range() :: 3
  def ack_range, do: 3

  @spec nack_all() :: 4
  def nack_all, do: 4

  @spec nack_range() :: 5
  def nack_range, do: 5

  @spec requeue_all() :: 6
  def requeue_all, do: 6

  @spec requeue_range() :: 7
  def requeue_range, do: 7

  @spec active_offsets() :: 8
  def active_offsets, do: 8

  @spec transaction_status() :: 9
  def transaction_status, do: 9

  @spec close_by_client() :: 10
  def close_by_client, do: 10

  @spec close_by_server() :: 11
  def close_by_server, do: 11
end

defmodule KubeMQ.EventsStoreType do
  @moduledoc false

  @spec start_new_only() :: 1
  def start_new_only, do: 1

  @spec start_from_first() :: 2
  def start_from_first, do: 2

  @spec start_from_last() :: 3
  def start_from_last, do: 3

  @spec start_at_sequence() :: 4
  def start_at_sequence, do: 4

  @spec start_at_time() :: 5
  def start_at_time, do: 5

  @spec start_at_time_delta() :: 6
  def start_at_time_delta, do: 6
end
