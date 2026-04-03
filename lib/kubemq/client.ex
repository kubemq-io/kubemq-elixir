defmodule KubeMQ.Client do
  @moduledoc """
  Main KubeMQ client GenServer — primary API surface for all messaging patterns.

  Owns a `KubeMQ.Connection` process and a `DynamicSupervisor` for subscriptions.
  All operations are delegated to the appropriate pattern modules and the transport layer.

  ## Quick Start

      {:ok, client} = KubeMQ.Client.start_link(address: "localhost:50000", client_id: "my-app")
      :ok = KubeMQ.Client.send_event(client, %KubeMQ.Event{channel: "test", body: "hello"})
      KubeMQ.Client.close(client)

  ## Supervision

      children = [
        {KubeMQ.Client, address: "localhost:50000", client_id: "my-app", name: MyApp.KubeMQ}
      ]
      Supervisor.start_link(children, strategy: :one_for_one)

  ## Configuration

  See `KubeMQ.Config` for the full NimbleOptions schema. All timeouts are in milliseconds.

  ## Architecture Note

  The Client GenServer serializes all operations through a single mailbox. This is the standard
  OTP pattern for state management and connection lifecycle. For high-throughput scenarios, use
  the streaming APIs (EventStreamHandle, EventStoreStreamHandle, QueueUpstreamHandle) which
  bypass the Client GenServer and communicate directly with the gRPC transport.
  """

  use GenServer
  require Logger

  alias KubeMQ.{
    Channels,
    ChannelInfo,
    Connection,
    Config,
    Error,
    RetryPolicy,
    ServerInfo,
    Subscription,
    UUID,
    Validation
  }

  @type t :: GenServer.server()
  @type channel_type :: :events | :events_store | :commands | :queries | :queues

  @gen_call_buffer 5_000

  defstruct [
    :connection,
    :client_id,
    :transport,
    :tree,
    :default_channel,
    :default_cache_ttl,
    :retry_policy,
    :config
  ]

  # ===========================================================================
  # Lifecycle
  # ===========================================================================

  @doc """
  Start a KubeMQ client process linked to the caller.

  ## Options

  See `KubeMQ.Config` for the full schema. Required: `:client_id`.

  ## Examples

      {:ok, client} = KubeMQ.Client.start_link(
        address: "localhost:50000",
        client_id: "my-app"
      )
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, client_opts} = extract_gen_opts(opts)
    GenServer.start_link(__MODULE__, client_opts, gen_opts)
  end

  @doc """
  Returns a child specification for use in supervision trees.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Returns `true` if the underlying connection is in the `:ready` state.
  """
  @spec connected?(t()) :: boolean()
  def connected?(client), do: GenServer.call(client, :connected?)

  @doc """
  Returns the current connection state atom.
  """
  @spec connection_state(t()) :: :connecting | :ready | :reconnecting | :closed
  def connection_state(client), do: GenServer.call(client, :connection_state)

  @doc """
  Ping the KubeMQ server and return server information.
  """
  @spec ping(t()) :: {:ok, ServerInfo.t()} | {:error, Error.t()}
  def ping(client), do: GenServer.call(client, :ping)

  @doc """
  Ping the KubeMQ server, raising on failure.
  """
  @spec ping!(t()) :: ServerInfo.t()
  def ping!(client), do: bang!(ping(client))

  @doc """
  Close the client, stopping all subscriptions and the underlying connection.
  """
  @spec close(t()) :: :ok
  def close(client), do: GenServer.stop(client, :normal)

  # ===========================================================================
  # Events (Pub/Sub)
  # ===========================================================================

  @doc """
  Send a single event (fire-and-forget).
  """
  @spec send_event(t(), KubeMQ.Event.t()) :: :ok | {:error, Error.t()}
  def send_event(client, event), do: GenServer.call(client, {:send_event, event})

  @doc """
  Send a single event, raising on failure.
  """
  @spec send_event!(t(), KubeMQ.Event.t()) :: :ok
  def send_event!(client, event), do: bang_ok!(send_event(client, event))

  @doc """
  Open a bidirectional event stream. Returns a handle for sending events.
  """
  @spec send_event_stream(t()) :: {:ok, KubeMQ.EventStreamHandle.t()} | {:error, Error.t()}
  def send_event_stream(client), do: GenServer.call(client, :send_event_stream)

  @doc """
  Open a bidirectional event stream, raising on failure.
  """
  @spec send_event_stream!(t()) :: KubeMQ.EventStreamHandle.t()
  def send_event_stream!(client), do: bang!(send_event_stream(client))

  @doc """
  Subscribe to events on a channel.

  ## Options

    * `:group` - Consumer group name
    * `:on_event` - Callback `fn %EventReceive{} -> :ok end`
    * `:on_error` - Error callback `fn %Error{} -> :ok end`
    * `:notify` - PID to receive `{:kubemq_event, event}` messages
  """
  @spec subscribe_to_events(t(), String.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, Error.t()}
  def subscribe_to_events(client, channel, opts \\ []),
    do: GenServer.call(client, {:subscribe_to_events, channel, opts})

  @doc """
  Subscribe to events, raising on failure.
  """
  @spec subscribe_to_events!(t(), String.t(), keyword()) :: Subscription.t()
  def subscribe_to_events!(client, channel, opts \\ []),
    do: bang!(subscribe_to_events(client, channel, opts))

  # ===========================================================================
  # Events Store (Persistent Pub/Sub)
  # ===========================================================================

  @doc """
  Send a persistent event to Events Store.
  """
  @spec send_event_store(t(), KubeMQ.EventStore.t()) ::
          {:ok, KubeMQ.EventStoreResult.t()} | {:error, Error.t()}
  def send_event_store(client, event), do: GenServer.call(client, {:send_event_store, event})

  @doc """
  Send a persistent event, raising on failure.
  """
  @spec send_event_store!(t(), KubeMQ.EventStore.t()) :: KubeMQ.EventStoreResult.t()
  def send_event_store!(client, event), do: bang!(send_event_store(client, event))

  @doc """
  Open a bidirectional event store stream with awaitable confirmation.
  """
  @spec send_event_store_stream(t()) ::
          {:ok, KubeMQ.EventStoreStreamHandle.t()} | {:error, Error.t()}
  def send_event_store_stream(client), do: GenServer.call(client, :send_event_store_stream)

  @doc """
  Subscribe to Events Store on a channel.

  ## Options

    * `:start_at` - **Required.** Start position (see `KubeMQ.start_position()`)
    * `:group` - Consumer group name
    * `:on_event` - Callback `fn %EventStoreReceive{} -> :ok end`
    * `:on_error` - Error callback
    * `:notify` - PID to receive `{:kubemq_event_store, event}` messages
  """
  @spec subscribe_to_events_store(t(), String.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, Error.t()}
  def subscribe_to_events_store(client, channel, opts \\ []),
    do: GenServer.call(client, {:subscribe_to_events_store, channel, opts})

  # ===========================================================================
  # Commands (RPC)
  # ===========================================================================

  @doc """
  Send a command and wait for a response.
  """
  @spec send_command(t(), KubeMQ.Command.t()) ::
          {:ok, KubeMQ.CommandResponse.t()} | {:error, Error.t()}
  def send_command(client, command) do
    timeout = Map.get(command, :timeout) || 10_000
    GenServer.call(client, {:send_command, command}, timeout + @gen_call_buffer)
  end

  @doc """
  Send a command, raising on failure.
  """
  @spec send_command!(t(), KubeMQ.Command.t()) :: KubeMQ.CommandResponse.t()
  def send_command!(client, command), do: bang!(send_command(client, command))

  @doc """
  Subscribe to incoming commands on a channel.

  ## Options

    * `:group` - Consumer group name
    * `:on_command` - **Required.** `fn %CommandReceive{} -> %CommandReply{} end`
    * `:on_error` - Error callback
  """
  @spec subscribe_to_commands(t(), String.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, Error.t()}
  def subscribe_to_commands(client, channel, opts \\ []),
    do: GenServer.call(client, {:subscribe_to_commands, channel, opts})

  @doc """
  Subscribe to commands, raising on failure.
  """
  @spec subscribe_to_commands!(t(), String.t(), keyword()) :: Subscription.t()
  def subscribe_to_commands!(client, channel, opts \\ []),
    do: bang!(subscribe_to_commands(client, channel, opts))

  @doc """
  Send a command response (for manual response handling).
  """
  @spec send_command_response(t(), KubeMQ.CommandReply.t()) :: :ok | {:error, Error.t()}
  def send_command_response(client, reply),
    do: GenServer.call(client, {:send_command_response, reply})

  # ===========================================================================
  # Queries (RPC with Cache)
  # ===========================================================================

  @doc """
  Send a query and wait for a response.
  """
  @spec send_query(t(), KubeMQ.Query.t()) ::
          {:ok, KubeMQ.QueryResponse.t()} | {:error, Error.t()}
  def send_query(client, query) do
    timeout = Map.get(query, :timeout) || 10_000
    GenServer.call(client, {:send_query, query}, timeout + @gen_call_buffer)
  end

  @doc """
  Send a query, raising on failure.
  """
  @spec send_query!(t(), KubeMQ.Query.t()) :: KubeMQ.QueryResponse.t()
  def send_query!(client, query), do: bang!(send_query(client, query))

  @doc """
  Subscribe to incoming queries on a channel.

  ## Options

    * `:group` - Consumer group name
    * `:on_query` - **Required.** `fn %QueryReceive{} -> %QueryReply{} end`
    * `:on_error` - Error callback
  """
  @spec subscribe_to_queries(t(), String.t(), keyword()) ::
          {:ok, Subscription.t()} | {:error, Error.t()}
  def subscribe_to_queries(client, channel, opts \\ []),
    do: GenServer.call(client, {:subscribe_to_queries, channel, opts})

  @doc """
  Subscribe to queries, raising on failure.
  """
  @spec subscribe_to_queries!(t(), String.t(), keyword()) :: Subscription.t()
  def subscribe_to_queries!(client, channel, opts \\ []),
    do: bang!(subscribe_to_queries(client, channel, opts))

  @doc """
  Send a query response (for manual response handling).
  """
  @spec send_query_response(t(), KubeMQ.QueryReply.t()) :: :ok | {:error, Error.t()}
  def send_query_response(client, reply),
    do: GenServer.call(client, {:send_query_response, reply})

  # ===========================================================================
  # Queues — Simple API
  # ===========================================================================

  @doc """
  Send a single queue message.
  """
  @spec send_queue_message(t(), KubeMQ.QueueMessage.t()) ::
          {:ok, KubeMQ.QueueSendResult.t()} | {:error, Error.t()}
  def send_queue_message(client, msg),
    do: GenServer.call(client, {:send_queue_message, msg})

  @doc """
  Send a single queue message, raising on failure.
  """
  @spec send_queue_message!(t(), KubeMQ.QueueMessage.t()) :: KubeMQ.QueueSendResult.t()
  def send_queue_message!(client, msg), do: bang!(send_queue_message(client, msg))

  @doc """
  Send a batch of queue messages.
  """
  @spec send_queue_messages(t(), [KubeMQ.QueueMessage.t()]) ::
          {:ok, KubeMQ.QueueBatchResult.t()} | {:error, Error.t()}
  def send_queue_messages(client, msgs),
    do: GenServer.call(client, {:send_queue_messages, msgs})

  @doc """
  Send a batch of queue messages, raising on failure.
  """
  @spec send_queue_messages!(t(), [KubeMQ.QueueMessage.t()]) :: KubeMQ.QueueBatchResult.t()
  def send_queue_messages!(client, msgs), do: bang!(send_queue_messages(client, msgs))

  @doc """
  Receive queue messages (pull mode).

  ## Options

    * `:max_messages` - Max messages to receive (1–1024, default 1)
    * `:wait_timeout` - Wait timeout in ms (default 5_000)
    * `:is_peek` - Peek without consuming (default false)
  """
  @spec receive_queue_messages(t(), String.t(), keyword()) ::
          {:ok, KubeMQ.QueueReceiveResult.t()} | {:error, Error.t()}
  def receive_queue_messages(client, channel, opts \\ []) do
    wait = Keyword.get(opts, :wait_timeout, 5_000)
    GenServer.call(client, {:receive_queue_messages, channel, opts}, wait + @gen_call_buffer)
  end

  @doc """
  Acknowledge all messages on a queue channel.

  ## Options

    * `:wait_timeout` - Wait timeout in ms (default 5_000)
  """
  @spec ack_all_queue_messages(t(), String.t(), keyword()) ::
          {:ok, KubeMQ.QueueAckAllResult.t()} | {:error, Error.t()}
  def ack_all_queue_messages(client, channel, opts \\ []) do
    wait = Keyword.get(opts, :wait_timeout, 5_000)
    GenServer.call(client, {:ack_all_queue_messages, channel, opts}, wait + @gen_call_buffer)
  end

  # ===========================================================================
  # Queues — Stream API
  # ===========================================================================

  @doc """
  Open a queue upstream (send) stream.
  """
  @spec queue_upstream(t()) ::
          {:ok, KubeMQ.QueueUpstreamHandle.t()} | {:error, Error.t()}
  def queue_upstream(client), do: GenServer.call(client, :queue_upstream)

  @doc """
  Poll a queue for messages via the downstream stream API.

  ## Options

    * `:channel` - **Required.** Queue channel name
    * `:max_items` - Max items (1–1024, default 1)
    * `:wait_timeout` - Wait timeout in ms (default 5_000)
    * `:auto_ack` - Auto-acknowledge (default false)
  """
  @spec poll_queue(t(), keyword()) :: {:ok, KubeMQ.PollResponse.t()} | {:error, Error.t()}
  def poll_queue(client, opts) do
    wait = Keyword.get(opts, :wait_timeout, 5_000)
    GenServer.call(client, {:poll_queue, opts}, wait + @gen_call_buffer)
  end

  # ===========================================================================
  # Channel Management — Generic
  # ===========================================================================

  @doc """
  Create a channel of the specified type.
  """
  @spec create_channel(t(), String.t(), channel_type()) :: :ok | {:error, Error.t()}
  def create_channel(client, name, type),
    do: GenServer.call(client, {:create_channel, name, type})

  @doc """
  Delete a channel of the specified type.
  """
  @spec delete_channel(t(), String.t(), channel_type()) :: :ok | {:error, Error.t()}
  def delete_channel(client, name, type),
    do: GenServer.call(client, {:delete_channel, name, type})

  @doc """
  List channels of the specified type, optionally filtered by search pattern.
  """
  @spec list_channels(t(), channel_type(), String.t()) ::
          {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_channels(client, type, search \\ ""),
    do: GenServer.call(client, {:list_channels, type, search})

  @doc """
  Purge all messages from a queue channel (ack all pending messages).
  """
  @spec purge_queue_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def purge_queue_channel(client, name),
    do: GenServer.call(client, {:purge_queue_channel, name})

  # ===========================================================================
  # Channel Management — Convenience Aliases
  # ===========================================================================

  @doc "Create an events channel."
  @spec create_events_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_events_channel(client, name), do: create_channel(client, name, :events)

  @doc "Create an events store channel."
  @spec create_events_store_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_events_store_channel(client, name), do: create_channel(client, name, :events_store)

  @doc "Create a commands channel."
  @spec create_commands_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_commands_channel(client, name), do: create_channel(client, name, :commands)

  @doc "Create a queries channel."
  @spec create_queries_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_queries_channel(client, name), do: create_channel(client, name, :queries)

  @doc "Create a queues channel."
  @spec create_queues_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_queues_channel(client, name), do: create_channel(client, name, :queues)

  @doc "Delete an events channel."
  @spec delete_events_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_events_channel(client, name), do: delete_channel(client, name, :events)

  @doc "Delete an events store channel."
  @spec delete_events_store_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_events_store_channel(client, name), do: delete_channel(client, name, :events_store)

  @doc "Delete a commands channel."
  @spec delete_commands_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_commands_channel(client, name), do: delete_channel(client, name, :commands)

  @doc "Delete a queries channel."
  @spec delete_queries_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_queries_channel(client, name), do: delete_channel(client, name, :queries)

  @doc "Delete a queues channel."
  @spec delete_queues_channel(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete_queues_channel(client, name), do: delete_channel(client, name, :queues)

  @doc "List events channels."
  @spec list_events_channels(t(), String.t()) :: {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_events_channels(client, search \\ ""), do: list_channels(client, :events, search)

  @doc "List events store channels."
  @spec list_events_store_channels(t(), String.t()) ::
          {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_events_store_channels(client, search \\ ""),
    do: list_channels(client, :events_store, search)

  @doc "List commands channels."
  @spec list_commands_channels(t(), String.t()) :: {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_commands_channels(client, search \\ ""), do: list_channels(client, :commands, search)

  @doc "List queries channels."
  @spec list_queries_channels(t(), String.t()) :: {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_queries_channels(client, search \\ ""), do: list_channels(client, :queries, search)

  @doc "List queues channels."
  @spec list_queues_channels(t(), String.t()) :: {:ok, [ChannelInfo.t()]} | {:error, Error.t()}
  def list_queues_channels(client, search \\ ""), do: list_channels(client, :queues, search)

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl GenServer
  def init(opts) do
    case Config.validate(opts) do
      {:ok, validated} ->
        client_id = Keyword.fetch!(validated, :client_id)
        transport = Keyword.get(validated, :transport, KubeMQ.Transport.GRPC)

        conn_opts =
          validated
          |> Keyword.put(:transport, transport)
          |> Keyword.delete(:name)

        case Connection.start_link(conn_opts) do
          {:ok, conn} ->
            Process.link(conn)

            case KubeMQ.Supervisor.start_client_tree(client_id) do
              {:ok, tree} ->
                retry_policy =
                  RetryPolicy.from_config(Keyword.get(validated, :retry_policy, []))

                state = %__MODULE__{
                  connection: conn,
                  client_id: client_id,
                  transport: transport,
                  tree: tree,
                  default_channel: Keyword.get(validated, :default_channel),
                  default_cache_ttl: Keyword.get(validated, :default_cache_ttl, 900_000),
                  retry_policy: retry_policy,
                  config: validated
                }

                {:ok, state}

              {:error, reason} ->
                Connection.close(conn)
                {:stop, reason}
            end

          {:error, reason} ->
            {:stop, reason}
        end

      {:error, %NimbleOptions.ValidationError{} = error} ->
        {:stop, Error.validation(Exception.message(error))}
    end
  end

  # --- Lifecycle ---

  @impl GenServer
  def handle_call(:connected?, _from, state) do
    {:reply, Connection.connected?(state.connection), state}
  end

  def handle_call(:connection_state, _from, state) do
    {:reply, Connection.connection_state(state.connection), state}
  end

  def handle_call(:ping, _from, state) do
    result =
      with_channel(state, "ping", nil, fn channel ->
        case state.transport.ping(channel) do
          {:ok, info} -> {:ok, ServerInfo.from_ping_result(info)}
          {:error, reason} -> {:error, wrap_error(reason, "ping")}
        end
      end)

    {:reply, result, state}
  end

  # --- Events ---

  def handle_call({:send_event, event}, _from, state) do
    result =
      with_channel(state, "send_event", Map.get(event, :channel), fn channel ->
        KubeMQ.Events.Publisher.send_event(
          channel,
          state.transport,
          event,
          state.client_id
        )
      end)

    {:reply, result, state}
  end

  def handle_call(:send_event_stream, _from, state) do
    result =
      with_channel(state, "send_event_stream", nil, fn channel ->
        handle_opts = [
          grpc_channel: channel,
          client_id: state.client_id,
          transport: state.transport
        ]

        case start_supervised_child(state, KubeMQ.EventStreamHandle, handle_opts) do
          {:ok, pid} -> {:ok, pid}
          {:error, reason} -> {:error, wrap_error(reason, "send_event_stream")}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:subscribe_to_events, channel, opts}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(channel, allow_wildcards: true),
           {:ok, grpc_channel} <- Connection.get_channel(state.connection) do
        sub_opts = [
          channel: channel,
          client_id: state.client_id,
          transport: state.transport,
          grpc_channel: grpc_channel,
          group: Keyword.get(opts, :group, ""),
          on_event: Keyword.get(opts, :on_event),
          on_error: Keyword.get(opts, :on_error),
          notify: Keyword.get(opts, :notify),
          conn: state.connection
        ]

        case start_supervised_child(state, KubeMQ.Events.Subscriber, sub_opts) do
          {:ok, pid} -> {:ok, Subscription.new(pid)}
          {:error, reason} -> {:error, wrap_error(reason, "subscribe_to_events", channel)}
        end
      else
        {:error, _} = err -> err
      end

    {:reply, result, state}
  end

  # --- Events Store ---

  def handle_call({:send_event_store, event}, _from, state) do
    result =
      with_channel(state, "send_event_store", Map.get(event, :channel), fn channel ->
        KubeMQ.EventsStore.Publisher.send_event_store(
          channel,
          state.transport,
          event,
          state.client_id
        )
      end)

    {:reply, result, state}
  end

  def handle_call(:send_event_store_stream, _from, state) do
    result =
      with_channel(state, "send_event_store_stream", nil, fn channel ->
        handle_opts = [
          grpc_channel: channel,
          client_id: state.client_id,
          transport: state.transport
        ]

        case start_supervised_child(state, KubeMQ.EventStoreStreamHandle, handle_opts) do
          {:ok, pid} -> {:ok, pid}
          {:error, reason} -> {:error, wrap_error(reason, "send_event_store_stream")}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:subscribe_to_events_store, channel, opts}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(channel),
           :ok <- Validation.validate_start_position(Keyword.get(opts, :start_at)),
           {:ok, grpc_channel} <- Connection.get_channel(state.connection) do
        {store_type_data, store_type_value} =
          resolve_start_position(Keyword.fetch!(opts, :start_at))

        sub_opts = [
          channel: channel,
          client_id: state.client_id,
          transport: state.transport,
          grpc_channel: grpc_channel,
          group: Keyword.get(opts, :group, ""),
          subscribe_type: :events_store,
          start_at: Keyword.fetch!(opts, :start_at),
          events_store_type_data: store_type_data,
          events_store_type_value: store_type_value,
          on_event: Keyword.get(opts, :on_event),
          on_error: Keyword.get(opts, :on_error),
          notify: Keyword.get(opts, :notify),
          conn: state.connection
        ]

        case start_supervised_child(state, KubeMQ.EventsStore.Subscriber, sub_opts) do
          {:ok, pid} -> {:ok, Subscription.new(pid)}
          {:error, reason} -> {:error, wrap_error(reason, "subscribe_to_events_store", channel)}
        end
      else
        {:error, _} = err -> err
      end

    {:reply, result, state}
  end

  # --- Commands ---

  def handle_call({:send_command, command}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(Map.get(command, :channel)),
           :ok <-
             Validation.validate_content(Map.get(command, :metadata), Map.get(command, :body)),
           :ok <- Validation.validate_timeout(Map.get(command, :timeout, 0)) do
        with_channel(state, "send_command", Map.get(command, :channel), fn channel ->
          request = %{
            id: UUID.ensure(Map.get(command, :id)),
            request_type: :command,
            client_id: Map.get(command, :client_id) || state.client_id,
            channel: Map.get(command, :channel),
            metadata: Map.get(command, :metadata, ""),
            body: Map.get(command, :body, ""),
            reply_channel: "",
            timeout: Map.get(command, :timeout, 10_000),
            cache_key: "",
            cache_ttl: 0,
            tags: Map.get(command, :tags, %{})
          }

          case state.transport.send_request(channel, request) do
            {:ok, response} ->
              {:ok, KubeMQ.CommandResponse.from_response(response)}

            {:error, reason} ->
              {:error, wrap_error(reason, "send_command", Map.get(command, :channel))}
          end
        end)
      end

    {:reply, result, state}
  end

  def handle_call({:subscribe_to_commands, channel, opts}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(channel),
           {:ok, grpc_channel} <- Connection.get_channel(state.connection) do
        sub_opts = [
          channel: channel,
          client_id: state.client_id,
          transport: state.transport,
          grpc_channel: grpc_channel,
          group: Keyword.get(opts, :group, ""),
          subscribe_type: :commands,
          on_command: Keyword.get(opts, :on_command),
          on_error: Keyword.get(opts, :on_error),
          conn: state.connection
        ]

        case start_supervised_child(state, KubeMQ.Commands.Subscriber, sub_opts) do
          {:ok, pid} -> {:ok, Subscription.new(pid)}
          {:error, reason} -> {:error, wrap_error(reason, "subscribe_to_commands", channel)}
        end
      else
        {:error, _} = err -> err
      end

    {:reply, result, state}
  end

  def handle_call({:send_command_response, reply}, _from, state) do
    result =
      with :ok <- Validation.validate_response_fields(reply) do
        with_channel(state, "send_command_response", nil, fn channel ->
          response = %{
            request_id: Map.get(reply, :request_id, ""),
            reply_channel: Map.get(reply, :response_to, ""),
            client_id: Map.get(reply, :client_id) || state.client_id,
            metadata: Map.get(reply, :metadata, ""),
            body: Map.get(reply, :body, ""),
            executed: Map.get(reply, :executed, false),
            error: Map.get(reply, :error, ""),
            tags: Map.get(reply, :tags, %{})
          }

          case state.transport.send_response(channel, response) do
            :ok -> :ok
            {:error, reason} -> {:error, wrap_error(reason, "send_command_response")}
          end
        end)
      end

    {:reply, result, state}
  end

  # --- Queries ---

  def handle_call({:send_query, query}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(Map.get(query, :channel)),
           :ok <- Validation.validate_content(Map.get(query, :metadata), Map.get(query, :body)),
           :ok <- Validation.validate_timeout(Map.get(query, :timeout, 0)),
           :ok <-
             Validation.validate_cache(
               Map.get(query, :cache_key),
               Map.get(query, :cache_ttl)
             ) do
        with_channel(state, "send_query", Map.get(query, :channel), fn channel ->
          cache_ttl_ms = Map.get(query, :cache_ttl) || state.default_cache_ttl
          cache_ttl_seconds = div(cache_ttl_ms, 1_000)

          request = %{
            id: UUID.ensure(Map.get(query, :id)),
            request_type: :query,
            client_id: Map.get(query, :client_id) || state.client_id,
            channel: Map.get(query, :channel),
            metadata: Map.get(query, :metadata, ""),
            body: Map.get(query, :body, ""),
            reply_channel: "",
            timeout: Map.get(query, :timeout, 10_000),
            cache_key: Map.get(query, :cache_key, ""),
            cache_ttl: if(Map.get(query, :cache_key), do: cache_ttl_seconds, else: 0),
            tags: Map.get(query, :tags, %{})
          }

          case state.transport.send_request(channel, request) do
            {:ok, response} ->
              {:ok, KubeMQ.QueryResponse.from_response(response)}

            {:error, reason} ->
              {:error, wrap_error(reason, "send_query", Map.get(query, :channel))}
          end
        end)
      end

    {:reply, result, state}
  end

  def handle_call({:subscribe_to_queries, channel, opts}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(channel),
           {:ok, grpc_channel} <- Connection.get_channel(state.connection) do
        sub_opts = [
          channel: channel,
          client_id: state.client_id,
          transport: state.transport,
          grpc_channel: grpc_channel,
          group: Keyword.get(opts, :group, ""),
          subscribe_type: :queries,
          on_query: Keyword.get(opts, :on_query),
          on_error: Keyword.get(opts, :on_error),
          conn: state.connection
        ]

        case start_supervised_child(state, KubeMQ.Queries.Subscriber, sub_opts) do
          {:ok, pid} -> {:ok, Subscription.new(pid)}
          {:error, reason} -> {:error, wrap_error(reason, "subscribe_to_queries", channel)}
        end
      else
        {:error, _} = err -> err
      end

    {:reply, result, state}
  end

  def handle_call({:send_query_response, reply}, _from, state) do
    result =
      with :ok <- Validation.validate_response_fields(reply) do
        with_channel(state, "send_query_response", nil, fn channel ->
          response = %{
            request_id: Map.get(reply, :request_id, ""),
            reply_channel: Map.get(reply, :response_to, ""),
            client_id: Map.get(reply, :client_id) || state.client_id,
            metadata: Map.get(reply, :metadata, ""),
            body: Map.get(reply, :body, ""),
            executed: Map.get(reply, :executed, false),
            error: Map.get(reply, :error, ""),
            cache_hit: Map.get(reply, :cache_hit, false),
            tags: Map.get(reply, :tags, %{})
          }

          case state.transport.send_response(channel, response) do
            :ok -> :ok
            {:error, reason} -> {:error, wrap_error(reason, "send_query_response")}
          end
        end)
      end

    {:reply, result, state}
  end

  # --- Queues Simple ---

  def handle_call({:send_queue_message, msg}, _from, state) do
    result =
      with :ok <- Validation.validate_channel(Map.get(msg, :channel)),
           :ok <- Validation.validate_content(Map.get(msg, :metadata), Map.get(msg, :body)) do
        with_channel(state, "send_queue_message", Map.get(msg, :channel), fn channel ->
          request = build_queue_request(msg, state.client_id)

          case state.transport.send_queue_message(channel, request) do
            {:ok, result} ->
              {:ok, KubeMQ.QueueSendResult.from_transport(result)}

            {:error, reason} ->
              {:error, wrap_error(reason, "send_queue_message", Map.get(msg, :channel))}
          end
        end)
      end

    {:reply, result, state}
  end

  def handle_call({:send_queue_messages, msgs}, _from, state) when is_list(msgs) do
    result =
      with :ok <- validate_queue_batch(msgs) do
        with_channel(state, "send_queue_messages", nil, fn channel ->
          batch = %{
            batch_id: UUID.generate(),
            messages: Enum.map(msgs, &build_queue_request(&1, state.client_id))
          }

          case state.transport.send_queue_messages_batch(channel, batch) do
            {:ok, batch_result} ->
              {:ok, KubeMQ.QueueBatchResult.from_transport(batch_result)}

            {:error, reason} ->
              {:error, wrap_error(reason, "send_queue_messages")}
          end
        end)
      end

    {:reply, result, state}
  end

  def handle_call({:receive_queue_messages, channel, opts}, _from, state) do
    max_messages = Keyword.get(opts, :max_messages, 1)
    wait_timeout_ms = Keyword.get(opts, :wait_timeout, 5_000)
    wait_time_seconds = div(wait_timeout_ms, 1_000)
    is_peek = Keyword.get(opts, :is_peek, false)

    result =
      with :ok <- Validation.validate_channel(channel),
           :ok <- Validation.validate_max_messages(max_messages),
           :ok <- Validation.validate_wait_timeout(wait_timeout_ms) do
        with_channel(state, "receive_queue_messages", channel, fn grpc_channel ->
          request = %{
            request_id: UUID.generate(),
            client_id: state.client_id,
            channel: channel,
            max_messages: max_messages,
            wait_time_seconds: max(wait_time_seconds, 1),
            is_peek: is_peek
          }

          case state.transport.receive_queue_messages(grpc_channel, request) do
            {:ok, recv_result} ->
              {:ok, KubeMQ.QueueReceiveResult.from_transport(recv_result)}

            {:error, reason} ->
              {:error, wrap_error(reason, "receive_queue_messages", channel)}
          end
        end)
      end

    {:reply, result, state}
  end

  def handle_call({:ack_all_queue_messages, channel, opts}, _from, state) do
    wait_timeout_ms = Keyword.get(opts, :wait_timeout, 5_000)
    wait_time_seconds = div(wait_timeout_ms, 1_000)

    result =
      with :ok <- Validation.validate_channel(channel) do
        with_channel(state, "ack_all_queue_messages", channel, fn grpc_channel ->
          request = %{
            request_id: UUID.generate(),
            client_id: state.client_id,
            channel: channel,
            wait_time_seconds: max(wait_time_seconds, 1)
          }

          case state.transport.ack_all_queue_messages(grpc_channel, request) do
            {:ok, ack_result} ->
              {:ok, KubeMQ.QueueAckAllResult.from_transport(ack_result)}

            {:error, reason} ->
              {:error, wrap_error(reason, "ack_all_queue_messages", channel)}
          end
        end)
      end

    {:reply, result, state}
  end

  # --- Queues Stream ---

  def handle_call(:queue_upstream, _from, state) do
    result =
      with_channel(state, "queue_upstream", nil, fn channel ->
        handle_opts = [
          grpc_channel: channel,
          client_id: state.client_id,
          transport: state.transport
        ]

        case start_supervised_child(state, KubeMQ.QueueUpstreamHandle, handle_opts) do
          {:ok, pid} -> {:ok, pid}
          {:error, reason} -> {:error, wrap_error(reason, "queue_upstream")}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:poll_queue, opts}, _from, state) do
    channel = Keyword.fetch!(opts, :channel)
    max_items = Keyword.get(opts, :max_items, 1)
    wait_timeout = Keyword.get(opts, :wait_timeout, 5_000)
    auto_ack = Keyword.get(opts, :auto_ack, false)

    result =
      with :ok <- Validation.validate_channel(channel),
           :ok <- Validation.validate_max_messages(max_items),
           :ok <- Validation.validate_wait_timeout(wait_timeout) do
        with_channel(state, "poll_queue", channel, fn grpc_channel ->
          downstream_opts = [
            grpc_channel: grpc_channel,
            client_id: state.client_id,
            transport: state.transport
          ]

          case KubeMQ.Queues.Downstream.start_link(downstream_opts) do
            {:ok, downstream_pid} ->
              # Unlink so downstream lifecycle doesn't affect the client
              Process.unlink(downstream_pid)

              poll_opts = [
                channel: channel,
                max_items: max_items,
                wait_timeout: wait_timeout,
                auto_ack: auto_ack
              ]

              KubeMQ.Queues.Downstream.poll(downstream_pid, poll_opts)

            {:error, reason} ->
              {:error, wrap_error(reason, "poll_queue", channel)}
          end
        end)
      end

    {:reply, result, state}
  end

  # --- Channel Management ---

  def handle_call({:create_channel, name, type}, _from, state) do
    result =
      with_channel(state, "create_channel", name, fn channel ->
        Channels.Manager.create_channel(channel, state.transport, state.client_id, name, type)
      end)

    {:reply, result, state}
  end

  def handle_call({:delete_channel, name, type}, _from, state) do
    result =
      with_channel(state, "delete_channel", name, fn channel ->
        Channels.Manager.delete_channel(channel, state.transport, state.client_id, name, type)
      end)

    {:reply, result, state}
  end

  def handle_call({:list_channels, type, search}, _from, state) do
    result =
      with_channel(state, "list_channels", nil, fn channel ->
        Channels.Manager.list_channels(channel, state.transport, state.client_id, type, search)
      end)

    {:reply, result, state}
  end

  def handle_call({:purge_queue_channel, name}, _from, state) do
    result =
      with_channel(state, "purge_queue_channel", name, fn channel ->
        Channels.Manager.purge_queue_channel(channel, state.transport, state.client_id, name)
      end)

    {:reply, result, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.info("[KubeMQ.Client] Terminating: #{inspect(reason)}")

    if state.connection && Process.alive?(state.connection) do
      Connection.close(state.connection)
    end

    if state.tree do
      KubeMQ.Supervisor.stop_client_tree(state.tree)
    end

    :ok
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp with_channel(state, operation, channel, fun) do
    emit_telemetry(operation, :start, %{system_time: System.system_time()}, %{
      operation: operation,
      channel: channel,
      client_id: state.client_id
    })

    retry_policy = state.retry_policy || %RetryPolicy{}

    result = execute_with_retry(state, operation, channel, fun, retry_policy, 0)

    phase = if match?({:error, _}, result), do: :exception, else: :stop

    emit_telemetry(operation, phase, %{duration: System.monotonic_time()}, %{
      operation: operation,
      channel: channel,
      client_id: state.client_id
    })

    result
  end

  defp execute_with_retry(state, operation, channel, fun, %RetryPolicy{} = retry_policy, attempt) do
    case Connection.get_channel(state.connection) do
      {:ok, grpc_channel} ->
        case fun.(grpc_channel) do
          {:error, %Error{retryable?: true} = error} ->
            maybe_retry(state, operation, channel, fun, retry_policy, attempt, error)

          {:error, {_code, _msg} = reason} ->
            error = wrap_error(reason, operation, channel)

            if error.retryable? do
              maybe_retry(state, operation, channel, fun, retry_policy, attempt, error)
            else
              {:error, error}
            end

          other ->
            other
        end

      {:error, reason} ->
        {:error, wrap_error(reason, operation, channel)}
    end
  end

  defp maybe_retry(state, operation, channel, fun, %RetryPolicy{} = retry_policy, attempt, error) do
    if RetryPolicy.should_retry?(retry_policy, attempt) do
      delay = RetryPolicy.delay_for_attempt(retry_policy, attempt)
      Process.sleep(delay)
      execute_with_retry(state, operation, channel, fun, retry_policy, attempt + 1)
    else
      {:error, error}
    end
  end

  # Infrastructure for threading config timeouts to transport calls.
  # Available for use when transport behaviour is updated to accept opts.
  @doc false
  def transport_opts(state) do
    [
      send_timeout: Keyword.get(state.config, :send_timeout, 5_000),
      rpc_timeout: Keyword.get(state.config, :rpc_timeout, 10_000)
    ]
  end

  defp start_supervised_child(state, module, opts) do
    DynamicSupervisor.start_child(
      state.tree.subscription_supervisor,
      {module, opts}
    )
  end

  defp extract_gen_opts(opts) do
    {name, rest} = Keyword.pop(opts, :name)

    gen_opts =
      if name do
        [name: name]
      else
        []
      end

    {gen_opts, rest}
  end

  defp wrap_error(%Error{} = error, _operation, _channel), do: error
  defp wrap_error(%Error{} = error, _operation), do: error

  defp wrap_error(%{code: code, message: msg}, operation, channel) do
    %Error{code: code, message: msg, operation: operation, channel: channel}
  end

  defp wrap_error({code, msg}, operation, channel) when is_atom(code) do
    Error.from_grpc_status(code,
      operation: operation,
      channel: channel,
      message: msg
    )
  end

  defp wrap_error(reason, operation, channel) do
    Error.fatal("#{operation} failed: #{inspect(reason)}",
      operation: operation,
      channel: channel
    )
  end

  defp wrap_error(reason, operation), do: wrap_error(reason, operation, nil)

  defp bang!({:ok, result}), do: result
  defp bang!({:error, %Error{} = error}), do: raise(error)

  defp bang_ok!(:ok), do: :ok
  defp bang_ok!({:error, %Error{} = error}), do: raise(error)

  @telemetry_atoms %{
    "ping" => :ping,
    "send_event" => :send_event,
    "send_event_stream" => :send_event_stream,
    "subscribe_to_events" => :subscribe_to_events,
    "send_event_store" => :send_event_store,
    "send_event_store_stream" => :send_event_store_stream,
    "subscribe_to_events_store" => :subscribe_to_events_store,
    "send_command" => :send_command,
    "subscribe_to_commands" => :subscribe_to_commands,
    "send_command_response" => :send_command_response,
    "send_query" => :send_query,
    "subscribe_to_queries" => :subscribe_to_queries,
    "send_query_response" => :send_query_response,
    "send_queue_message" => :send_queue_message,
    "send_queue_messages" => :send_queue_messages,
    "receive_queue_messages" => :receive_queue_messages,
    "ack_all_queue_messages" => :ack_all_queue_messages,
    "queue_upstream" => :queue_upstream,
    "poll_queue" => :poll_queue,
    "create_channel" => :create_channel,
    "delete_channel" => :delete_channel,
    "list_channels" => :list_channels,
    "purge_queue_channel" => :purge_queue_channel
  }

  defp emit_telemetry(action, phase, measurements, metadata) do
    atom_action = Map.get(@telemetry_atoms, action, :unknown)
    :telemetry.execute([:kubemq, :client, atom_action, phase], measurements, metadata)
  rescue
    _ -> :ok
  end

  # --- Response Builders ---

  defp build_queue_request(msg, default_client_id) do
    policy = Map.get(msg, :policy)

    policy_map =
      if policy do
        %{
          expiration_seconds: Map.get(policy, :expiration_seconds, 0),
          delay_seconds: Map.get(policy, :delay_seconds, 0),
          max_receive_count: Map.get(policy, :max_receive_count, 0),
          max_receive_queue: Map.get(policy, :max_receive_queue, "")
        }
      end

    %{
      id: UUID.ensure(Map.get(msg, :id)),
      channel: Map.get(msg, :channel),
      metadata: Map.get(msg, :metadata, ""),
      body: Map.get(msg, :body, ""),
      client_id: Map.get(msg, :client_id) || default_client_id,
      tags: Map.get(msg, :tags, %{}),
      policy: policy_map
    }
  end

  defp resolve_start_position(:start_new_only), do: {:StartNewOnly, 0}
  defp resolve_start_position(:start_from_new), do: {:StartNewOnly, 0}
  defp resolve_start_position(:start_from_first), do: {:StartFromFirst, 0}
  defp resolve_start_position(:start_from_last), do: {:StartFromLast, 0}
  defp resolve_start_position({:start_at_sequence, seq}), do: {:StartAtSequence, seq}
  defp resolve_start_position({:start_at_time, time}), do: {:StartAtTime, time}

  defp resolve_start_position({:start_at_time_delta, delta_ms}) do
    {:StartAtTimeDelta, div(delta_ms, 1_000)}
  end

  defp validate_queue_batch([]), do: {:error, Error.validation("message batch cannot be empty")}

  defp validate_queue_batch(msgs) when is_list(msgs) do
    Enum.reduce_while(msgs, :ok, fn msg, :ok ->
      case Validation.validate_channel(Map.get(msg, :channel)) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end
end
