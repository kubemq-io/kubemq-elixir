defmodule KubeMQ.Connection do
  @moduledoc false

  use GenServer
  require Logger

  @type state :: :connecting | :ready | :reconnecting | :closed

  @type t :: %__MODULE__{
          address: String.t(),
          client_id: String.t(),
          channel: GRPC.Channel.t() | nil,
          state: state(),
          transport: module(),
          config: map(),
          buffer: :queue.queue(),
          buffer_size: non_neg_integer(),
          max_buffer_size: non_neg_integer(),
          subscriptions: [map()],
          reconnect_attempt: non_neg_integer(),
          reconnect_timer: reference() | nil,
          health_timer: reference() | nil,
          token_refresh_timer: reference() | nil,
          callbacks: map(),
          connect_task: {pid(), reference()} | nil
        }

  defstruct [
    :address,
    :client_id,
    :channel,
    :transport,
    :config,
    :reconnect_timer,
    :connect_task,
    :health_timer,
    :token_refresh_timer,
    state: :connecting,
    buffer: :queue.new(),
    buffer_size: 0,
    max_buffer_size: 1_000,
    subscriptions: [],
    reconnect_attempt: 0,
    callbacks: %{}
  ]

  @default_initial_delay 1_000
  @default_max_delay 30_000
  @default_multiplier 2.0
  @default_max_attempts 0
  @default_buffer_size 1_000
  @default_connection_timeout 10_000
  @health_check_interval 30_000

  # --- Public API ---

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, conn_opts} = extract_gen_opts(opts)
    GenServer.start_link(__MODULE__, conn_opts, gen_opts)
  end

  @spec get_channel(GenServer.server()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def get_channel(conn) do
    GenServer.call(conn, :get_channel)
  end

  @spec get_channel(GenServer.server(), timeout()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  def get_channel(conn, timeout) do
    GenServer.call(conn, :get_channel, timeout)
  end

  @spec connection_state(GenServer.server()) :: state()
  def connection_state(conn) do
    GenServer.call(conn, :connection_state)
  end

  @spec connected?(GenServer.server()) :: boolean()
  def connected?(conn) do
    connection_state(conn) == :ready
  end

  @spec close(GenServer.server()) :: :ok
  def close(conn) do
    GenServer.call(conn, :close)
  end

  @spec register_subscription(GenServer.server(), map()) :: :ok
  def register_subscription(conn, sub_info) do
    GenServer.cast(conn, {:register_subscription, sub_info})
  end

  @spec unregister_subscription(GenServer.server(), reference()) :: :ok
  def unregister_subscription(conn, sub_ref) do
    GenServer.cast(conn, {:unregister_subscription, sub_ref})
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    address = Keyword.fetch!(opts, :address)
    client_id = Keyword.fetch!(opts, :client_id)
    transport = Keyword.get(opts, :transport, KubeMQ.Transport.GRPC)
    max_buffer = Keyword.get(opts, :reconnect_buffer_size, @default_buffer_size)

    reconnect_policy = Keyword.get(opts, :reconnect_policy, [])

    callbacks = %{
      on_connected: Keyword.get(opts, :on_connected),
      on_disconnected: Keyword.get(opts, :on_disconnected),
      on_reconnecting: Keyword.get(opts, :on_reconnecting),
      on_reconnected: Keyword.get(opts, :on_reconnected),
      on_closed: Keyword.get(opts, :on_closed)
    }

    config = %{
      connection_timeout: Keyword.get(opts, :connection_timeout, @default_connection_timeout),
      reconnect_enabled: Keyword.get(reconnect_policy, :enabled, true),
      initial_delay: Keyword.get(reconnect_policy, :initial_delay, @default_initial_delay),
      max_delay: Keyword.get(reconnect_policy, :max_delay, @default_max_delay),
      multiplier: Keyword.get(reconnect_policy, :multiplier, @default_multiplier),
      max_attempts: Keyword.get(reconnect_policy, :max_attempts, @default_max_attempts),
      max_receive_size: Keyword.get(opts, :max_receive_size, 4_194_304),
      max_send_size: Keyword.get(opts, :max_send_size, 104_857_600),
      keepalive_time: max(Keyword.get(opts, :keepalive_time, 10_000), 5_000),
      keepalive_timeout: Keyword.get(opts, :keepalive_timeout, 5_000),
      tls: Keyword.get(opts, :tls),
      auth_token: Keyword.get(opts, :auth_token),
      credential_provider: Keyword.get(opts, :credential_provider)
    }

    state = %__MODULE__{
      address: address,
      client_id: client_id,
      transport: transport,
      config: config,
      max_buffer_size: max_buffer,
      callbacks: callbacks,
      state: :connecting
    }

    emit_telemetry(:connect, :start, %{system_time: System.system_time()}, %{
      address: address,
      client_id: client_id
    })

    {:ok, state, {:continue, :connect}}
  end

  @impl GenServer
  def handle_continue(:connect, %{state: :connecting} = state) do
    # Connect synchronously in handle_continue so the GRPC channel process
    # is linked to this GenServer (not a temporary task that dies immediately).
    result = do_connect(state.address, state.config)
    send(self(), {:connect_result, result})
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get_channel, from, state) do
    case state.state do
      :ready ->
        {:reply, {:ok, state.channel}, state}

      pending when pending in [:connecting, :reconnecting] ->
        case buffer_request_or_reject(state, from) do
          {:buffered, new_state} ->
            {:noreply, new_state}

          {:rejected, error} ->
            {:reply, {:error, error}, state}
        end

      :closed ->
        {:reply, {:error, closed_error()}, state}
    end
  end

  def handle_call(:connection_state, _from, state) do
    {:reply, state.state, state}
  end

  def handle_call(:close, _from, state) do
    new_state = do_close(state)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_cast({:register_subscription, sub_info}, state) do
    {:noreply, %{state | subscriptions: [sub_info | state.subscriptions]}}
  end

  def handle_cast({:unregister_subscription, sub_ref}, state) do
    subs = Enum.reject(state.subscriptions, fn sub -> sub.ref == sub_ref end)
    {:noreply, %{state | subscriptions: subs}}
  end

  @impl GenServer
  def handle_info({:connect_result, {:ok, channel}}, %{state: conn_state} = state)
      when conn_state in [:connecting, :reconnecting] do
    case health_check(state.transport, channel) do
      {:ok, _info} ->
        was_reconnecting = state.state == :reconnecting

        new_state =
          state
          |> transition(:ready, channel)
          |> flush_buffer()

        if was_reconnecting do
          invoke_callback(new_state.callbacks, :on_reconnected)
        else
          invoke_callback(new_state.callbacks, :on_connected)
        end

        emit_telemetry(:connect, :stop, %{duration: System.monotonic_time()}, %{
          address: state.address,
          client_id: state.client_id
        })

        if was_reconnecting do
          reestablish_subscriptions(new_state)
        end

        # H-8: Schedule periodic health check
        health_timer = schedule_health_check(new_state)
        new_state = %{new_state | health_timer: health_timer}

        # FIX-1/FIX-6: Kick off (or restart) periodic token refresh
        new_state = schedule_token_refresh(new_state)

        {:noreply, new_state}

      {:error, reason} ->
        Logger.warning("[KubeMQ.Connection] Health check failed: #{inspect(reason)}")

        if channel do
          GRPC.Stub.disconnect(channel)
        end

        if state.state == :connecting do
          {:noreply, schedule_reconnect(%{state | state: :reconnecting, reconnect_attempt: 0})}
        else
          {:noreply, schedule_reconnect(state)}
        end
    end
  end

  def handle_info({:connect_result, {:error, reason}}, %{state: conn_state} = state)
      when conn_state in [:connecting, :reconnecting] do
    Logger.warning("[KubeMQ.Connection] Connection failed: #{inspect(reason)}")

    emit_telemetry(:connect, :exception, %{duration: System.monotonic_time()}, %{
      address: state.address,
      client_id: state.client_id,
      reason: reason
    })

    if state.state == :connecting do
      invoke_callback(state.callbacks, :on_disconnected)
    end

    {:noreply, schedule_reconnect(%{state | state: :reconnecting})}
  end

  def handle_info({:connect_result, _}, state) do
    {:noreply, state}
  end

  def handle_info(:reconnect_tick, %{state: :reconnecting} = state) do
    max_attempts = state.config.max_attempts

    if max_attempts > 0 and state.reconnect_attempt >= max_attempts do
      Logger.error(
        "[KubeMQ.Connection] Max reconnection attempts (#{max_attempts}) reached, closing"
      )

      reject_buffer(state, max_attempts_error())
      new_state = do_close(state)
      {:noreply, new_state}
    else
      invoke_callback(state.callbacks, :on_reconnecting)
      # Connect synchronously to keep GRPC channel linked to this GenServer
      result = do_connect(state.address, state.config)
      send(self(), {:connect_result, result})

      {:noreply,
       %{
         state
         | reconnect_attempt: state.reconnect_attempt + 1,
           connect_task: nil,
           reconnect_timer: nil
       }}
    end
  end

  def handle_info(:reconnect_tick, state) do
    {:noreply, state}
  end

  def handle_info({:grpc_closed, _reason}, %{state: :ready} = state) do
    Logger.warning("[KubeMQ.Connection] gRPC connection lost, entering reconnection")
    invoke_callback(state.callbacks, :on_disconnected)

    {:noreply,
     schedule_reconnect(%{state | state: :reconnecting, channel: nil, reconnect_attempt: 0})}
  end

  def handle_info({:grpc_closed, _reason}, state) do
    {:noreply, state}
  end

  # M-16: Token refresh for long-lived connections
  def handle_info(:refresh_auth_token, %{state: :ready} = state) do
    case resolve_auth_token(state.config) do
      {:ok, token} when is_binary(token) and token != "" ->
        # grpc-elixir doesn't support header updates on live channels.
        # Schedule reconnect to pick up new token.
        Logger.info("[KubeMQ.Connection] Auth token refreshed, reconnecting to apply")

        {:noreply,
         schedule_reconnect(%{state | state: :reconnecting, channel: nil, reconnect_attempt: 0})}

      _ ->
        new_state = schedule_token_refresh(state)
        {:noreply, new_state}
    end
  end

  def handle_info(:refresh_auth_token, state), do: {:noreply, state}

  # H-8: Periodic health check
  def handle_info(:health_check, %{state: :ready} = state) do
    case health_check(state.transport, state.channel) do
      {:ok, _} ->
        timer = schedule_health_check(state)
        {:noreply, %{state | health_timer: timer}}

      {:error, reason} ->
        Logger.warning("[KubeMQ.Connection] Health check failed: #{inspect(reason)}")
        invoke_callback(state.callbacks, :on_disconnected)

        {:noreply,
         schedule_reconnect(%{state | state: :reconnecting, channel: nil, reconnect_attempt: 0})}
    end
  end

  def handle_info(:health_check, state), do: {:noreply, state}

  # C-4: EXIT handlers (trap_exit is enabled in init)
  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}
  def handle_info({:EXIT, _pid, :shutdown}, state), do: {:noreply, state}
  def handle_info({:EXIT, _pid, {:shutdown, _}}, state), do: {:noreply, state}

  def handle_info({:EXIT, pid, reason}, state) do
    Logger.warning(
      "[KubeMQ.Connection] Linked process #{inspect(pid)} exited: #{inspect(reason)}"
    )

    {:noreply, state}
  end

  def handle_info({ref, _result}, state) when is_reference(ref) do
    {:noreply, state}
  end

  # FIX-3: Destructure connect_task as {_pid, ref} tuple for comparison
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    connect_ref =
      case state.connect_task do
        {_pid, r} -> r
        nil -> nil
      end

    if ref == connect_ref do
      Logger.warning("[KubeMQ.Connection] Connect task died: #{inspect(reason)}")

      case state.state do
        s when s in [:connecting, :reconnecting] ->
          {:noreply, schedule_reconnect(%{state | connect_task: nil, state: :reconnecting})}

        _ ->
          {:noreply, %{state | connect_task: nil}}
      end
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    do_close(state)
    :ok
  end

  # --- Private Functions ---

  # FIX-3: Return {pid, ref} tuple so callers can kill the task process on close
  @spec start_connect_task(t()) :: {pid(), reference()}
  defp start_connect_task(state) do
    parent = self()
    address = state.address
    config = state.config

    # Use Task.start_link so the task is linked to the Connection GenServer.
    # The GRPC channel process is started under GRPC.Client.Supervisor, so it
    # is NOT linked to this task. When the task exits normally after sending
    # :connect_result, only the task process dies — the GRPC channel stays alive.
    {:ok, pid} =
      Task.start_link(fn ->
        result = do_connect(address, config)
        send(parent, {:connect_result, result})
      end)

    ref = Process.monitor(pid)
    {pid, ref}
  end

  @spec do_connect(String.t(), map()) :: {:ok, GRPC.Channel.t()} | {:error, term()}
  defp do_connect(address, config) do
    opts = build_channel_opts(config, address)

    GRPC.Stub.connect(address, opts)
  rescue
    e -> {:error, Exception.message(e)}
  end

  @spec build_channel_opts(map(), String.t()) :: keyword()
  defp build_channel_opts(config, address) do
    opts = []

    # Extract hostname from address for SNI
    hostname = extract_hostname(address)

    # TLS: use KubeMQ.TLS module instead of raw GRPC.Credential.new
    opts =
      if config.tls do
        cred = KubeMQ.TLS.to_credential(config.tls, hostname)
        Keyword.put(opts, :cred, cred)
      else
        opts
      end

    # Auth headers
    headers =
      case resolve_auth_token(config) do
        {:ok, token} when is_binary(token) and token != "" ->
          # M-10: Warn if sending auth token over plaintext
          if is_nil(config.tls) do
            Logger.warning(
              "[KubeMQ.Connection] Auth token is being sent over a plaintext connection. " <>
                "Consider enabling TLS for secure token transmission."
            )
          end

          %{"authorization" => token}

        _ ->
          %{}
      end

    opts =
      if map_size(headers) > 0 do
        Keyword.put(opts, :headers, headers)
      else
        opts
      end

    # Note: Connection timeout and keepalive are managed at the GenServer level
    # (health_check, reconnect timers) rather than at the GRPC adapter level,
    # because the adapter options differ between Gun and Mint backends.
    opts
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp extract_hostname(address) when is_binary(address) do
    case String.split(address, ":", parts: 2) do
      [host | _] -> host
      _ -> address
    end
  end

  @spec resolve_auth_token(map()) :: {:ok, String.t()} | {:error, term()} | :none
  defp resolve_auth_token(%{credential_provider: provider} = config)
       when not is_nil(provider) do
    provider.get_token(Map.to_list(config))
  end

  defp resolve_auth_token(%{auth_token: token}) when is_binary(token) and token != "" do
    {:ok, token}
  end

  defp resolve_auth_token(_config), do: :none

  @spec health_check(module(), GRPC.Channel.t()) :: {:ok, map()} | {:error, term()}
  defp health_check(transport, channel) do
    transport.ping(channel)
  end

  @spec transition(t(), state(), GRPC.Channel.t() | nil) :: t()
  defp transition(state, new_state, channel \\ nil) do
    if state.reconnect_timer do
      Process.cancel_timer(state.reconnect_timer)
    end

    %{
      state
      | state: new_state,
        channel: channel || state.channel,
        reconnect_timer: nil
    }
  end

  @spec schedule_reconnect(t()) :: t()
  defp schedule_reconnect(state) do
    if not state.config.reconnect_enabled do
      Logger.error("[KubeMQ.Connection] Reconnection disabled, closing")
      do_close(state)
    else
      delay = compute_backoff_delay(state)
      timer_ref = Process.send_after(self(), :reconnect_tick, delay)

      Logger.info(
        "[KubeMQ.Connection] Scheduling reconnect attempt #{state.reconnect_attempt + 1} in #{delay}ms"
      )

      %{state | reconnect_timer: timer_ref, state: :reconnecting}
    end
  end

  defp schedule_health_check(state) do
    interval = state.config.keepalive_time || @health_check_interval
    Process.send_after(self(), :health_check, interval)
  end

  defp schedule_token_refresh(state) do
    # Cancel any existing token refresh timer to prevent leaks
    if state.token_refresh_timer, do: Process.cancel_timer(state.token_refresh_timer)

    # Only schedule if credential_provider is configured (dynamic tokens)
    if state.config.credential_provider do
      timer = Process.send_after(self(), :refresh_auth_token, 600_000)
      %{state | token_refresh_timer: timer}
    else
      %{state | token_refresh_timer: nil}
    end
  end

  @spec compute_backoff_delay(t()) :: non_neg_integer()
  defp compute_backoff_delay(state) do
    initial = state.config.initial_delay
    max_delay = state.config.max_delay
    multiplier = state.config.multiplier
    attempt = state.reconnect_attempt

    base_delay = trunc(initial * :math.pow(multiplier, attempt))
    capped = min(base_delay, max_delay)

    jitter = :rand.uniform(max(trunc(capped * 0.2), 1))
    min(capped + jitter, max_delay)
  end

  @spec buffer_request(t(), GenServer.from()) :: t()
  defp buffer_request(state, from) do
    new_buffer = :queue.in(from, state.buffer)
    %{state | buffer: new_buffer, buffer_size: state.buffer_size + 1}
  end

  @spec buffer_request_or_reject(t(), GenServer.from()) ::
          {:buffered, t()} | {:rejected, map()}
  defp buffer_request_or_reject(state, from) do
    if state.buffer_size >= state.max_buffer_size do
      {:rejected, buffer_full_error()}
    else
      {:buffered, buffer_request(state, from)}
    end
  end

  @spec flush_buffer(t()) :: t()
  defp flush_buffer(%{channel: channel} = state) do
    flush_buffer_loop(state.buffer, channel)
    %{state | buffer: :queue.new(), buffer_size: 0}
  end

  @spec flush_buffer_loop(:queue.queue(), GRPC.Channel.t()) :: :ok
  defp flush_buffer_loop(queue, channel) do
    case :queue.out(queue) do
      {{:value, from}, rest} ->
        GenServer.reply(from, {:ok, channel})
        flush_buffer_loop(rest, channel)

      {:empty, _} ->
        :ok
    end
  end

  @spec reject_buffer(t(), map()) :: :ok
  defp reject_buffer(state, error) do
    reject_buffer_loop(state.buffer, error)
  end

  @spec reject_buffer_loop(:queue.queue(), map()) :: :ok
  defp reject_buffer_loop(queue, error) do
    case :queue.out(queue) do
      {{:value, from}, rest} ->
        GenServer.reply(from, {:error, error})
        reject_buffer_loop(rest, error)

      {:empty, _} ->
        :ok
    end
  end

  @spec do_close(t()) :: t()
  defp do_close(state) do
    # Cancel reconnect timer
    if state.reconnect_timer, do: Process.cancel_timer(state.reconnect_timer)

    # Cancel health check timer
    if state.health_timer, do: Process.cancel_timer(state.health_timer)

    # Cancel token refresh timer
    if state.token_refresh_timer, do: Process.cancel_timer(state.token_refresh_timer)

    # M-19 + FIX-3: Kill in-flight connect task process, then demonitor
    if state.connect_task do
      {pid, ref} = state.connect_task
      Process.exit(pid, :kill)
      Process.demonitor(ref, [:flush])
    end

    reject_buffer(state, closed_error())

    if state.channel do
      GRPC.Stub.disconnect(state.channel)
    end

    invoke_callback(state.callbacks, :on_closed)

    %{
      state
      | state: :closed,
        channel: nil,
        reconnect_timer: nil,
        health_timer: nil,
        token_refresh_timer: nil,
        connect_task: nil,
        buffer: :queue.new(),
        buffer_size: 0,
        subscriptions: []
    }
  end

  @spec reestablish_subscriptions(t()) :: :ok
  defp reestablish_subscriptions(state) do
    Enum.each(state.subscriptions, fn sub ->
      if sub[:resubscribe] do
        Task.start(fn ->
          sub.resubscribe.(state.channel)
        end)
      end
    end)
  end

  @spec invoke_callback(map(), atom()) :: :ok
  defp invoke_callback(callbacks, name) do
    case Map.get(callbacks, name) do
      fun when is_function(fun, 0) ->
        Task.start(fn ->
          try do
            fun.()
          rescue
            e ->
              Logger.error("[KubeMQ.Connection] Callback #{name} raised: #{Exception.message(e)}")
          end
        end)

        :ok

      _ ->
        :ok
    end
  end

  @spec emit_telemetry(atom(), atom(), map(), map()) :: :ok
  defp emit_telemetry(action, phase, measurements, metadata) do
    :telemetry.execute(
      [:kubemq, :connection, action, phase],
      measurements,
      metadata
    )
  end

  @spec extract_gen_opts(keyword()) :: {keyword(), keyword()}
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

  @spec closed_error() :: map()
  defp closed_error do
    %{code: :client_closed, message: "connection is closed"}
  end

  @spec buffer_full_error() :: map()
  defp buffer_full_error do
    %{code: :buffer_full, message: "reconnect buffer is full"}
  end

  @spec max_attempts_error() :: map()
  defp max_attempts_error do
    %{code: :fatal, message: "max reconnection attempts reached"}
  end
end
