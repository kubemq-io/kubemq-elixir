defmodule KubeMQ.Commands.Subscriber do
  @moduledoc false

  use GenServer
  require Logger

  alias KubeMQ.{CommandReceive, CommandReply, Error}

  defstruct [
    :channel,
    :group,
    :client_id,
    :transport,
    :grpc_channel,
    :stream,
    :on_command,
    :on_error,
    :conn,
    :recv_pid,
    :task_supervisor
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    channel = Keyword.fetch!(opts, :channel)
    client_id = Keyword.fetch!(opts, :client_id)
    on_command = Keyword.fetch!(opts, :on_command)
    transport = Keyword.get(opts, :transport, KubeMQ.Transport.GRPC)
    grpc_channel = Keyword.fetch!(opts, :grpc_channel)
    conn = Keyword.get(opts, :conn)
    max_callback_concurrency = Keyword.get(opts, :max_callback_concurrency, 100)
    {:ok, task_sup} = Task.Supervisor.start_link(max_children: max_callback_concurrency)

    state = %__MODULE__{
      channel: channel,
      group: Keyword.get(opts, :group, ""),
      client_id: client_id,
      transport: transport,
      grpc_channel: grpc_channel,
      on_command: on_command,
      on_error: Keyword.get(opts, :on_error),
      conn: conn,
      recv_pid: nil,
      task_supervisor: task_sup
    }

    {:ok, state, {:continue, :subscribe}}
  end

  @impl GenServer
  def handle_continue(:subscribe, state) do
    if state.recv_pid && Process.alive?(state.recv_pid) do
      Process.exit(state.recv_pid, :shutdown)
    end

    parent = self()
    recv_pid = spawn_link(fn -> subscribe_and_recv(state, parent) end)
    {:noreply, %{state | recv_pid: recv_pid}}
  end

  def handle_continue(:resubscribe, state) do
    if state.conn && Process.alive?(state.conn) do
      case KubeMQ.Connection.get_channel(state.conn) do
        {:ok, grpc_channel} ->
          new_state = %{state | grpc_channel: grpc_channel}

          if state.recv_pid && Process.alive?(state.recv_pid) do
            Process.exit(state.recv_pid, :shutdown)
          end

          parent = self()
          recv_pid = spawn_link(fn -> subscribe_and_recv(new_state, parent) end)
          {:noreply, %{new_state | recv_pid: recv_pid}}

        {:error, _} ->
          Process.send_after(self(), :retry_resubscribe, 5_000)
          {:noreply, state}
      end
    else
      {:stop, :connection_closed, state}
    end
  end

  @impl GenServer
  def handle_info({:subscribed, _pid}, state) do
    {:noreply, state}
  end

  def handle_info({:subscription_failed, reason}, state) do
    dispatch_error(
      state,
      Error.transient("subscription failed: #{inspect(reason)}",
        operation: "subscribe_to_commands",
        channel: state.channel
      )
    )

    Process.send_after(self(), :retry_resubscribe, 5_000)
    {:noreply, state}
  end

  def handle_info({:stream_request, proto_request}, state) do
    command_receive = CommandReceive.from_proto(proto_request)
    handle_command_and_respond(state, command_receive)
    {:noreply, state}
  end

  def handle_info({:stream_error, reason}, state) do
    Logger.warning("[Commands.Subscriber] Stream error on #{state.channel}: #{inspect(reason)}")

    dispatch_error(
      state,
      Error.stream_broken("subscription stream error: #{inspect(reason)}",
        operation: "subscribe_to_commands",
        channel: state.channel
      )
    )

    {:noreply, state, {:continue, :resubscribe}}
  end

  def handle_info({:stream_closed, _reason}, state) do
    Logger.info("[Commands.Subscriber] Stream closed on #{state.channel}")
    {:noreply, %{state | recv_pid: nil}, {:continue, :resubscribe}}
  end

  def handle_info(:retry_resubscribe, state) do
    {:noreply, state, {:continue, :resubscribe}}
  end

  def handle_info({:EXIT, pid, reason}, state) when pid == state.recv_pid do
    Logger.warning("[Commands.Subscriber] recv_loop exited: #{inspect(reason)}")
    {:noreply, %{state | recv_pid: nil}, {:continue, :resubscribe}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.recv_pid && Process.alive?(state.recv_pid) do
      Process.exit(state.recv_pid, :shutdown)
    end

    :ok
  end

  # --- Private ---

  defp do_subscribe(state) do
    subscribe_request = %{
      subscribe_type: :commands,
      client_id: state.client_id,
      channel: state.channel,
      group: state.group || ""
    }

    state.transport.subscribe(state.grpc_channel, subscribe_request)
  end

  defp handle_command_and_respond(state, %CommandReceive{} = command_receive) do
    Task.Supervisor.start_child(state.task_supervisor, fn ->
      try do
        reply = state.on_command.(command_receive)

        reply =
          case reply do
            %CommandReply{} ->
              %{
                reply
                | request_id: reply.request_id || command_receive.id,
                  response_to: reply.response_to || command_receive.reply_channel,
                  client_id: reply.client_id || state.client_id
              }

            _ ->
              %CommandReply{
                request_id: command_receive.id,
                response_to: command_receive.reply_channel,
                client_id: state.client_id,
                executed: false,
                error: "invalid reply type from on_command callback"
              }
          end

        response_map = CommandReply.to_response_map(reply)

        # M-15: Get fresh channel for response (connection may have reconnected)
        grpc_channel =
          case KubeMQ.Connection.get_channel(state.conn) do
            {:ok, ch} -> ch
            {:error, _} -> state.grpc_channel
          end

        case state.transport.send_response(grpc_channel, response_map) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("[Commands.Subscriber] Failed to send response: #{inspect(reason)}")
        end
      rescue
        e ->
          Logger.error("[Commands.Subscriber] on_command callback error: #{Exception.message(e)}")

          # G13/M-11: Sanitize exception message in response to avoid leaking internal details
          error_reply = %CommandReply{
            request_id: command_receive.id,
            response_to: command_receive.reply_channel,
            client_id: state.client_id,
            executed: false,
            error: "internal callback error"
          }

          response_map = CommandReply.to_response_map(error_reply)

          grpc_channel =
            case KubeMQ.Connection.get_channel(state.conn) do
              {:ok, ch} -> ch
              {:error, _} -> state.grpc_channel
            end

          state.transport.send_response(grpc_channel, response_map)
      end
    end)
  end

  defp subscribe_and_recv(state, parent) do
    case do_subscribe(state) do
      {:ok, stream} ->
        Kernel.send(parent, {:subscribed, self()})
        recv_loop(stream, parent)

      {:error, reason} ->
        Kernel.send(parent, {:subscription_failed, reason})
    end
  end

  defp recv_loop(stream, parent) do
    Enum.each(stream, fn
      {:ok, request} ->
        Kernel.send(parent, {:stream_request, request})

      {:error, reason} ->
        Kernel.send(parent, {:stream_error, reason})
    end)

    # Stream ended normally (server closed)
    Kernel.send(parent, {:stream_closed, :eof})
  rescue
    e ->
      Kernel.send(parent, {:stream_closed, {:exception, Exception.message(e)}})
  end

  defp dispatch_error(state, error) do
    if is_function(state.on_error, 1) do
      Task.Supervisor.start_child(state.task_supervisor, fn ->
        try do
          state.on_error.(error)
        rescue
          e ->
            Logger.error("[Commands.Subscriber] on_error callback error: #{Exception.message(e)}")
        end
      end)
    end
  end
end
