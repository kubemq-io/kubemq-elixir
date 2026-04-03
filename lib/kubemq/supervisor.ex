defmodule KubeMQ.Supervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    client_id = Keyword.fetch!(opts, :client_id)
    Supervisor.start_link(__MODULE__, client_id)
  end

  @impl Supervisor
  def init(client_id) do
    # NOTE: No per-client Registry child here. All {:via, Registry} names use the
    # global KubeMQ.ProcessRegistry started by KubeMQ.Application. A per-client
    # Registry would be unused and waste resources.
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: subscription_supervisor_name(client_id)}
    ]

    # :one_for_one is appropriate because Connection, DynamicSupervisor, and the
    # task supervisor are independent -- a crash in one should not restart others.
    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec start_client_tree(String.t()) ::
          {:ok,
           %{
             subscription_supervisor: GenServer.name(),
             registry: GenServer.name(),
             supervisor: pid()
           }}
          | {:error, term()}
  def start_client_tree(client_id) when is_binary(client_id) do
    case start_link(client_id: client_id) do
      {:ok, sup_pid} ->
        {:ok,
         %{
           subscription_supervisor: subscription_supervisor_name(client_id),
           registry: registry_name(client_id),
           supervisor: sup_pid
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec stop_client_tree(%{supervisor: pid()}) :: :ok
  def stop_client_tree(%{supervisor: sup}) when is_pid(sup) do
    Supervisor.stop(sup, :normal)
  end

  def stop_client_tree(%{subscription_supervisor: sup, registry: registry}) do
    # Backward compatibility: stop individual processes
    stop_process(sup)
    stop_process(registry)
    :ok
  end

  # H-10: Use {:via, Registry} instead of Module.concat
  defp registry_name(client_id) do
    {:via, Registry, {KubeMQ.ProcessRegistry, {client_id, :registry}}}
  end

  defp subscription_supervisor_name(client_id) do
    {:via, Registry, {KubeMQ.ProcessRegistry, {client_id, :subscription_supervisor}}}
  end

  defp stop_process(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> stop_process(pid)
    end
  end

  defp stop_process({:via, _, _} = name) do
    case GenServer.whereis(name) do
      nil -> :ok
      pid -> stop_process(pid)
    end
  end

  defp stop_process(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal)
    :ok
  end
end
