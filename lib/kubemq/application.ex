defmodule KubeMQ.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {GRPC.Client.Supervisor, []},
      {Registry, keys: :unique, name: KubeMQ.ProcessRegistry}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: KubeMQ.AppSupervisor)
  end
end
