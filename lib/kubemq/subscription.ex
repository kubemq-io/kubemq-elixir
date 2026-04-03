defmodule KubeMQ.Subscription do
  @moduledoc """
  Handle for an active subscription. Provides cancellation and status checking.

  ## Usage

      {:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "my-channel", on_event: &handler/1)
      true = KubeMQ.Subscription.active?(sub)
      :ok = KubeMQ.Subscription.cancel(sub)
  """

  @type t :: %__MODULE__{
          pid: pid(),
          ref: reference()
        }

  defstruct [:pid, :ref]

  @spec new(pid()) :: t()
  def new(pid) when is_pid(pid) do
    ref = Process.monitor(pid)
    %__MODULE__{pid: pid, ref: ref}
  end

  @doc """
  Cancel an active subscription and stop its process.

  Demonitors the subscription process and stops it gracefully. Safe to call
  on already-stopped subscriptions.

  ## Examples

      {:ok, sub} = KubeMQ.Client.subscribe_to_events(client, "channel", on_event: &handler/1)
      :ok = KubeMQ.Subscription.cancel(sub)
  """
  @spec cancel(t()) :: :ok
  def cancel(%__MODULE__{pid: pid, ref: ref}) do
    Process.demonitor(ref, [:flush])

    if Process.alive?(pid) do
      GenServer.stop(pid, :normal)
    end

    :ok
  end

  @doc """
  Check whether the subscription process is still alive.

  ## Examples

      true = KubeMQ.Subscription.active?(sub)
  """
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{pid: pid}) do
    Process.alive?(pid)
  end
end
