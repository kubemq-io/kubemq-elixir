defmodule KubeMQ.SubscriptionTest do
  use ExUnit.Case, async: true

  alias KubeMQ.Subscription

  describe "new/1" do
    test "creates a subscription with pid and monitor ref" do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      sub = Subscription.new(pid)

      assert %Subscription{pid: ^pid, ref: ref} = sub
      assert is_reference(ref)

      # Cleanup
      Process.exit(pid, :kill)
    end
  end

  describe "active?/1" do
    test "returns true when the process is alive" do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      sub = Subscription.new(pid)

      assert Subscription.active?(sub)

      # Cleanup
      Process.exit(pid, :kill)
    end

    test "returns false when the process has exited" do
      pid = spawn(fn -> :ok end)
      # Give the process time to exit
      Process.sleep(50)
      sub = %Subscription{pid: pid, ref: make_ref()}

      refute Subscription.active?(sub)
    end
  end

  describe "cancel/1" do
    test "stops the process and returns :ok" do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      sub = Subscription.new(pid)
      assert Process.alive?(pid)

      assert :ok = Subscription.cancel(sub)
      Process.sleep(50)
      refute Process.alive?(pid)
    end

    test "returns :ok when the process is already dead" do
      pid = spawn(fn -> :ok end)
      Process.sleep(50)
      sub = %Subscription{pid: pid, ref: make_ref()}

      assert :ok = Subscription.cancel(sub)
    end
  end
end
