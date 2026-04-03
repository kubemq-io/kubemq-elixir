defmodule KubeMQ.SupervisorTest do
  use ExUnit.Case, async: false

  alias KubeMQ.Supervisor, as: KSupervisor

  describe "start_client_tree/1" do
    test "returns OK with supervisor pid and names" do
      client_id = "sup-test-#{System.unique_integer([:positive])}"

      result = KSupervisor.start_client_tree(client_id)
      assert {:ok, tree} = result
      assert is_pid(tree.supervisor)
      assert tree.subscription_supervisor != nil
      assert tree.registry != nil

      # Cleanup immediately rather than in on_exit to avoid race conditions
      KSupervisor.stop_client_tree(tree)
      Process.sleep(50)
    end

    test "multiple client_ids don't create atoms" do
      # Uses {:via, Registry, ...} names, not Module.concat / String.to_atom
      atom_count_before = :erlang.system_info(:atom_count)

      trees =
        Enum.map(1..5, fn i ->
          client_id = "no-atom-test-#{i}-#{System.unique_integer([:positive])}"
          {:ok, tree} = KSupervisor.start_client_tree(client_id)
          tree
        end)

      atom_count_after = :erlang.system_info(:atom_count)

      # We expect very few atoms created (at most a handful for internal ops),
      # but certainly not 5+ for client IDs. The key assertion is that
      # client_id strings are NOT converted to atoms.
      assert atom_count_after - atom_count_before < 10

      # Cleanup immediately
      Enum.each(trees, fn tree ->
        try do
          if is_pid(tree.supervisor) and Process.alive?(tree.supervisor) do
            KSupervisor.stop_client_tree(tree)
          end
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
      end)
    end
  end

  describe "stop_client_tree/1" do
    test "stops children and supervisor" do
      client_id = "stop-test-#{System.unique_integer([:positive])}"
      {:ok, tree} = KSupervisor.start_client_tree(client_id)
      sup_pid = tree.supervisor

      assert Process.alive?(sup_pid)

      :ok = KSupervisor.stop_client_tree(tree)

      # Give time for the process to terminate
      Process.sleep(100)
      refute Process.alive?(sup_pid)
    end
  end

  describe "uses :one_for_one strategy" do
    test "supervisor init returns :one_for_one strategy" do
      client_id = "strategy-test-#{System.unique_integer([:positive])}"

      # We can inspect the init return value directly
      {:ok, {sup_flags, _children}} = KSupervisor.init(client_id)
      assert sup_flags.strategy == :one_for_one
    end
  end

  describe "stop_client_tree/1 backward-compat signature" do
    test "stops processes given subscription_supervisor and registry keys" do
      client_id = "compat-test-#{System.unique_integer([:positive])}"
      {:ok, tree} = KSupervisor.start_client_tree(client_id)

      # Use the backward-compatible map shape (no :supervisor key)
      compat_map = %{
        subscription_supervisor: tree.subscription_supervisor,
        registry: tree.registry
      }

      assert :ok = KSupervisor.stop_client_tree(compat_map)
      Process.sleep(50)

      # Also stop the supervisor pid to clean up
      if Process.alive?(tree.supervisor), do: Supervisor.stop(tree.supervisor, :normal)
      Process.sleep(50)
    end

    test "stop_client_tree backward-compat handles already-stopped processes" do
      # Pass {:via, Registry, ...} names that don't resolve to any process
      compat_map = %{
        subscription_supervisor:
          {:via, Registry, {KubeMQ.ProcessRegistry, {"nonexistent-1", :subscription_supervisor}}},
        registry: {:via, Registry, {KubeMQ.ProcessRegistry, {"nonexistent-1", :registry}}}
      }

      assert :ok = KSupervisor.stop_client_tree(compat_map)
    end

    test "stop_client_tree backward-compat with atom names" do
      # Test the stop_process(name) when is_atom(name) path
      # Use an atom name that doesn't resolve to any process
      compat_map = %{
        subscription_supervisor: :nonexistent_atom_sup,
        registry: :nonexistent_atom_reg
      }

      assert :ok = KSupervisor.stop_client_tree(compat_map)
    end

    test "stop_client_tree backward-compat with atom name pointing to alive process" do
      # Start a GenServer with an atom name, then stop it via backward-compat path
      {:ok, pid} = Agent.start_link(fn -> :ok end, name: :test_stop_atom_sup)
      assert Process.alive?(pid)

      compat_map = %{
        subscription_supervisor: :test_stop_atom_sup,
        registry: :nonexistent_atom_reg2
      }

      assert :ok = KSupervisor.stop_client_tree(compat_map)
      Process.sleep(50)
      refute Process.alive?(pid)
    end
  end

  describe "start_client_tree/1 error handling" do
    test "returns error when supervisor fails to start (duplicate client_id)" do
      client_id = "dup-test-#{System.unique_integer([:positive])}"
      {:ok, tree} = KSupervisor.start_client_tree(client_id)

      # Starting again with same client_id should fail because the DynamicSupervisor name is taken
      # The second start_link crashes the calling process with EXIT, so we trap exits
      Process.flag(:trap_exit, true)
      result = KSupervisor.start_client_tree(client_id)
      assert {:error, _reason} = result

      KSupervisor.stop_client_tree(tree)
      Process.sleep(50)
    end
  end

  describe "init/1 returns correct children" do
    test "init includes one child for DynamicSupervisor" do
      client_id = "init-child-test-#{System.unique_integer([:positive])}"
      {:ok, {_flags, children}} = KSupervisor.init(client_id)

      assert length(children) == 1
      # The child spec should have a start tuple pointing to DynamicSupervisor
      [child_spec] = children
      {mod, _fun, _args} = child_spec.start
      assert mod == DynamicSupervisor
    end
  end
end
