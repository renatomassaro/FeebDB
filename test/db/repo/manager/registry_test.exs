defmodule Feeb.DB.Repo.Manager.RegistryTest do
  use Test.Feeb.DBCase, async: true
  import ExUnit.CaptureLog
  alias Feeb.DB.Repo.Manager.Registry

  @ets_table_name :feebdb_manager_registry

  describe "fetch_or_create/2" do
    test "creates a previously non-existent Manager", %{shard_id: shard_id} do
      assert {:ok, manager_pid} = Registry.fetch_or_create(:test, shard_id)

      # The manager exists
      assert Process.alive?(manager_pid)

      # It has the expected data
      manager_state = :sys.get_state(manager_pid)
      assert manager_state.context == :test
      assert manager_state.shard_id == shard_id
      assert manager_state.write_1 == %{pid: nil, busy?: false}
      assert manager_state.read_1 == %{pid: nil, busy?: false}
      assert manager_state.read_2 == %{pid: nil, busy?: false}

      # The manager PID is stored in the ETS registry table
      assert [{{:test, shard_id}, manager_pid}] == :ets.lookup(@ets_table_name, {:test, shard_id})
    end

    test "returns a previously created Manager", %{shard_id: shard_id} do
      # First we create it (it did not exist before)
      assert {:ok, manager_pid} = Registry.fetch_or_create(:test, shard_id)

      # But once we call it again, it is returned from the local ETS lookup
      assert {:ok, manager_pid} == Registry.fetch_or_create(:test, shard_id)

      # This is what the ETS table looks like after both operations (it has a single entry)
      assert [{{:test, shard_id}, manager_pid}] == :ets.lookup(@ets_table_name, {:test, shard_id})
    end

    test "creates a new manager if the cached one is dead", %{shard_id: shard_id} do
      # `spawn` returns the PID of the new process, which will die right after it is instantiated
      dead_pid = spawn(fn -> nil end)

      # Wait a bit so that the spawned process above finishes executing and dies
      :timer.sleep(10)

      # We use a custom ETS table so that we can add a dead PID there
      table_name = :custom_registry
      :ets.new(table_name, [:set, :public, :named_table])

      # Store the dead pid as if it were a manager
      :ets.insert(table_name, {{:test, shard_id}, dead_pid})

      # It returned a different PID than `dead_pid`
      log =
        capture_log(fn ->
          assert {:ok, manager_pid} = Registry.fetch_or_create(:test, shard_id, table_name)
          refute manager_pid == dead_pid
          assert Process.alive?(manager_pid)

          # If we look at the ETS table, the dead PID has been replaced by the manager PID
          assert [{{:test, shard_id}, manager_pid}] == :ets.lookup(table_name, {:test, shard_id})
        end)

      assert log =~ "Manager #{inspect(dead_pid)} found dead"
    end
  end

  describe "handle_call/3" do
    setup do
      # For this kind of test, we use a custom ETS table and make it public (instead of protected).
      :ets.new(:custom_registry, [:set, :public, :named_table])

      :ok
    end

    test "returns an ETS entry if one exists and is alive", %{shard_id: shard_id} do
      table_name = :custom_registry

      # Let's pretend an entry already exists
      :ets.insert(table_name, {{:test, shard_id}, self()})

      # Registry will return the test pid (instead of a Manager pid)
      assert {:reply, {:ok, mocked_manager_pid}, _state} =
               Registry.handle_call({:fetch_or_create, :test, shard_id, table_name}, self(), %{})

      assert mocked_manager_pid == self()
    end

    test "creates (and replaces) a new manager if the cached one is dead", %{shard_id: shard_id} do
      # `spawn` returns the PID of the new process, which will die right after it is instantiated
      dead_pid = spawn(fn -> nil end)

      # Wait a bit so that the spawned process above finishes executing and dies
      :timer.sleep(10)

      # Store the dead pid as if it were a manager
      table_name = :custom_registry
      :ets.insert(table_name, {{:test, shard_id}, dead_pid})

      # Registry shall return a real manager PID
      log =
        capture_log(fn ->
          assert {:reply, {:ok, manager_pid}, _state} =
                   Registry.handle_call(
                     {:fetch_or_create, :test, shard_id, table_name},
                     self(),
                     %{}
                   )

          # It did not return the dead PID
          refute manager_pid == dead_pid

          # The manager is alive and well
          assert Process.alive?(manager_pid)
          assert :sys.get_state(manager_pid).shard_id == shard_id

          # If we look at the ETS table, the dead PID has been replaced by the manager PID
          assert [{{:test, shard_id}, manager_pid}] == :ets.lookup(table_name, {:test, shard_id})
        end)

      assert log =~ "Manager #{inspect(dead_pid)} found dead"
    end
  end
end
