defmodule Feeb.DB.LocalStateTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias Feeb.DB.LocalState

  describe "add_entry/3" do
    test "adds a new entry" do
      # Initially, the `feebdb_state` is empty
      refute state_var()

      # Add an entry to the state
      LocalState.add_entry(:context, 1, {self(), self(), :write})

      assert %{{:context, 1} => entry} = state_var()
      assert entry.context == :context
      assert entry.shard_id == 1
      assert entry.manager_pid == self()
      assert entry.repo_pid == self()
      assert entry.access_type == :write
    end

    test "supports multiple concurrent entries" do
      # Initially, the `feebdb_state` is empty
      refute state_var()

      # Add several entries to the state
      LocalState.add_entry(:context, 1, {self(), self(), :read})
      LocalState.add_entry(:context, 2, {self(), self(), :write})
      LocalState.add_entry(:context, 3, {self(), self(), :write})
      LocalState.add_entry(:other_context, 1, {self(), self(), :write})

      state = state_var()
      assert Enum.find(state, fn {key, _} -> key == {:context, 1} end)
      assert Enum.find(state, fn {key, _} -> key == {:context, 2} end)
      assert Enum.find(state, fn {key, _} -> key == {:context, 3} end)
      assert Enum.find(state, fn {key, _} -> key == {:other_context, 1} end)
    end

    test "warns if the entry already exists" do
      LocalState.add_entry(:context, 1, {self(), self(), :read})

      log =
        capture_log(fn ->
          # Repeated entry
          LocalState.add_entry(:context, 1, {self(), self(), :read})
        end)

      assert log =~ "[warning] Adding LocalState entry to a key that already exists"
      assert log =~ "{:context, 1}"
    end
  end

  describe "remove_entry/2" do
    test "removes the entry" do
      # Initially, the `feebdb_state` is empty
      refute state_var()

      # Add an entry to the state
      LocalState.add_entry(:context, 1, {self(), self(), :write})
      LocalState.add_entry(:context, 2, {self(), self(), :write})

      # The entry can be found in the state var
      assert %{{:context, 1} => _, {:context, 2} => _} = state_var()

      # Remove one of the entries
      LocalState.remove_entry(:context, 1)

      # Only the other entry is found in the state var
      assert %{{:context, 2} => _} = state_var()

      # Which will be empty if we remove that one too
      LocalState.remove_entry(:context, 2)
      assert %{} == state_var()
    end

    test "warns if the entry doesn't exist" do
      # Assume `feebdb_state` is an empty map
      Process.put(:feebdb_state, %{})

      log = capture_log(fn -> LocalState.remove_entry(:context, 1) end)
      assert log =~ "[warning] Attempted to delete {:context, 1} from State but it no longer exists"
    end
  end

  describe "get_current_context!/0" do
    test "returns the current context when set" do
      # There is something set as current context
      LocalState.add_entry(:context, 1, {self(), self(), :read})
      LocalState.set_current_context(:context, 1)

      state = LocalState.get_current_context!()
      assert state.context == :context
      assert state.shard_id == 1
      assert state.manager_pid == self()
      assert state.repo_pid == self()
      assert state.access_type == :read
    end

    test "raises if no current context is set" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          LocalState.get_current_context!()
        end

      assert error == "Current context not set"
    end
  end

  describe "set_current_context/2" do
    test "updates the Process state" do
      LocalState.set_current_context(:context, 1)
      assert current_context_var() == {:context, 1}
    end
  end

  describe "unset_current_context/0" do
    test "unsets the Process state" do
      # There is something set as current context
      LocalState.set_current_context(:context, 1)
      assert current_context_var()

      # Once unset, nothing else is defined in the context var
      LocalState.unset_current_context()
      refute current_context_var()
    end
  end

  defp current_context_var, do: Process.get(:feebdb_current_context)
  defp state_var, do: Process.get(:feebdb_state)
end
