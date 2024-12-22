defmodule Feeb.DB.LocalStateTest do
  use ExUnit.Case, async: true
  alias Feeb.DB.LocalState

  describe "add_context/3" do
    test "adds a new context entry" do
      # Initially, there are no contexts
      refute contexts()

      # Add an entry to the state
      LocalState.add_context(:context, 1, {self(), self(), :write})

      refute Stack.empty?(contexts())

      assert %Stack{entries: [entry]} = contexts()
      assert entry.context == :context
      assert entry.shard_id == 1
      assert entry.manager_pid == self()
      assert entry.repo_pid == self()
      assert entry.access_type == :write
    end

    test "supports multiple concurrent entries" do
      # Initially, the `feebdb_state` is empty
      refute contexts()

      # Add several entries to the state
      LocalState.add_context(:ctx, 1, {self(), self(), :read})
      LocalState.add_context(:ctx, 2, {self(), self(), :write})
      LocalState.add_context(:ctx, 3, {self(), self(), :write})
      LocalState.add_context(:other, 1, {self(), self(), :write})

      assert Stack.find(contexts(), fn %{context: ctx, shard_id: s} -> ctx == :ctx && s == 1 end)
      assert Stack.find(contexts(), fn %{context: ctx, shard_id: s} -> ctx == :ctx && s == 2 end)
      assert Stack.find(contexts(), fn %{context: ctx, shard_id: s} -> ctx == :ctx && s == 3 end)
      assert Stack.find(contexts(), fn %{context: ctx, shard_id: s} -> ctx == :other && s == 1 end)
    end

    test "supports duplicated entries" do
      LocalState.add_context(:context, 1, {:c.pid(0, 10, 0), self(), :read})
      LocalState.add_context(:context, 1, {:c.pid(0, 11, 0), self(), :read})

      with {:ok, {new_stack, second_entry}} <- Stack.pop(contexts()),
           {:ok, {empty_stack, first_entry}} <- Stack.pop(new_stack) do
        # The second entry is the topmost in the Stack. Once popped, we can get to the first entry
        # Each one has the expected PIDs set for the manager
        assert second_entry.manager_pid == :c.pid(0, 11, 0)
        assert first_entry.manager_pid == :c.pid(0, 10, 0)

        # Once both are popped, the LocalState stack is empty
        assert Stack.empty?(empty_stack)
      end
    end
  end

  describe "remove_current_context/2" do
    test "removes the entry" do
      # Initially, the `feebdb_state` is empty
      refute contexts()

      # Add a couple of entries to the state
      LocalState.add_context(:context, 1, {self(), self(), :write})
      LocalState.add_context(:context, 2, {self(), self(), :write})

      # They are both stored correctly
      assert Stack.find(contexts(), fn %{shard_id: s} -> s == 1 end)
      assert Stack.find(contexts(), fn %{shard_id: s} -> s == 2 end)

      # Remove the current (topmost) entry
      LocalState.remove_current_context()

      # Only the first entry is found in the state var
      assert Stack.find(contexts(), fn %{shard_id: s} -> s == 1 end)
      refute Stack.find(contexts(), fn %{shard_id: s} -> s == 2 end)

      # Which will be empty if we remove that one too
      LocalState.remove_current_context()
      assert Stack.empty?(contexts())
    end

    test "raises if the entry doesn't exist" do
      assert %{message: error} =
               assert_raise(RuntimeError, fn ->
                 LocalState.remove_current_context()
               end)

      assert error =~ "Can't remove context from empty stack"
    end
  end

  describe "get_current_context!/0" do
    test "returns the current context when set" do
      # There is something set as current context
      LocalState.add_context(:context, 1, {self(), self(), :read})

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
    test "rearranges the state accordingly" do
      # We have [1, 2, 3] as stack entries
      LocalState.add_context(:context, 1, {self(), self(), :read})
      LocalState.add_context(:context, 2, {self(), self(), :read})
      LocalState.add_context(:context, 3, {self(), self(), :read})
      assert [%{shard_id: 1}, %{shard_id: 2}, %{shard_id: 3}] = Stack.to_list(contexts())

      # We'll set "1" as current context, meaning the Stack needs to change to [2, 3, 1]
      LocalState.set_current_context(:context, 1)
      assert [%{shard_id: 2}, %{shard_id: 3}, %{shard_id: 1}] = Stack.to_list(contexts())

      # Now "3" is set as current context, meaning the stack should now be [2, 1, 3]
      LocalState.set_current_context(:context, 3)
      assert [%{shard_id: 2}, %{shard_id: 1}, %{shard_id: 3}] = Stack.to_list(contexts())
    end

    test "raises when setting a context that doesn't exist in the state" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          # This will raise because there is no corresponding entry in the `feebdb_state`
          LocalState.set_current_context(:context, 1)
        end

      assert error == "Attempted to set a context that doesn't exist: {:context, 1}"
    end
  end

  describe "remove_current_context/0" do
    test "deletes the Process state" do
      # There is something set as current context
      LocalState.add_context(:context, 1, {self(), self(), :read})
      refute Stack.empty?(contexts())

      # Once unset, nothing else is defined in the context var
      LocalState.remove_current_context()
      assert Stack.empty?(contexts())
    end
  end

  describe "count_open_contexts/0" do
    test "returns the expected number of contexts" do
      assert 0 == LocalState.count_open_contexts()

      LocalState.add_context(:context, 1, {self(), self(), :read})
      assert 1 == LocalState.count_open_contexts()

      LocalState.add_context(:context, 2, {self(), self(), :read})
      assert 2 == LocalState.count_open_contexts()

      LocalState.remove_current_context()
      assert 1 == LocalState.count_open_contexts()

      LocalState.remove_current_context()
      assert 0 == LocalState.count_open_contexts()
    end
  end

  defp contexts, do: Process.get(:feebdb_contexts)
end
