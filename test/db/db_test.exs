defmodule Feeb.DBTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Feeb.DB.LocalState
  alias Sample.Post

  @context :test

  describe "begin/3" do
    test "initiates a write transaction", %{shard_id: shard_id, db: db} do
      assert :ok == DB.begin(@context, shard_id, :write)

      # The environment was set up
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :write
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # Manager has correct data
      m_state = :sys.get_state(state.manager_pid)
      assert m_state.shard_id == shard_id
      assert m_state.write_1.pid == state.repo_pid
      assert m_state.write_1.busy?
      refute m_state.read_1.pid
      refute m_state.read_1.busy?
      refute m_state.read_2.pid
      refute m_state.read_2.busy?

      # Repo has correct data
      r_state = :sys.get_state(state.repo_pid)
      assert r_state.mode == :readwrite
      assert r_state.path == db
      assert r_state.shard_id == shard_id
      assert is_integer(r_state.transaction_id)
    end

    test "initiates a read transaction", %{shard_id: shard_id} do
      Test.Feeb.DB.ensure_migrated(@context, shard_id)

      assert :ok == DB.begin(@context, shard_id, :read)

      # The environment was set up:
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :read

      # Manager has correct data
      m_state = :sys.get_state(state.manager_pid)
      assert m_state.shard_id == shard_id
      assert m_state.read_1.pid == state.repo_pid
      assert m_state.read_1.busy?
      refute m_state.write_1.pid
      refute m_state.write_1.busy?
      refute m_state.read_2.pid
      refute m_state.read_2.busy?

      # Repo has correct data
      r_state = :sys.get_state(state.repo_pid)
      assert r_state.mode == :readonly

      DB.commit()
    end

    test "supports multiple contexts in the same process" do
      {:ok, test_shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, test_shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, raw_shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:raw)

      # We can start many different BEGIN EXCLUSIVE connections in the same (Erlang) process!
      # Note they are all different SQLite databases, which is why we don't get a :busy error.
      DB.begin(:test, test_shard_1, :write)
      DB.begin(:test, test_shard_2, :write)
      DB.begin(:raw, raw_shard_1, :write)

      local_state = Process.get(:feebdb_state)
      assert {_, test_entry_1} = Enum.find(local_state, fn {k, _} -> k == {:test, test_shard_1} end)
      assert {_, test_entry_2} = Enum.find(local_state, fn {k, _} -> k == {:test, test_shard_2} end)
      assert {_, raw_entry_1} = Enum.find(local_state, fn {k, _} -> k == {:raw, raw_shard_1} end)

      # Each entry has the correct context and shard ID
      assert test_entry_1.context == :test
      assert test_entry_1.shard_id == test_shard_1
      assert test_entry_2.context == :test
      assert test_entry_2.shard_id == test_shard_2
      assert raw_entry_1.context == :raw
      assert raw_entry_1.shard_id == raw_shard_1

      # Each one has a different Manager / Repo PID (they are different databases, after all)
      refute test_entry_1.manager_pid == test_entry_2.manager_pid
      refute test_entry_2.manager_pid == raw_entry_1.manager_pid
      refute test_entry_1.repo_pid == test_entry_2.repo_pid
      refute test_entry_2.repo_pid == raw_entry_1.repo_pid
    end

    @tag capture_log: true
    test "fails on parallel calls", %{shard_id: shard_id} do
      # Write
      assert :ok == DB.begin(@context, shard_id, :write)
      assert_raise MatchError, fn -> DB.begin(@context, shard_id, :write) end

      # Read
      assert :ok == DB.begin(@context, shard_id, :read)
      assert :ok == DB.begin(@context, shard_id, :read)
      assert_raise MatchError, fn -> DB.begin(@context, shard_id, :read) end
    end
  end

  describe "with_context/2" do
    test "changes the process context" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_1, :write)
      DB.begin(:test, shard_2, :write)

      # We are at `{:test, shard_2}` because that was the last call to DB.begin/4
      state = LocalState.get_current_context!()
      assert state.context == :test
      assert state.shard_id == shard_2

      # Now we will switch to `{:test, shard_1}`
      DB.with_context(:test, shard_1)
      assert LocalState.get_current_context!().shard_id == shard_1

      # And back again to `{:test, shard_2}`
      DB.with_context(:test, shard_2)
      assert LocalState.get_current_context!().shard_id == shard_2
    end

    test "enables interaction between multiple databases in the same Elixir process" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      # This would fail if they were both in the same shard due to the PK
      post_shard_1 = Post.new(%{id: 1, title: "Foo", body: "Body"})
      post_shard_2 = Post.new(%{id: 1, title: "Foo", body: "Body"})

      DB.begin(:test, shard_1, :write)
      DB.begin(:test, shard_2, :write)

      # Insert post in `{:test, shard_1}`
      DB.with_context(:test, shard_1)
      assert {:ok, _} = DB.insert(post_shard_1)

      # Insert post in `{:test, shard_2}`
      DB.with_context(:test, shard_2)
      assert {:ok, _} = DB.insert(post_shard_2)

      # Commit in `{:test, shard_2}`
      DB.commit()

      # Commit in `{:test, shard_1}`
      DB.with_context(:test, shard_1)
      DB.commit()
    end
  end

  describe "commit/0" do
    test "finishes a transaction", %{shard_id: shard_id} do
      # First we start a transaction
      assert :ok == DB.begin(@context, shard_id, :write)

      # LocalState exists
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :write

      # Naturally repo and manager are alive
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # Then we COMMIT it
      assert :ok == DB.commit()

      # Corresponding environment no longer exists
      assert_raise RuntimeError, fn ->
        LocalState.get_current_context!()
      end

      # More specifically, we can assert the state was removed from internal variables
      refute Process.get(:feebdb_current_context)
      assert Process.get(:feebdb_state) == %{}

      # Repo and Manager are still alive after the transaction
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # The write connection is available for another request
      m_state = :sys.get_state(state.manager_pid)
      refute m_state.write_1.busy?

      # The `transaction_id` entry no longer exists
      r_state = :sys.get_state(state.repo_pid)
      refute r_state.transaction_id

      # Indeed, we can BEGIN again after COMMIT has finished
      assert :ok == DB.begin(@context, shard_id, :write)
      assert :ok == DB.commit()
      assert :ok == DB.begin(@context, shard_id, :write)
      assert :ok == DB.commit()
    end

    @tag capture_log: true
    test "fails if no transactions are open" do
      assert_raise RuntimeError, fn ->
        DB.commit()
      end
    end
  end

  describe "one/1" do
    test "returns the expected result", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)
      assert %{id: 1, name: "Phoebe"} = DB.one({:friends, :get_by_id}, [1])
      assert [_] = DB.one({:pragma, :user_version})
      assert nil == DB.one({:friends, :get_by_id}, [0])
    end

    test "raises if multiple results are found", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      assert_raise RuntimeError, fn ->
        DB.one({:friends, :get_all})
      end
    end
  end
end
