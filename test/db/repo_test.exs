defmodule Feeb.DB.RepoTest do
  use Test.Feeb.DBCase, async: true

  alias GenServer, as: GS
  alias Feeb.DB.{Config, Repo, SQLite}

  @context :test

  setup %{shard_id: shard_id, db: db} = flags do
    repo_pid =
      unless Map.get(flags, :skip_init, false) do
        {:ok, pid} = start_supervised({Repo, {@context, shard_id, db, :readwrite, nil}})

        pid
      else
        nil
      end

    {:ok, %{repo: repo_pid}}
  end

  describe "start_link/1" do
    @describetag skip_init: true

    test "in readwrite mode", %{shard_id: shard_id, db: db} do
      assert {:ok, pid} = start_supervised({Repo, {@context, shard_id, db, :readwrite, nil}})

      state = :sys.get_state(pid)
      c = state.conn

      # A connection for that specific shard has been created
      assert is_reference(state.conn)
      assert state.shard_id == shard_id
      assert state.mode == :readwrite
      refute state.transaction_id

      # I can use the conn directly to query or modify the database
      assert [[_]] = SQLite.raw!(c, "SELECT name FROM friends WHERE id = 1")
      assert [] = SQLite.raw!(c, "DELETE FROM friends WHERE id = 1")
      assert [] = SQLite.raw!(c, "SELECT name FROM friends WHERE id = 1")

      # PRAGMAs defaults were changed upon connection
      assert [[1]] = SQLite.raw!(c, "PRAGMA foreign_keys")
      assert [[0]] = SQLite.raw!(c, "PRAGMA query_only")

      # Below is only changed for the test environment
      assert [[0]] = SQLite.raw!(c, "PRAGMA synchronous")
    end

    test "in readonly mode", %{shard_id: shard_id, db: db} do
      assert {:ok, pid} = start_supervised({Repo, {@context, shard_id, db, :readonly, nil}})

      state = :sys.get_state(pid)
      c = state.conn

      # The connection should be in readonly mode
      assert state.mode == :readonly
      assert [[1]] = SQLite.raw!(c, "PRAGMA query_only")

      # I can read
      assert [[_]] = SQLite.raw!(c, "SELECT name FROM friends WHERE id = 1")

      # But I can't write
      assert {:error, reason} = SQLite.raw(c, "DELETE FROM friends WHERE id = 1")

      assert reason =~ "attempt to write a readonly database"
    end
  end

  describe "init/1" do
    test "upon error, describes what went wrong (missing file)" do
      shard_id = 123
      db = Path.join(Config.data_dir(), "/not_a_context/#{shard_id}.db")

      %{message: reason} =
        assert_raise RuntimeError, fn ->
          Repo.init({@context, 123, db, :readwrite, nil})
        end

      assert reason =~ "Unable to open database"
      assert reason =~ "Database file #{db} does not exist"
    end
  end

  describe "handle_call: begin/commit" do
    test "initiates and completes a transaction", %{repo: repo} do
      refute :sys.get_state(repo).transaction_id

      # Now we are in a transaction
      assert :ok == GS.call(repo, {:begin, :exclusive})
      assert :sys.get_state(repo).transaction_id

      # No longer in a transaction once we commit
      assert :ok == GS.call(repo, {:commit})
      refute :sys.get_state(repo).transaction_id

      # Proof that we are no longer in a transaction: rollback won't work
      c = :sys.get_state(repo).conn
      assert {:error, "cannot rollback - no transaction is active"} = SQLite.raw(c, "ROLLBACK")
    end

    @tag capture_log: true
    test "can't BEGIN twice", %{repo: repo} do
      assert :ok == GS.call(repo, {:begin, :deferred})

      assert {:error, :already_in_transaction} ==
               GS.call(repo, {:begin, :exclusive})

      # Proof that we are in a transaction: we can rollback (only once, of course)
      c = :sys.get_state(repo).conn
      assert {:ok, _} = SQLite.raw(c, "ROLLBACK")
      assert {:error, "cannot rollback - no transaction is active"} = SQLite.raw(c, "ROLLBACK")
    end

    @tag capture_log: true
    test "can't COMMIT when not in a transaction", %{repo: repo} do
      assert {:error, :not_in_transaction} == GS.call(repo, {:commit})

      assert :ok == GS.call(repo, {:begin, :deferred})
      assert :ok == GS.call(repo, {:commit})
      assert {:error, :not_in_transaction} == GS.call(repo, {:commit})
    end
  end

  describe "handle_call: query (one)" do
    test "returns the corresponding result", %{repo: repo} do
      q = {:friends, :get_by_id}

      assert {:ok, %{id: 1, name: "Phoebe"}} = GS.call(repo, {:query, :one, q, [1], []})

      assert {:ok, nil} == GS.call(repo, {:query, :one, q, [9], []})
    end

    @tag capture_log: true
    test "handles errors", %{repo: repo} do
      # Multiple results being returned at once
      assert {:error, :multiple_results} =
               GS.call(repo, {:query, :one, {:friends, :get_all}, [], []})

      # Wrong number of bindings
      assert {:error, :arguments_wrong_length} =
               GS.call(repo, {:query, :one, {:friends, :get_by_id}, [1, 2], []})
    end
  end

  describe "handle_call: raw" do
    @tag capture_log: true
    test "executes raw queries", %{repo: repo} do
      assert {:ok, rows} = GS.call(repo, {:raw, "select * from friends", []})
      assert length(rows) == 6

      # Now with bindings
      assert {:ok, _} = GS.call(repo, {:raw, "delete from friends where id = ?", [1]})

      assert {:ok, rows} = GS.call(repo, {:raw, "select * from friends", []})
      assert length(rows) == 5
    end

    test "warns when not in a transaction", %{repo: repo} do
      refute :sys.get_state(repo).transaction_id

      log =
        capture_log(fn ->
          assert {:ok, _} = GS.call(repo, {:raw, "select * from friends", []})
        end)

      assert log =~ "warning"
      assert log =~ "implicit transaction"
    end
  end

  describe "handle_call: close" do
    test "closes the connection and kills the server", %{repo: repo} do
      assert :ok == GS.call(repo, {:close})
      # Process is dead
      refute Process.alive?(repo)
    end

    test "can't close the connection while in a transaction", %{repo: repo} do
      # We are in a transaction
      assert :ok == GS.call(repo, {:begin, :exclusive})
      assert :sys.get_state(repo).transaction_id

      # Can't close this
      log =
        capture_log(fn ->
          assert {:error, :cant_close_with_transaction} ==
                   GS.call(repo, {:close})

          assert Process.alive?(repo)
        end)

      assert log =~ "[error] Tried to close a Repo while in a transaction"

      # But once we commit...
      assert :ok == GS.call(repo, {:commit})

      # We can close it
      assert :ok == GS.call(repo, {:close})
      refute Process.alive?(repo)
    end
  end

  describe "handle_call: mgt_connection_released" do
    test "performs a no-op on the regular lifecycle", %{repo: repo} do
      # This Repo had a regular lifecycle: it started a transaction
      assert :ok == GS.call(repo, {:begin, :exclusive})
      assert :sys.get_state(repo).transaction_id

      # And then it committed, which resets the transaction_id
      assert :ok == GS.call(repo, {:commit})
      state_after_commit = :sys.get_state(repo)
      refute state_after_commit.transaction_id

      assert :ok == GS.call(repo, {:mgt_connection_released})
      state_after_release = :sys.get_state(repo)

      # Nothing changes when the connection is released
      assert state_after_release == state_after_commit
    end

    test "rolls back transaction if one exists", %{repo: repo} do
      # We are in a transaction
      assert :ok == GS.call(repo, {:begin, :exclusive})
      assert :sys.get_state(repo).transaction_id

      # Let's assume mgt_connection_released was called with an active transaction -- that means the
      # caller was forced to end its connection (e.g. due to timeout or caller dying)
      assert :ok == GS.call(repo, {:mgt_connection_released})
      refute :sys.get_state(repo).transaction_id

      # If we try to ROLLBACK, SQLite yells at us saying there's no open transaction
      c = :sys.get_state(repo).conn
      assert {:error, "cannot rollback - no transaction is active"} = SQLite.raw(c, "ROLLBACK")
    end

    @tag capture_log: true
    test "only the repo manager can send the release signal", %{shard_id: shard_id, db: db} do
      assert {:ok, repo} = Repo.start_link({@context, shard_id, db, :readwrite, self()})

      # It works if called from the same process (here, test process == manager process)
      assert :ok == GS.call(repo, {:mgt_connection_released})

      # It blows up if called from elsewhere
      spawn(fn ->
        GS.call(repo, {:mgt_connection_released})
        block_forever()
      end)

      # The Repo died abruptly because someone other than the Manager tried to release it
      Process.flag(:trap_exit, true)
      assert_receive {:EXIT, ^repo, {%RuntimeError{message: error}, _stacktrace}}, 100
      assert error =~ "Repo can only be released by its Manager"
    end
  end
end
