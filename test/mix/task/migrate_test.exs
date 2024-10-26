defmodule Mix.Tasks.FeebDb.MigrateTest do
  # `async: false` on purpose, read comments below to understand why
  use Test.Feeb.DBCase, async: false
  alias Mix.Tasks.FeebDb.Migrate, as: MigrateTask
  alias Feeb.DB.{Config, SQLite}

  @moduletag db: :raw

  describe "run/1" do
    test "migrates all shards", %{db: db} = ctx do
      # Every test has the capability to create its own shard, which is fully isolated from any
      # other test. As scuh, it's very common for me to not close/release/commit connections that
      # were opened during the test, resulting in these shards being effectively locked.
      # That's not a problem, since these shards are never to be used inside another test. However,
      # when triggering `feeb_db.migrate`, we end up migrating every shard that is in the data
      # directory, many of which are locked because the Repo has an open transaction.
      # As a workaround, I'm simply deleting every shard (except the one in this test), which is why
      # this test needs to run with `async: false`.
      # Note, however, that _even_ if I closed the connections from every test, running _this_ test
      # with `async: true` would probably result in flakes: it's possible (or, rather, certain) that
      # some test would have its shard migrated midway, causing inconsistencies or unexpected locks.
      # In the future, it may make sense to refactor the `feeb_db.migrate` task to accept arguments
      # that specify a custom data directory. By using a custom data dir, I'd be able to "mock" the
      # shards without affecting other tests, and then we could use `async: true` here.
      delete_all_shards_but_this_one(db)

      # We are using a `raw` DB, meaning it has nothing in it (100% fresh DB)
      assert ctx.db_context == :raw

      # Proof: users/sessions tables from lobby are not present:
      conn = open_conn(db)
      assert {:ok, []} == SQLite.raw(conn, "pragma table_info(users)")
      assert {:ok, []} == SQLite.raw(conn, "pragma table_info(sessions)")
      SQLite.close(conn)

      # Let's move this DB from the `raw` domain to the `lobby` domain.
      lobby_db = String.replace(db, "/raw/", "/lobby/")
      :ok = File.cp(db, lobby_db)

      # Trigger the migrate command. Every shard (for every context) will be migrated.
      assert :ok == MigrateTask.run([])

      # One of the migrated shards is the newest lobby one (i.e. `lobby_db`)
      # The `users` and `sessions` table are now set, as one would expect
      conn = open_conn(lobby_db)
      refute {:ok, []} == SQLite.raw(conn, "pragma table_info(users)")
      refute {:ok, []} == SQLite.raw(conn, "pragma table_info(sessions)")
      assert {:ok, [["lobby", 2]]} == SQLite.raw(conn, "select * from __db_migrations_summary")
      SQLite.close(conn)
    end

    test "sets up the shard directory (if one doesn't exist)", %{db: db} do
      # Read comment on test above to understand why we need to do this
      delete_all_shards_but_this_one(db)

      # Create a `fake_context` that never existed until now
      # Another reason why this suite must run with `async: false`: we are hijacking the config
      original_contexts = Application.get_env(:feebdb, :contexts)
      new_contexts = Map.put(original_contexts, :fake_context, %{shard_type: :dedicated})
      Application.put_env(:feebdb, :contexts, new_contexts)

      # The `fake_context` is there
      assert Enum.find(Config.contexts(), &(&1.name == :fake_context))

      # Before the migration, its directory did not exist
      ctx_dir = Path.join(Config.data_dir(), "/fake_context")
      File.rmdir(ctx_dir)
      assert {:error, :enoent} = File.stat(ctx_dir)

      # But it exists after the migration
      assert :ok == MigrateTask.run([])
      assert {:ok, %{type: :directory}} = File.stat(ctx_dir)
    end
  end

  defp delete_all_shards_but_this_one(db) do
    "#{Config.data_dir()}/**/*.db"
    |> Path.wildcard()
    |> Enum.reject(fn path -> path == db end)
    |> Enum.each(fn path -> File.rm(path) end)
  end

  defp open_conn(db) do
    {:ok, conn} = SQLite.open(db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")
    conn
  end
end
