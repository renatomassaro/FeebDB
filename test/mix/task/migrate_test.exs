defmodule Mix.Tasks.FeebDb.MigrateTest do
  # `async: false` on purpose, read comments below to understand why
  use Test.Feeb.DBCase, async: false
  alias Mix.Tasks.FeebDb.Migrate, as: MigrateTask
  alias Feeb.DB.{Config, SQLite}

  @moduletag db: :raw

  describe "run/1" do
    test "migrates all shards", %{db: db} = ctx do
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)

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
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)

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

      # Restore the original contexts (other synchronous tests may be affected otherwise)
      Application.put_env(:feebdb, :contexts, original_contexts)
    end
  end

  defp open_conn(db) do
    {:ok, conn} = SQLite.open(db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")
    conn
  end
end
