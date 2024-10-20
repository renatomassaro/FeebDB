defmodule Mix.Tasks.FeebDb.MigrateTest do
  use Test.Feeb.DBCase, async: true
  alias Mix.Tasks.FeebDb.Migrate, as: MigrateTask
  alias Feeb.DB.SQLite

  @moduletag db: :raw

  describe "run/1" do
    test "migrates all shards", %{db: db} = ctx do
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
  end

  defp open_conn(db) do
    {:ok, conn} = SQLite.open(db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")
    conn
  end
end
