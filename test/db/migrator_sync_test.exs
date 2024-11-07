defmodule Feeb.Db.MigratorSyncTest do
  # This is an `async: false` version of `Feeb.DB.MigratorTest` for specific tests
  use Test.Feeb.DBCase, async: false

  alias Feeb.DB.{Migrator, SQLite}

  @moduletag db: :raw

  describe "migrate/2" do
    test "migrations are atomic (ACID)", %{db: db} do
      # CONTEXT: Please read `priv/test/migrations/will_fail/1_partially_correct_migration.sql` for
      # full context, but basically we are executing 2 DDL statements: a CREATE TABLE and a CREATE
      # INDEX. The first will succeed while the second will fail. We need to ensure that the
      # migration rolled back entirely (i.e. it was an "all or nothing" operation).
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)

      {:ok, conn} = SQLite.open(db)
      SQLite.raw!(conn, "PRAGMA synchronous=1")
      assert [] == SQLite.raw!(conn, "pragma table_info(users)")

      # Another reason why this suite must run with `async: false`: we are hijacking the config
      original_contexts = Application.get_env(:feebdb, :contexts)
      new_contexts = Map.put(original_contexts, :will_fail, %{shard_type: :dedicated})
      Application.put_env(:feebdb, :contexts, new_contexts)

      assert {:needs_migration, migrations} =
               Migrator.get_migration_status(conn, :will_fail, :readwrite)

      assert [] == SQLite.raw!(conn, "pragma table_info(users)")

      # Attempt to migrate raised an error
      %{term: {:error, reason}} =
        assert_raise(MatchError, fn ->
          Migrator.migrate(conn, migrations)
        end)

      assert reason =~ "no such column: username"

      # NOTE: We need to re-open the connection because _technically_ we never really rolled back,
      # instead we simply never COMMITted. That's fine and harmless for now, but in the future we
      # would benefit from better error-handling. When that time comes, re-opening the connection
      # will no longer be necessary.
      SQLite.close(conn)
      {:ok, conn} = SQLite.open(db)

      # Even though `CREATE TABLE` succeeded, there's no table because `CREATE INDEX` failed
      assert [] == SQLite.raw!(conn, "pragma table_info(users)")

      # Resume the original contexts (other synchronous tests may be affected otherwise)
      Application.put_env(:feebdb, :contexts, original_contexts)
    end
  end
end
