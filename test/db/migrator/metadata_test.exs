defmodule Feeb.DB.Migrator.MetadataTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB.{SQLite}
  alias Feeb.DB.Migrator.Metadata

  @moduletag db: :raw

  @migrations_table "__db_migrations"
  @summary_table "__db_migrations_summary"

  setup %{db: db} do
    {:ok, conn} = SQLite.open(db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")
    {:ok, %{conn: conn}}
  end

  def with_migrator_setup(%{conn: conn}) do
    Metadata.setup(conn)
    {:ok, %{}}
  end

  describe "setup/1" do
    test "creates the expected tables", %{conn: conn} do
      # Initially, there's no migrations or summary tables
      assert [] = SQLite.raw!(conn, "pragma table_info(#{@migrations_table})")
      assert [] = SQLite.raw!(conn, "pragma table_info(#{@summary_table})")

      Metadata.setup(conn)

      # Now the migrations table is properly set up
      assert [
               [0, "domain", "TEXT", 1, nil, 1],
               [1, "version", "INTEGER", 1, nil, 2],
               [2, "inserted_at", "TEXT", 1, nil, 0]
             ] = SQLite.raw!(conn, "pragma table_info(#{@migrations_table})")

      # And so is the summary table
      assert [
               [0, "domain", "TEXT", 1, nil, 1],
               [1, "version", "INTEGER", 1, nil, 2],
               [2, "inserted_at", "TEXT", 1, nil, 0]
             ] = SQLite.raw!(conn, "pragma table_info(#{@summary_table})")
    end
  end

  describe "insert_migration/3" do
    setup [:with_migrator_setup]

    test "inserts migration", %{conn: conn} do
      list_migrations = "SELECT * FROM #{@migrations_table} ORDER BY domain ASC, version DESC"

      # Initially we have no migrations
      assert [] == SQLite.raw!(conn, list_migrations)

      # Insert a new migration entry
      Metadata.insert_migration(conn, :core, 1)

      # We now have one migration!
      assert [["core", 1, date_1]] = SQLite.raw!(conn, list_migrations)
      assert String.length(date_1) == 19

      # And another!
      Metadata.insert_migration(conn, :core, 2)

      assert [["core", 2, _], ["core", 1, _]] = SQLite.raw!(conn, list_migrations)

      # And another, now from a different domain
      Metadata.insert_migration(conn, :mob, 1)

      assert [["core", 2, _], ["core", 1, _], ["mob", 1, _]] = SQLite.raw!(conn, list_migrations)

      # The summary table was updated correctly
      assert [["core", 2, _], ["mob", 1, _]] =
               SQLite.raw!(conn, "SELECT * FROM #{@summary_table} ORDER BY domain ASC")
    end
  end

  describe "summarize_migrations/2" do
    setup [:with_migrator_setup]

    test "returns the latest version for each domain", %{conn: conn} do
      # Core will be at 4
      Metadata.insert_migration(conn, :core, 1)
      Metadata.insert_migration(conn, :core, 2)
      Metadata.insert_migration(conn, :core, 3)
      Metadata.insert_migration(conn, :core, 4)

      # Mob will be at 2
      Metadata.insert_migration(conn, :mob, 1)
      Metadata.insert_migration(conn, :mob, 2)

      # Inventory will be at 1
      Metadata.insert_migration(conn, :inventory, 1)

      # Test will be at 0 (this data comes from the initial summary)
      assert %{core: 4, mob: 2, inventory: 1, test: 0} == Metadata.summarize_migrations(conn, :test)
    end
  end
end
