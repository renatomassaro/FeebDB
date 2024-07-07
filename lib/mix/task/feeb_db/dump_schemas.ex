defmodule Mix.Tasks.FeebDb.DumpSchemas do
  use Mix.Task
  alias Feeb.DB.{Migrator, SQLite}

  # TODO: Get these paths from args instead of hard-coding them here
  @tmp_db "/tmp/helix_tmp_dump.db"
  @output_path "priv/schemas"

  @contexts [:lobby]

  @impl Mix.Task
  def run(_) do
    {t, _} = :timer.tc(fn -> do_run() end)
    IO.puts("Dumped all schemas in #{trunc(t / 1000)}ms")
  end

  defp do_run do
    Enum.each(@contexts, &dump_schema/1)
  end

  defp dump_schema(context) do
    File.rm(@tmp_db)

    # Prepare env
    Migrator.setup()

    # Migrate
    {:ok, conn} = SQLite.open(@tmp_db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")

    {:needs_migration, migrations} = Migrator.get_migration_status(conn, context, :readwrite)

    :ok = Migrator.migrate(conn, migrations)

    output_file = "#{@output_path}/#{context}.sql"

    # Dump schema
    {_, 0} = System.shell("sqlite3 #{@tmp_db} .dump > #{output_file}")

    # Cleanup
    File.rm(@tmp_db)
  end
end
