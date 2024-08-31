defmodule Feeb.DB.Migrator.Metadata do
  require Logger

  alias Feeb.DB.{Config, SQLite}

  @migrations_table "__db_migrations"
  @summary_table "__db_migrations_summary"

  @doc """
  Sets up all the metadata tables required by Migrator to operate properly.
  """
  def setup(conn) do
    Logger.info("Setting up metadata table")

    """
    CREATE TABLE #{@migrations_table} (
      domain text,
      version integer,
      inserted_at TEXT NOT NULL,
      PRIMARY KEY (domain, version)
    ) STRICT;
    """
    |> SQLite.raw2!(conn)

    """
    CREATE TABLE #{@summary_table} (
      domain text,
      version integer,
      inserted_at TEXT NOT NULL,
      PRIMARY KEY (domain, version)
    ) STRICT;
    """
    |> SQLite.raw2!(conn)
  end

  @doc """
  Inserts a new migration entry in the database.
  """
  def insert_migration(conn, domain, version) do
    """
    INSERT INTO #{@migrations_table}
      (domain, version, inserted_at)
    VALUES
      ('#{domain}', #{version}, datetime());
    """
    |> SQLite.raw2!(conn)

    """
    DELETE FROM #{@summary_table}
    WHERE domain = '#{domain}';
    """
    |> SQLite.raw2!(conn)

    """
    INSERT INTO #{@summary_table}
      (domain, version, inserted_at)
    VALUES
      ('#{domain}', #{version}, datetime());
    """
    |> SQLite.raw2!(conn)
  end

  @doc """
  Returns a summary of the migrations, with the latest version for each domain.
  """
  def summarize_migrations(conn, context) do
    # The "initial" summary is basically a map of all possible domains with no migrations applied to
    # them. We will use this as a starting point, in case the shard we are connecting to does not
    # have yet a fully set up `@summary_table`.
    initial_summary = initial_summary_for_context(context)

    """
    SELECT * FROM #{@summary_table}
    """
    |> SQLite.raw2!(conn)
    |> Enum.reduce(initial_summary, fn [domain, version, _], acc ->
      Map.put(acc, String.to_atom(domain), version)
    end)
  end

  def is_set_up?(conn) do
    case SQLite.raw!(conn, "PRAGMA table_info(#{@summary_table})") do
      [] -> false
      [_ | _] -> true
    end
  end

  defp initial_summary_for_context(context) do
    Config.contexts()
    |> Enum.find(fn %{name: name} -> name == context end)
    |> Map.fetch!(:domains)
    |> Enum.map(fn domain -> {domain, 0} end)
    |> Map.new()
  end
end
