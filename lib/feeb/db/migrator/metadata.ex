defmodule Feeb.DB.Migrator.Metadata do
  require Logger

  alias Feeb.DB.SQLite

  @migrations_table "__db_migrations"
  @summary_table "__db_migrations_summary"

  # TODO: Precisa ser configuravel / vir da AppConfig
  # TODO: In the future this should be kept in `__db_apps` but for now that's GoodEnough(TM)
  def initial_summary_for_context(:lobby), do: %{lobby: 0}
  def initial_summary_for_context(:singleplayer), do: %{game: 0}
  def initial_summary_for_context(:multiplayer), do: %{game: 0}
  def initial_summary_for_context(:test), do: %{test: 0}
  def initial_summary_for_context(:raw), do: %{raw: 0}
  def initial_summary_for_context(:saas_prod_one), do: %{events: 0, crm: 0}
  def initial_summary_for_context(:saas_prod_two), do: %{events: 0, erp: 0}

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
    base_summary = initial_summary_for_context(context)

    """
    SELECT * FROM #{@summary_table}
    """
    |> SQLite.raw2!(conn)
    |> Enum.reduce(base_summary, fn [domain, version, _], acc ->
      Map.put(acc, String.to_atom(domain), version)
    end)
  end

  def is_set_up?(conn) do
    case SQLite.raw!(conn, "PRAGMA table_info(#{@summary_table})") do
      [] -> false
      [_ | _] -> true
    end
  end
end
