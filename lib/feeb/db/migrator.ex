defmodule Feeb.DB.Migrator do
  require Logger
  alias Feeb.DB.{Config, SQLite}
  alias Feeb.DB.Migrator.{Metadata, Parser}

  @env Mix.env()

  @doc """
  On application startup, iterate over all the migrations and cache their
  values, as well as the latest migration in file. These will be used later on,
  and they are used in critical paths, hence the caching.

  In production we always rely on persistent terms, but for tests we can specify
  custom values per process.
  """
  def setup do
    # Cache migrations
    all_migrations = calculate_all_migrations()
    setup(all_migrations, :persistent_term)
  end

  def setup(all_migrations, location) do
    cache_all_migrations(all_migrations, location)

    # Cache `latest_version` for each domain
    all_migrations
    |> Map.keys()
    |> Enum.each(fn domain ->
      latest_version = calculate_latest_version(domain, all_migrations)
      cache_latest_version(domain, latest_version, location)
    end)
  end

  # Checks if the DB needs migration, which contexts need migration etc
  def get_migration_status(conn, context, mode) do
    setup_check_result =
      if Metadata.is_set_up?(conn) do
        :ok
      else
        if mode == :readonly do
          {:error, :needs_write_access}
        else
          Metadata.setup(conn)
          :ok
        end
      end

    case setup_check_result do
      :ok ->
        conn
        |> Metadata.summarize_migrations(context)
        |> Enum.reduce({:migrated, []}, fn {domain, v}, acc ->
          latest_version = get_latest_version(domain)

          if latest_version == v do
            acc
          else
            {_, acc_migs} = acc
            {:needs_migration, [{domain, v, latest_version} | acc_migs]}
          end
        end)
        |> case do
          {:migrated, []} -> :migrated
          {:needs_migration, migs} -> {:needs_migration, migs}
        end

      {:error, :needs_write_access} ->
        :needs_write_access
    end
  end

  def migrate(conn, domains_to_migrate) do
    migrations = get_all_migrations()

    Enum.each(domains_to_migrate, fn {domain, cur_v, latest_v} ->
      if cur_v < latest_v do
        missing_versions =
          migrations
          |> Map.fetch!(domain)
          |> Map.keys()
          |> Enum.reject(fn v -> v <= cur_v end)
          |> Enum.sort()

        SQLite.raw!(conn, "BEGIN EXCLUSIVE")
        migrate_next!(conn, migrations, domain, missing_versions)
        SQLite.raw!(conn, "COMMIT")
      else
        :noop
      end
    end)
  end

  defp migrate_next!(conn, migrations, domain, [v | next_migrations]) do
    apply_migration!(conn, migrations, domain, v)
    Metadata.insert_migration(conn, domain, v)

    # Keep migrating until all missing migrations are applied
    migrate_next!(conn, migrations, domain, next_migrations)
  end

  defp migrate_next!(_, _, _, []), do: :ok

  # NOTE: Performance-wise, this is a low hanging fruit. While `queries_from_sql_lines/1` could
  # be substantially improved, it is fast enough. However, imagine one is migrating thousands of
  # shards. The migrations will be parsed again for each shard. We'd benefit from some cache.
  defp apply_migration!(conn, migrations, domain, v) do
    migration = get_in(migrations, [domain, v])

    case migration do
      {:sql_only, sql_file} ->
        Logger.info("Running SQL-only migration #{domain}@#{v}")

        # Simply run the sql file directly, line by line
        sql_file
        |> File.read!()
        |> Parser.queries_from_sql_lines()
        |> Enum.each(fn query -> SQLite.raw!(conn, query) end)

      {:exs_only, _path} ->
        raise "Not supported yet"

      {:sql_and_exs, _paths} ->
        # DB.Query.compile(sql_file, :migration)
        # [{mod, _}] = Code.compile_file(exs_file)
        # apply(mod, :change, [conn])
        raise "Not supported yet"

      nil ->
        raise "Migration not found: #{domain}@#{v}"
    end
  end

  @doc """
  Returns a map with all migrations and the corresponding .exs and/or .sql
  files. Notice that the result of this function is the same for every shard,
  regardless of their version. As such, it may make sense to memoize its result.
  """
  def calculate_all_migrations do
    [Config.migrations_path()]
    |> Enum.uniq()
    |> Enum.map(fn path -> calculate_all_migrations(path) end)
    |> Enum.reduce(%{}, fn migs, acc ->
      Map.merge(migs, acc)
    end)
  end

  defp calculate_all_migrations(base_path) do
    dir_length = String.length(base_path)

    "#{base_path}/**/*.{exs,sql}"
    |> Path.wildcard()
    |> Enum.map(fn path ->
      [_, domain, file_name] =
        path
        |> String.slice(dir_length..-1//1)
        |> String.split("/")

      version =
        file_name
        |> String.split("_")
        |> List.first()
        |> Integer.parse()
        |> elem(0)

      {String.to_atom(domain), version, path}
    end)
    |> Enum.group_by(fn {domain, _, _} -> domain end)
    |> Enum.map(fn {domain, domain_entries} ->
      entries =
        domain_entries
        |> Enum.group_by(fn {_, v, _} -> v end)
        |> Enum.map(fn {v, path_entries} ->
          path_entries = Enum.map(path_entries, fn {_domain, _v, path} -> path end)

          {type, path_entries} =
            case path_entries do
              [path] ->
                if String.ends_with?(path, ".sql"),
                  do: {:sql_only, path},
                  else: {:exs_only, path}

              [_ | _] ->
                {:sql_and_exs, path_entries}
            end

          {v, {type, path_entries}}
        end)
        |> Map.new()

      {domain, entries}
    end)
    |> Map.new()
  end

  @doc """
  Based on the migrations found in the local filesystem, return the latest one
  for the given domain.
  """
  def calculate_latest_version(domain, migrations) do
    migrations
    |> Map.fetch!(domain)
    |> Map.keys()
    |> Enum.sort(:desc)
    |> List.first()
  end

  @doc """
  Stores the migrations in a persistent term for fast lookup
  """
  def cache_all_migrations(migrations, location) do
    case location do
      :persistent_term ->
        :persistent_term.put({:migrator, :all_migrations}, migrations)

      :process ->
        Process.put({:migrator, :all_migrations}, migrations)
    end
  end

  @doc """
  Stores the latest version for the domain in a persistent term for fast lookup
  """
  def cache_latest_version(domain, latest_value, location) do
    case location do
      :persistent_term ->
        :persistent_term.put({:migrator, :latest_version, domain}, latest_value)

      :process ->
        Process.put({:migrator, :latest_version, domain}, latest_value)
    end
  end

  if @env == :test do
    def get_all_migrations do
      if Process.get({:migrator, :all_migrations}, false) do
        Process.get({:migrator, :all_migrations})
      else
        :persistent_term.get({:migrator, :all_migrations})
      end
    end
  else
    def get_all_migrations do
      :persistent_term.get({:migrator, :all_migrations})
    end
  end

  if @env == :test do
    def get_latest_version(domain) do
      if Process.get({:migrator, :latest_version, domain}, false) do
        Process.get({:migrator, :latest_version, domain}, 0)
      else
        :persistent_term.get({:migrator, :latest_version, domain}, 0)
      end
    end
  else
    def get_latest_version(domain) do
      :persistent_term.get({:migrator, :latest_version, domain}, 0)
    end
  end
end
