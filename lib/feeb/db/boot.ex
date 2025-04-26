defmodule Feeb.DB.Boot do
  @moduledoc false

  require Logger
  alias Feeb.DB.{Config, Migrator, Query, Schema}

  @boot_indicator_key :feebdb_finished_booting

  def run do
    {t, _} = :timer.tc(fn -> migrate_shards() end)
    Logger.info("Shards migrated in #{trunc(t / 1000)}ms")

    {t, _} = :timer.tc(fn -> setup() end)
    Logger.info("DB setup completed in #{trunc(t / 1000)}ms")

    :persistent_term.put(@boot_indicator_key, true)
  end

  def wait_boot!(n \\ 0) do
    cond do
      :persistent_term.get(@boot_indicator_key, false) ->
        {:ok, n}

      n == 500 ->
        raise "Timeout"

      :else ->
        :timer.sleep(10)
        wait_boot!(n + 1)
    end
  end

  ##################################################################################################
  # Functions below this point are internal. They may be public for testing purposes only, but they
  # should not be called directly. Only Boot.run/0 is a valid entrypoint for this module.
  # One acceptable exception is the `mix feeb_db.migrate` task.
  ##################################################################################################

  ##################################################################################################
  # Shard migration
  ##################################################################################################

  def migrate_shards do
    Migrator.setup()

    Enum.each(Config.contexts(), fn context ->
      # Make sure the context is fully set up and ready to migrate shards
      bootstrap_context(context)

      # Migrate all shards (global, setup and application ones)
      :done = migrate_global_shards(context)
      :done = migrate_setup_shards(context)
      :done = migrate_application_shards(context)
    end)
  end

  defp bootstrap_context(context) do
    context_data_path = "#{Config.data_dir()}/#{context.name}/"
    File.mkdir_p(context_data_path)
  end

  defp migrate_global_shards(%{shard_type: :global} = context) do
    migrate_shard(context, 1)
    :done
  end

  defp migrate_global_shards(_), do: :done

  defp migrate_application_shards(context) do
    context
    |> get_shards_for_context()
    |> Stream.each(fn shard_id -> migrate_shard(context, shard_id) end)
    |> Stream.run()

    :done
  end

  defp migrate_setup_shards(context) do
    migrate_shard(context, -1)
    :done
  end

  # Shards of ID -99 are for internal testing and are not migrated by default
  defp migrate_shard(_, -99), do: :ok

  defp migrate_shard(context, shard_id) do
    # We automatically migrate the shard by simply opening and closing a connection to the shard
    Feeb.DB.begin(context.name, shard_id, :write)
    Feeb.DB.commit()
    :ok
  end

  defp get_shards_for_context(context) do
    base_path = "#{Config.data_dir()}/#{context.name}/"
    base_path_len = String.length(base_path)

    # TODO: This function stores all shards (paths) in memory, which may not be a good idea when
    # handling thousands of shards. Benchmark, see how the code reacts and refactor if needed.
    # TODO: Support "shard of shards" directory hierarchy
    "#{base_path}/*.db"
    |> Path.wildcard()
    |> Stream.map(fn path ->
      path
      |> String.slice(base_path_len..-4//1)
      |> String.to_integer()
    end)
  end

  ##################################################################################################
  # Setup
  ##################################################################################################

  def setup do
    all_contexts = Config.contexts()
    all_models = get_all_models()
    all_queries = get_all_queries()
    compile_queries(all_queries, all_contexts)

    Enum.each(all_contexts, fn context ->
      save_database_metadata(all_models, context)
      Feeb.DB.begin(context.name, -1, :read)
      validate_database(all_models, context.name)
      Feeb.DB.commit()
    end)
  end

  def get_all_models do
    Schema.List.all()
    |> Enum.map(fn {context, modules} ->
      modules_details =
        Enum.map(modules, fn mod ->
          true = is_atom(mod.__table__())
          {context, mod, mod.__table__(), mod.__schema__()}
        end)

      {context, modules_details}
    end)
  end

  def get_all_queries do
    Config.queries_search_path()
    |> Path.wildcard()
    |> Enum.map(fn path ->
      [raw_domain, raw_context | _] =
        path
        |> String.split("/")
        |> Enum.reverse()

      domain =
        raw_domain
        |> String.split(".")
        |> List.first()
        |> String.to_atom()

      {String.to_atom(raw_context), domain, path}
    end)
  end

  def compile_queries(all_queries, all_contexts) do
    # TODO: Rethink the names here. Sometimes I'm conflating context and domain. It's confusing.
    # Compile each context query
    all_queries
    |> Enum.each(fn {context, domain, path} ->
      Query.compile(path, {context, domain})
    end)

    # TODO: Test this directly (within Feeb.DB; it is tested indirectly in HE)
    # Copy queries from another domain
    # Some contexts use queries from a different domain. For example, in HE the Singleplayer and
    # Multiplayer contexts each use queries from the same Game domain.
    all_contexts
    # Ignore contexts that don't use another domain
    |> Enum.reject(fn %{name: name, domains: domains} -> domains == [name] end)
    |> Enum.map(fn %{name: name, domains: domains} ->
      # Now we get all queries from each `domain` and replace them with the context (`name`)
      domains
      |> Enum.map(fn domain ->
        all_queries
        |> Enum.filter(fn {c, _, _} -> c == domain end)
        |> Enum.each(fn {_context, table, path} ->
          Query.compile(path, {name, table})
        end)
      end)
    end)

    # TODO: Also compile "empty" (non-existent) query files, since we may only use
    # the templated queries and that should still be fine
  end

  def save_database_metadata(all_models, %{name: ctx_name, domains: ctx_domains}) do
    # Save models based on their defined context
    all_models
    |> Enum.filter(fn {ctx, _} -> ctx == ctx_name end)
    |> Enum.each(fn {_, modules_details} ->
      Enum.each(modules_details, fn model ->
        save_model(model)
      end)
    end)

    # Copy models from another domain into the context of `ctx_name`
    # Usage example: Multiplayer/Singleplayer contexts share the Game domain
    ctx_domains
    |> Enum.reject(fn entry -> entry == ctx_name end)
    |> Enum.map(fn domain_name ->
      all_models
      |> Enum.filter(fn {ctx, _} -> ctx == domain_name end)
      |> Enum.each(fn {_, modules_details} ->
        Enum.each(modules_details, fn {_ctx, model, table, schema} ->
          # This is the same `model` as above, except that it had its original context name switched
          # with the `ctx_name` defined in the configuration settings
          new_model = {ctx_name, model, table, schema}
          save_model(new_model)
        end)
      end)
    end)
  end

  def validate_database(all_models, context) do
    all_models
    |> Enum.filter(fn {ctx, _} -> ctx == context end)
    |> Enum.each(fn {_, modules_details} ->
      Enum.each(modules_details, fn {_, _, table, _} = model ->
        {:ok, table_info} = Feeb.DB.raw("PRAGMA table_info(#{table})")
        validate_table_info!(model, table_info)
      end)
    end)
  end

  defp validate_table_info!({context, model, _table, schema}, table_info) do
    # Fields defined in the Schema (not counting virtual ones)
    schema_table_fields = model.__cols__()

    # Fields found in the database
    table_fields =
      Enum.map(table_info, fn [_, field, _, _, _, _] ->
        String.to_atom(field)
      end)

    # They should match. If they don't, one of the two is out-of-sync
    if length(table_fields) != length(schema_table_fields) do
      extra_fields =
        if length(table_fields) > length(schema_table_fields),
          do: table_fields -- schema_table_fields,
          else: schema_table_fields -- table_fields

      "Schema fields and #{context}@#{model} fields do not match: #{inspect(extra_fields)}"
      |> raise()
    end

    Enum.each(table_info, fn [_, field, sqlite_type, _nullable?, _default, _pk?] ->
      # TODO: Validate nullable
      # TODO: Validate PK
      field = String.to_atom(field)

      if not Map.has_key?(schema, field),
        do: raise("Unable to find field #{field} on #{model}")

      {type_module, _, _} = Map.fetch!(schema, field)
      field_type = type_module.sqlite_type()

      case {sqlite_type, field_type} do
        {"INTEGER", :integer} ->
          :ok

        {"TEXT", :text} ->
          :ok

        {"REAL", :real} ->
          :ok

        {"BLOB", :blob} ->
          :ok

        _ ->
          raise "Type mismatch: #{sqlite_type}/#{field_type} for #{field} @ #{model}"
      end
    end)
  end

  defp save_model({context, model, table, _}) do
    :persistent_term.put({:db_table_models, {context, table}}, model)
  end
end
