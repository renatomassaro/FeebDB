defmodule Feeb.DB.Boot do
  @moduledoc """
  NOTE: I have contexts and domains. Using mob as an example:
  `lobby` and `mob` are contexts. Lobby has `lobby` and `events` domains, whereas
  `mob` has `core`, `crm`, `mob` and `events` domains

  Using he:
  We only have `lobby` context with `lobby` domain, they map 1:1.
  Most of the time (I think) we don't need shared domains, but that's supported.
  """

  require Logger
  alias Feeb.DB.{Config, Migrator, Query, Repo, Schema}

  @env Mix.env()
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

  ##############################################################################
  # Functions below this point are internal. They may be public for testing
  # purposes only, but they should not be called directly. Only Boot.run/0 is a
  # valid entrypoint for this module.
  ##############################################################################

  ##############################################################################
  # Shard migration
  ##############################################################################

  def migrate_shards do
    Migrator.setup()

    Enum.each(Config.contexts(), fn context ->
      :done = migrate_shards_for_context(context)
      :done = migrate_setup_shards_for_context(context)
    end)
  end

  defp migrate_shards_for_context(context, n \\ 1) do
    case do_migrate_shard(context, n) do
      :ok -> migrate_shards_for_context(context, n + 1)
      {:error, :shard_not_found} -> :done
    end
  end

  defp migrate_setup_shards_for_context(context),
    do: :ok == do_migrate_shard(context, -1) && :done

  defp do_migrate_shard(context, shard_id) do
    path = Repo.get_path(context.name, shard_id)

    continue_migrating? =
      cond do
        # Global shards have a single shard, so we can stop here
        context.shard_type == :global and shard_id > 1 ->
          false

        # Setup shard should always be migrated
        shard_id == -1 ->
          true

        # If the file does not exist, create if global shard. Otherwise, stop migrating
        {:error, :enoent} == File.stat(path) ->
          context.shard_type == :global and shard_id == 1

        :else ->
          true
      end

    if continue_migrating? do
      Feeb.DB.begin(context.name, shard_id, :write)
      Feeb.DB.commit()
      :ok
    else
      {:error, :shard_not_found}
    end
  end

  ##############################################################################
  # Setup
  ##############################################################################

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
    # TODO: Schema.List should be generated from a JSON that lives in the application's priv/ folder
    Schema.List.all()
    |> filter_models_by_env()
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
    # TODO: Make it configurable so it works with test/prod queries
    Config.queries_path()
    |> Path.wildcard()
    |> Enum.map(fn path ->
      name =
        path
        |> String.slice(13..-1//1)
        |> String.split(".")
        |> List.first()

      [context, domain] =
        name
        |> String.split("/")
        |> Enum.map(&String.to_atom/1)

      {context, domain, path}
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
        save_table_fields(model)
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
          save_table_fields(new_model)
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

  defp validate_table_info!({model, context, _table, schema}, table_info) do
    table_fields =
      Enum.map(table_info, fn [_, field, _, _, _, _] ->
        String.to_atom(field)
      end)

    if length(table_fields) != length(Map.keys(schema)) do
      schema_fields = Map.keys(schema)

      extra_fields =
        if length(table_fields) > length(schema_fields),
          do: table_fields -- schema_fields,
          else: schema_fields -- table_fields

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

  defp save_table_fields({_context, model, _table, _schema}) do
    fields = model.__cols__()
    :persistent_term.put({:db_table_fields, model}, fields)
  end

  defp save_model({context, model, table, _}) do
    :persistent_term.put({:db_table_models, {context, table}}, model)
  end

  defp filter_models_by_env(models) do
    # This can be turned into a config, like: test_contexts
    # TODO: I think this is not needed because we filter models when saving metadata
    if @env != :test do
      # Enum.reject(models, fn {domain, _} -> domain == :test end)
      Map.drop(models, [:test])
    else
      models
    end
  end
end