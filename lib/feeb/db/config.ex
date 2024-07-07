defmodule Feeb.DB.Config do
  # TODO: maybe create a `validate_config` that runs (and fails) on boot

  @default_migration_dir "priv/migrations"
  @default_schemas_list_file "priv/feebdb_schemas.json"

  def contexts do
    Application.get_env(:feebdb, :contexts)
    |> format_contexts()
  end

  def get_schemas_list_path,
    do: Application.get_env(:feebdb, :schemas_list, @default_schemas_list_file)

  def data_dir,
    do: Application.get_env(:feebdb, :data_dir)

  def migrations_path,
    do: Application.get_env(:feebdb, :migrations_dir, @default_migration_dir)

  def queries_path do
    # TODO: If this is used (it is), then it is untested
    "priv/queries/**/*.sql"
  end

  defp format_contexts(contexts) do
    Enum.map(contexts, fn {name, ctx_data} ->
      %{
        name: name,
        shard_type: ctx_data.shard_type,
        domains: ctx_data[:domains] || [name]
      }
    end)
  end
end
