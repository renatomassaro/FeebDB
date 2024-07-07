defmodule Feeb.DB.Schema.List do
  @moduledoc """
  This module is generated automatically via `mix db.schema.list`.

  It is used by DB.Boot to load all existing tables and verify their
  SQLite schemas match the schemas defined in the codebase.
  """

  alias Feeb.DB.Config

  def get_schemas do
    path = Config.get_schemas_list_path()

    raw_schemas =
      case File.read(path) do
        {:ok, contents} ->
          contents

        {:error, reason} ->
          raise "Unable to read feebdb_schemas file at #{path}: #{inspect(reason)}"
      end

    raw_schemas
    |> :json.decode()
    |> Enum.map(fn {context, modules} ->
      {String.to_atom(context), Enum.map(modules, &String.to_atom/1)}
    end)
    |> Enum.into(%{})
  end

  # @doc """
  # Returns a list of all the schemas defined in the codebase.
  # """
  # def all, do: @modules

  def all do
    get_schemas()
  end
end
