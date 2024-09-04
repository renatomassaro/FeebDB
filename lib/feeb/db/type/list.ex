defmodule Feeb.DB.Type.List do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger
  alias Utils.JSON

  def sqlite_type, do: :text

  def cast!(v, _, _) when is_list(v), do: v
  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(v, _, _) when is_list(v), do: JSON.encode!(v)
  def dump!(nil, %{nullable: true}, _), do: nil

  def load!(v, _, _) when is_binary(v), do: JSON.decode!(v)
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
