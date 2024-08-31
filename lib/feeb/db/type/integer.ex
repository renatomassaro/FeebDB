defmodule Feeb.DB.Type.Integer do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger

  def sqlite_type, do: :integer

  def cast!(v, _, _) when is_integer(v), do: v
  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(v, _, _) when is_integer(v), do: v
  def dump!(nil, %{nullable: true}, _), do: nil

  def load!(v, _, _) when is_integer(v), do: v
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
