defmodule Feeb.DB.Type.UUID do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger

  def sqlite_type, do: :text

  def cast!(v, _, _) when is_binary(v) do
    if not Utils.UUID.is_valid?(v), do: raise("Invalid UUID value: #{v}")
    v
  end

  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(v, _, _) when is_binary(v), do: v
  def dump!(nil, %{nullable: true}, _), do: nil

  def load!(v, _, _) when is_binary(v), do: v
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
