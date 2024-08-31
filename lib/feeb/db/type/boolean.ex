defmodule Feeb.DB.Type.Boolean do
  require Logger

  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :integer

  def cast!(v, _, _) when is_boolean(v), do: v
  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(true, _, _), do: 1
  def dump!(false, _, _), do: 0
  def dump!(nil, %{nullable: true}, _), do: nil

  def load!(1, _, _), do: true
  def load!(0, _, _), do: false
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
