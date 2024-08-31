defmodule Feeb.DB.Type.Boolean do
  require Logger

  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :integer

  def cast!(v, _) when is_boolean(v), do: v
  def cast!(nil, %{nullable: true}), do: nil

  def dump!(true, _), do: 1
  def dump!(false, _), do: 0
  def dump!(nil, %{nullable: true}), do: nil

  def load!(1, _), do: true
  def load!(0, _), do: false
  def load!(nil, %{nullable: true}), do: nil

  def load!(nil, _) do
    Logger.warn("Loaded `nil` value from non-nullable field")
    nil
  end
end
