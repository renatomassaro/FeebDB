defmodule Feeb.DB.Type.Boolean do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :integer
  def cast!(v, _) when is_boolean(v), do: v
  def dump!(true, _), do: 1
  def dump!(false, _), do: 0
  def load!(v, _) when is_boolean(v), do: v
  def load!(1, _), do: true
  def load!(0, _), do: false
end
