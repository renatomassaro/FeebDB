defmodule Feeb.DB.Type.Integer do
  def sqlite_type, do: :integer
  def cast!(v, _) when is_integer(v), do: v
  def dump!(v, _) when is_integer(v), do: v
  def load!(v, _) when is_integer(v), do: v
end
