defmodule Feeb.DB.Type.Integer do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :integer
  def cast!(v, _, _) when is_integer(v), do: v
  def dump!(v, _, _) when is_integer(v), do: v
  def load!(v, _, _) when is_integer(v), do: v
end
