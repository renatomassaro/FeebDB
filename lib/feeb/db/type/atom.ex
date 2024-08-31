defmodule Feeb.DB.Type.Atom do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text
  def cast!(nil, %{nullable: true}, _), do: nil
  def cast!(v, _, _) when is_atom(v) and not (is_nil(v) or is_boolean(v)), do: v
  def cast!(v, _, _) when is_binary(v), do: String.to_atom(v)

  def dump!(v, _, _) when is_atom(v), do: "#{v}"

  def load!(v, _, _) when is_binary(v), do: String.to_atom(v)
  def load!(nil, %{nullable: true}, _), do: nil
end
