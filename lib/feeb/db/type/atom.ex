defmodule Feeb.DB.Type.Atom do
  def sqlite_type, do: :text
  def cast!(nil, %{nullable: true}), do: nil
  def cast!(v, _) when is_atom(v) and not (is_nil(v) or is_boolean(v)), do: v
  def cast!(v, _) when is_binary(v), do: String.to_atom(v)

  def dump!(v, _) when is_atom(v), do: "#{v}"

  def load!(v, _) when is_binary(v), do: String.to_atom(v)
  def load!(nil, %{nullable: true}), do: nil
end
