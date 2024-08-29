defmodule Feeb.DB.Type.UUID do
  def sqlite_type, do: :text

  def cast!(v, _) when is_binary(v) do
    if not Utils.UUID.is_valid?(v), do: raise("Invalid UUID value: #{v}")
    v
  end

  def cast!(nil, _), do: nil

  def dump!(v, _) when is_binary(v), do: v
  def dump!(nil, _), do: nil

  def load!(v, _) when is_binary(v), do: v
  def load!(nil, _), do: nil
end
