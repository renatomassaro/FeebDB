defmodule Feeb.DB.Type.UUID do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text

  def cast!(v, _, _) when is_binary(v) do
    if not Utils.UUID.is_valid?(v), do: raise("Invalid UUID value: #{v}")
    v
  end

  def cast!(nil, _, _), do: nil

  def dump!(v, _, _) when is_binary(v), do: v
  def dump!(nil, _, _), do: nil

  def load!(v, _, _) when is_binary(v), do: v
  def load!(nil, _, _), do: nil
end
