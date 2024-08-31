defmodule Feeb.DB.Type.Map do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text

  def cast!(v, o, m) when is_map(v), do: v |> dump!(o, m) |> load!(o, m)
  def cast!(nil, %{nullable: true}, _), do: nil

  # TODO: Use native decoder now :)
  # def dump!(v, _) when is_map(v), do: Jason.encode!(v)
  def dump!(nil, _, _), do: nil

  # TODO: Use native decoder now :)
  # def load!(v, %{keys: :string}) when is_binary(v), do: Jason.decode!(v)
  # def load!(v, %{keys: :atom}) when is_binary(v), do: Jason.decode!(v, keys: :atoms)
  # def load!(v, _) when is_binary(v), do: Jason.decode!(v, keys: :atoms)
  def load!(nil, %{nullable: true}, _), do: nil
end
