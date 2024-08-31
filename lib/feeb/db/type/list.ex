defmodule Feeb.DB.Type.List do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text

  def cast!(v, o, m) when is_list(v), do: v |> dump!(o, m) |> load!(o, m)
  def cast!(nil, %{default: v}, _), do: v
  def cast!(nil, %{nullable: true}, _), do: nil

  # TODO: Use native decoder now :)
  # def dump!(v, _) when is_list(v), do: Jason.encode!(v)
  # def dump!(nil, %{default: v}) when is_list(v), do: Jason.encode!(v)
  def dump!(nil, %{default: nil}, _), do: nil
  def dump!(nil, %{nullable: true}, _), do: nil

  # TODO: Use native decoder now :)
  # def load!(v, _) when is_binary(v), do: Jason.decode!(v)
  def load!(nil, %{default: v}, _), do: v
  def load!(nil, %{nullable: true}, _), do: nil
end
