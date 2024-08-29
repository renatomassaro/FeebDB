defmodule Feeb.DB.Type.List do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text

  def cast!(v, o) when is_list(v), do: v |> dump!(o) |> load!(o)
  def cast!(nil, %{default: v}), do: v
  def cast!(nil, %{nullable: true}), do: nil

  # TODO: Use native decoder now :)
  # def dump!(v, _) when is_list(v), do: Jason.encode!(v)
  # def dump!(nil, %{default: v}) when is_list(v), do: Jason.encode!(v)
  def dump!(nil, %{default: nil}), do: nil
  def dump!(nil, %{nullable: true}), do: nil

  # TODO: Use native decoder now :)
  # def load!(v, _) when is_binary(v), do: Jason.decode!(v)
  def load!(nil, %{default: v}), do: v
  def load!(nil, %{nullable: true}), do: nil
end
