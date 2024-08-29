defmodule Feeb.DB.Type.String do
  def sqlite_type, do: :text
  def cast!(v, _) when is_binary(v), do: v
  def dump!(v, _) when is_binary(v), do: v
  def load!(v, _) when is_binary(v), do: v
  # TODO: Support nullable opt and warn if casting `nil` on non-nullable field
  # def load!(:undefined, _), do: nil
end
