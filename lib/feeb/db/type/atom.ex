defmodule Feeb.DB.Type.Atom do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger

  def sqlite_type, do: :text

  @doc """
  NOTE: `true`, `false` and `nil` _are_ atoms, however we don't want to accept them to avoid
  silent errors. If the user wants to use true/false, go for a `:boolean` type. `nil` is okay
  iff it has the `nullable` flag.
  """
  def cast!(nil, %{nullable: true}, _), do: nil
  def cast!(v, _, _) when is_atom(v) and not (is_nil(v) or is_boolean(v)), do: v
  def cast!(v, _, _) when is_binary(v), do: String.to_atom(v)

  def dump!(v, _, _) when is_atom(v), do: "#{v}"
  def dump!(nil, %{nullable: true}, _), do: nil

  def load!("", %{nullable: true}, _), do: nil
  def load!(v, _, _) when is_binary(v), do: String.to_atom(v)
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
