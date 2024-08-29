defmodule Feeb.DB.Type.DateTimeUTC do
  @behaviour Feeb.DB.Type.Behaviour

  def sqlite_type, do: :text

  def cast!(nil, %{nullable: true}),
    do: nil

  def cast!(%DateTime{} = dt, %{precision: precision}),
    do: DateTime.truncate(dt, precision)

  def cast!(%DateTime{} = dt, _), do: dt

  def dump!(nil, %{nullable: true}), do: nil
  def dump!(%DateTime{} = dt, _), do: DateTime.to_string(dt)

  def load!(nil, %{nullable: true}), do: nil
  def load!(v, _) when is_binary(v), do: DateTime.from_iso8601(v) |> elem(1)
end
