defmodule Feeb.DB.Type.DateTimeUTC do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger

  @default_precision :microsecond

  def sqlite_type, do: :text

  @doc """
  Add default precision to the column `opts` if none is set.
  """
  def overwrite_opts(%{precision: _} = opts, _, _), do: opts
  def overwrite_opts(opts, _, _), do: Map.put(opts, :precision, @default_precision)

  def cast!(%DateTime{microsecond: {_, p}} = dt, %{precision: :second} = o, m) when p > 0,
    do: cast!(%{dt | microsecond: {0, 0}}, o, m)

  def cast!(%DateTime{microsecond: {_, 0}} = dt, %{precision: :millisecond} = o, m),
    do: cast!(%{dt | microsecond: {0, 3}}, o, m)

  def cast!(%DateTime{microsecond: {_, 6}} = dt, %{precision: :millisecond} = o, m),
    do: cast!(DateTime.truncate(dt, :millisecond), o, m)

  def cast!(%DateTime{microsecond: {_, p}} = dt, %{precision: :microsecond} = o, m) when p < 6,
    do: cast!(%{dt | microsecond: {0, 6}}, o, m)

  def cast!(%DateTime{} = dt, _, _), do: dt

  def cast!(datetime_str, o, m) when is_binary(datetime_str) do
    case DateTime.from_iso8601(datetime_str) do
      {:ok, dt, _} -> cast!(dt, o, m)
      error -> raise "Invalid DateTime string: #{datetime_str} - #{inspect(error)}"
    end
  end

  def cast!(nil, %{nullable: true}, _),
    do: nil

  def dump!(nil, %{nullable: true}, _), do: nil
  def dump!(%DateTime{} = dt, _, _), do: DateTime.to_string(dt)

  def load!(v, _, _) when is_binary(v), do: DateTime.from_iso8601(v) |> elem(1)
  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end
end
