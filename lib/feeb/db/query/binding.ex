defmodule Feeb.DB.Query.Binding do
  def parse_atstring(raw_bindings) do
    raw_bindings
    |> String.replace("]", "")
    |> String.replace(":", "")
    |> String.split(", ")
    |> Enum.map(&String.to_atom/1)
  end

  def parse_params(_qt, _sql, [_ | _] = bindings), do: bindings

  def parse_params(:select, sql, []), do: parse_kv(sql)
  def parse_params(:update, sql, []), do: parse_kv(sql)
  def parse_params(:delete, sql, []), do: parse_kv(sql)

  def parse_params(:insert, sql, []) do
    sql = String.downcase(sql)
    [_, rest] = String.split(sql, "(", parts: 2)
    [raw_fields | _] = String.split(rest, ") values (")
    parse_comma_separated(raw_fields)
  end

  def parse_fields(:select, sql, []) do
    [raw_fields | _] =
      sql
      |> String.downcase()
      |> String.slice(7..-1//1)
      |> String.split(" from ", parts: 2)

    if raw_fields == "*" do
      [:*]
    else
      parse_comma_separated(raw_fields)
    end
  end

  def parse_fields(qt, _, _) when qt in [:insert, :update, :delete],
    do: []

  def validate(_, sql, {_, bindings}),
    do: Utils.String.count(sql, "?") == length(bindings)

  defp parse_kv(sql) do
    sql
    |> String.split(" = ?")
    |> List.delete_at(-1)
    |> Enum.map(fn expr ->
      expr
      |> String.split(" ")
      |> Enum.at(-1)
      |> String.to_atom()
    end)
  end

  defp parse_comma_separated(raw_fields) do
    raw_fields
    |> String.split(",")
    |> Enum.map(fn s -> s |> String.trim() |> String.to_atom() end)
  end
end
