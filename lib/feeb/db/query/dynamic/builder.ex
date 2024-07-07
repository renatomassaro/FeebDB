defmodule Feeb.DB.Query.Dynamic.Builder do
  def build_select(main_alias, opts) do
    if Keyword.get(opts, :count) do
      "SELECT COUNT(*)"
    else
      "SELECT #{main_alias}.*"
    end
  end

  def build_wheres(assoc_map, filter_map) do
    {t, w, b} =
      filter_map
      |> Enum.map(fn {table, filters} ->
        table_alias = get_table_alias(assoc_map, table)

        {wheres, bindings} =
          Enum.reduce(filters, {[], []}, fn
            :noop, acc ->
              acc

            filter, {acc_w, acc_b} ->
              case build_where(table_alias, filter) do
                {w, b} ->
                  {[w | acc_w], [b | acc_b]}

                :noop ->
                  {acc_w, acc_b}
              end
          end)

        {table, {wheres, bindings}}
      end)
      |> Enum.reduce({[], [], []}, fn
        {_t, {[], []}}, acc ->
          acc

        {t, {ws, bs}}, {acc_t, acc_w, acc_b} ->
          {[t | acc_t], [ws | acc_w], [bs | acc_b]}
      end)

    # There's room for optimization here (but the entire build is so fast...)
    {w, b} = {
      w |> List.flatten() |> Enum.reverse(),
      b |> List.flatten() |> Enum.reverse()
    }

    w_count = Enum.count(w)

    w_str =
      w
      |> Enum.with_index()
      |> Enum.reduce("", fn {w, idx}, acc ->
        q =
          cond do
            idx == 0 and w_count == 1 -> "#{w}"
            idx == w_count - 1 -> "#{w}"
            true -> "#{w} AND"
          end

        "#{acc} #{q}"
      end)

    w_str =
      if w_str != "" do
        "WHERE#{w_str}"
      else
        ""
      end

    {w_str, t, w, b}
  end

  def build_joins(main_table, main_alias, assoc_map, where_assocs) do
    main_j = "FROM #{main_table} #{main_alias}"

    Enum.reduce(assoc_map, main_j, fn
      {assoc_table, {assoc_alias, main_fk, assoc_fk}}, acc ->
        if assoc_table in where_assocs do
          join = "JOIN #{assoc_table} #{assoc_alias}"
          on = "ON #{main_alias}.#{main_fk} = #{assoc_alias}.#{assoc_fk}"
          "#{acc} #{join} #{on}"
        else
          acc
        end

      {^main_table, _}, acc ->
        acc
    end)
  end

  def build_sorts(assoc_map, sort_map) do
    sorts =
      Enum.reduce(sort_map, [], fn {table, sorts}, acc ->
        table_alias = get_table_alias(assoc_map, table)
        total_sorts = Enum.count(sorts)

        table_sorts =
          sorts
          |> Enum.with_index()
          |> Enum.reduce("", fn {{field, direction}, idx}, iacc ->
            true = direction in [:asc, :desc]
            direction = String.upcase("#{direction}")

            sep =
              cond do
                idx == 0 and total_sorts == 1 -> ""
                idx == total_sorts - 1 -> ""
                true -> ", "
              end

            "#{iacc}#{table_alias}.#{field} #{direction}#{sep}"
          end)

        [table_sorts | acc]
      end)
      |> Enum.reverse()
      |> Enum.join(", ")

    if sorts != "" do
      "ORDER BY #{sorts}"
    else
      ""
    end
  end

  def build_limits(%{limit: limit, offset: offset}) do
    "LIMIT #{limit} OFFSET #{offset}"
  end

  def to_string(select, wheres, joins, sorts, limits) do
    "#{select} #{joins} #{wheres} #{sorts} #{limits}"
  end

  defp build_where(a, {c, {:eq, v}}) when is_binary(v) or is_integer(v),
    do: {"#{a}.#{c} = ?", v}

  defp build_where(a, {c, {:gte, v}}) when is_integer(v),
    do: {"#{a}.#{c} >= ?", v}

  defp build_where(a, {c, {:gt, v}}) when is_integer(v),
    do: {"#{a}.#{c} > ?", v}

  defp build_where(a, {c, {:lte, v}}) when is_integer(v),
    do: {"#{a}.#{c} <= ?", v}

  defp build_where(a, {c, {:lt, v}}) when is_integer(v),
    do: {"#{a}.#{c} < ?", v}

  # Alternatively, just raise for an empty list
  defp build_where(_, {_, {:in, []}}), do: :noop

  defp build_where(a, {c, {:in, values}}) when is_list(values) do
    total = Enum.count(values)

    values
    |> Enum.with_index()
    |> Enum.reduce({"#{a}.#{c} IN (", []}, fn {v, idx}, {acc_q, acc_v} ->
      q =
        cond do
          idx == 0 and total == 1 -> "?)"
          idx == total - 1 -> "?)"
          true -> "?, "
        end

      {"#{acc_q}#{q}", [v | acc_v]}
    end)
  end

  defp build_where(a, {c, {:likep, v}}) when is_binary(v),
    do: {"#{a}.#{c} LIKE ? || '%'", v}

  defp build_where(a, {c, {:plikep, v}}) when is_binary(v),
    do: {"#{a}.#{c} LIKE '%' || ? || '%'", v}

  defp build_where(a, {c, {:fragment, fragment}}) when is_binary(fragment),
    do: {"#{a}.#{c} #{fragment}", []}

  defp get_table_alias(assoc_map, table) do
    case Map.fetch!(assoc_map, table) do
      {a, _, _} -> a
      a when is_binary(a) -> a
    end
  end
end
