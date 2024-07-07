defmodule Feeb.DB.Query.Dynamic do
  alias __MODULE__.Builder

  def build(schema, assoc_map, search_input) do
    %{filter: filter_map, page: page_map, sort: sort_map, opts: opts} = search_input

    main_table = schema.__table__
    main_alias = Map.fetch!(assoc_map, main_table)

    {wheres, where_assocs, _, bindings} = Builder.build_wheres(assoc_map, filter_map)

    select = Builder.build_select(main_alias, opts)
    joins = Builder.build_joins(main_table, main_alias, assoc_map, where_assocs)
    sorts = Builder.build_sorts(assoc_map, sort_map)
    limits = Builder.build_limits(page_map)

    {Builder.to_string(select, wheres, joins, sorts, limits), bindings}
  end
end
