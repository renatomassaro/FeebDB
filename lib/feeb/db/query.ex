defmodule Feeb.DB.Query do
  require Logger
  alias __MODULE__.Binding

  @env Mix.env()
  @initial_q {"", {[], []}, nil}

  def compile(path, {context, domain}) do
    Process.put({:db_sql, :path}, path)
    Process.put({:db_sql, :context}, context)
    Process.put({:db_sql, :domain}, domain)

    initial_acc = {%{}, nil, @initial_q}

    queries =
      path
      |> File.read!()
      |> String.split("\n")
      |> Enum.reduce(initial_acc, fn line, {qs, id, q} ->
        line = String.trim(line)
        handle_line(line, qs, id, q)
      end)
      |> elem(0)

    # Clean up
    Process.put({:db_sql, :path}, nil)
    Process.put({:db_sql, :context}, nil)
    Process.put({:db_sql, :domain}, nil)

    # Store these queries in a persistent table so they can be used by the application
    store_queries(queries, context, domain)
  end

  @doc """
  Compiling an adhoc query is useful when you want to select custom fields
  off of a "select *" query. It's like a subset of the original query
  """
  def compile_adhoc_query({context, domain, query_name} = query_id, custom_fields) do
    query_name = :"#{query_name}$#{Enum.join(custom_fields, "$")}"
    adhoc_query_id = {context, domain, query_name}

    {sql, {fields_bindings, params_bindings}, qt} = fetch!(query_id)

    if fields_bindings != [:*] do
      raise "#{inspect(query_id)}: Custom fields can only be used on 'SELECT *' queries"
    end

    # "Compile" new query
    new_sql = String.replace(sql, "*", Enum.join(custom_fields, ", "))
    adhoc_q = {new_sql, {custom_fields, params_bindings}, qt}

    append_runtime_query(adhoc_query_id, adhoc_q)
    adhoc_query_id
  end

  def get_templated_query_id({context, domain, :__insert}, target_fields, meta) do
    target_fields =
      if target_fields == :all do
        meta.schema.__cols__()
      else
        raise "Not supported for now, add & test it once needed"
        target_fields
      end
      |> Enum.sort()

    real_query_id = {context, domain, :__insert}

    case get(real_query_id) do
      {_, _, _} = _compiled_query ->
        real_query_id

      nil ->
        compile_templated_query(:__insert, real_query_id, target_fields)
    end
  end

  def get_templated_query_id({context, domain, :__update}, target_fields, _meta) do
    target_fields = Enum.sort(target_fields)

    query_name_suffix =
      target_fields
      |> Enum.reduce([], fn field, acc ->
        ["#{field}" | acc]
      end)
      |> Enum.reverse()
      |> Enum.join("$")

    real_query_id = {context, domain, :"__update$#{query_name_suffix}"}

    case get(real_query_id) do
      {_, _, _} = _compiled_query ->
        real_query_id

      nil ->
        compile_templated_query(:__update, real_query_id, target_fields)
    end
  end

  def get_templated_query_id({_context, _domain, query_name} = query_id, target_fields, _meta)
      when query_name in [:__all, :__fetch, :__delete] do
    case get(query_id) do
      {_, _, _} = _compiled_query ->
        query_id

      nil ->
        compile_templated_query(query_name, query_id, target_fields)
    end
  end

  defp compile_templated_query(:__insert, {_, domain, _} = query_id, target_fields) do
    columns_clause = target_fields |> Enum.join(", ")

    values_clause =
      target_fields
      |> Enum.map(fn _ -> "?" end)
      |> Enum.join(", ")

    sql = "INSERT INTO #{domain} ( #{columns_clause} ) VALUES ( #{values_clause} );"

    adhoc_query = {sql, {[], target_fields}, :insert}
    append_runtime_query(query_id, adhoc_query)

    query_id
  end

  defp compile_templated_query(:__update, {_, domain, _} = query_id, target_fields) do
    # Hard-coded for now, but we may need this to be dynamc in the future
    primary_key_col = :id

    set_clause =
      target_fields
      |> Enum.reduce([], fn field, acc ->
        ["#{field} = ?" | acc]
      end)
      |> Enum.reverse()
      |> Enum.join(", ")

    sql = "UPDATE #{domain} SET #{set_clause} WHERE id = ?;"
    adhoc_query = {sql, {[], target_fields ++ [primary_key_col]}, :update}
    append_runtime_query(query_id, adhoc_query)

    query_id
  end

  defp compile_templated_query(:__all, {_, domain, _} = query_id, _target_fields) do
    sql = "SELECT * FROM #{domain};"
    adhoc_query = {sql, {[:*], []}, :select}
    append_runtime_query(query_id, adhoc_query)
    query_id
  end

  defp compile_templated_query(:__fetch, {_, domain, _} = query_id, _target_fields) do
    sql = "SELECT * FROM #{domain} WHERE id = ?;"
    adhoc_query = {sql, {[:*], [:id]}, :select}
    append_runtime_query(query_id, adhoc_query)
    query_id
  end

  defp compile_templated_query(:__delete, {_, domain, _} = query_id, _target_fields) do
    sql = "DELETE FROM #{domain} WHERE id = ?"
    adhoc_query = {sql, {[], [:id]}, :delete}
    append_runtime_query(query_id, adhoc_query)
    query_id
  end

  defp append_runtime_query({context, domain, query_name}, {_, _, _} = adhoc_query) do
    if @env == :test,
      do: :persistent_term.put(:last_db_sql_query_appended_at, System.monotonic_time())

    # Replace domain queries with the new query we compiled in runtime
    adhoc_queries = :persistent_term.get({:db_sql_queries, {context, domain}}, %{})

    new_adhoc_queries = Map.put(adhoc_queries, query_name, adhoc_query)

    :persistent_term.put({:db_sql_queries, {context, domain}}, new_adhoc_queries)
  end

  # TODO
  def fetch!({:pragma, :user_version}), do: {"PRAGMA user_version", [], nil}
  def fetch!({:pragma, :set_user_version}), do: {"PRAGMA user_version = ?", [], nil}
  def fetch!({:begin, :deferred}), do: {"BEGIN DEFERRED", [], nil}
  def fetch!({:begin, :concurrent}), do: {"BEGIN CONCURRENT", [], nil}
  def fetch!({:begin, :exclusive}), do: {"BEGIN EXCLUSIVE", [], nil}
  def fetch!({_, :pragma, name}), do: fetch!({:pragma, name})

  def fetch!({context, domain, name}) do
    fetch_all!({context, domain})
    |> Map.fetch!(name)
  end

  def get({context, domain, name}) do
    fetch_all!({context, domain})
    |> Map.get(name)
  end

  def fetch_all!({context, domain}) do
    {:db_sql_queries, {context, domain}}
    |> :persistent_term.get()
  end

  @doc """
  This is dangerous and should be used only when I have 100% control over the query.
  """
  def inline_bind(raw_query, bindings) do
    if String.contains?(raw_query, "?") do
      raw_query
      |> String.split("?")
      |> Enum.zip(bindings)
      |> Enum.reduce("", fn {a, b}, acc ->
        a <> "#{b}" <> acc
      end)
    else
      raw_query
    end
  end

  # Line-break
  defp handle_line(<<>>, qs, id, q) when not is_nil(id) do
    {sql, {fields_bindings, params_bindings}, query_type} = q
    sql = sql |> String.trim() |> String.downcase()

    fields_bindings = Binding.parse_fields(query_type, sql, fields_bindings)
    params_bindings = Binding.parse_params(query_type, sql, params_bindings)
    bindings = {fields_bindings, params_bindings}

    queries = Map.put(qs, id, {sql, bindings, query_type})

    validate_sql!(id, query_type, sql, bindings)

    {queries, nil, @initial_q}
  end

  # Ignore consecutive line-breaks between each query
  defp handle_line(<<>>, qs, nil, q), do: {qs, nil, q}

  # Comments starting with "-- :", which define the query ID
  defp handle_line(<<45, 45, 32, 58, raw_id::binary>>, qs, nil, q),
    do: {qs, String.to_atom(raw_id), q}

  # Comments starting with "-- @" (aka "atstring")
  defp handle_line(<<45, 45, 32, 64, atstring::binary>>, qs, id, q),
    do: handle_atstring(atstring, qs, id, q)

  # Ignore other comments
  defp handle_line(<<45, 45, _::binary>>, qs, id, q), do: {qs, id, q}

  # When parsing the first line of the query, detect its type
  defp handle_line(<<115, 101, 108, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:select, line, qs, id, q)

  defp handle_line(<<83, 69, 76, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:select, line, qs, id, q)

  defp handle_line(<<105, 110, 115, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:insert, line, qs, id, q)

  defp handle_line(<<73, 78, 83, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:insert, line, qs, id, q)

  defp handle_line(<<117, 112, 100, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:update, line, qs, id, q)

  defp handle_line(<<85, 80, 68, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:update, line, qs, id, q)

  defp handle_line(<<100, 101, 108, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:delete, line, qs, id, q)

  defp handle_line(<<68, 69, 76, _::binary>> = line, qs, id, {_, _, nil} = q),
    do: add_query_type(:delete, line, qs, id, q)

  # Append line(s) from query once its type has been identified
  defp handle_line(line, qs, id, {sql, b, qt}) when not is_nil(id) and not is_nil(qt),
    do: {qs, id, {sql <> " " <> String.trim(line), b, qt}}

  defp add_query_type(qt, line, qs, id, {sql, bindings, nil}),
    do: handle_line(line, qs, id, {sql, bindings, qt})

  # @bind atstring: Matching on "bind ["
  defp handle_atstring(<<98, 105, 110, 100, 32, 91, raw_bindings::binary>>, qs, id, q) do
    {sql, {field_bindings, prev_params_bindings}, qt} = q

    # `-- @bind` comments accumulate, in case the user needs/wants to spread the bindings
    # over multiple lines
    params_bindings =
      (prev_params_bindings ++ [Binding.parse_atstring(raw_bindings)])
      |> List.flatten()

    {qs, id, {sql, {field_bindings, params_bindings}, qt}}
  end

  defp validate_sql!(id, qt, sql, bindings) do
    name = fn -> {Process.get({:db_sql, :domain}), id} end

    if String.at(sql, -1) != ";",
      do: raise("You forgot to end this SQL query with a semicolon: #{inspect(name.())}")

    if Utils.String.count(sql, ";") > 1,
      do: raise("You've got multiple semicolons at SQL query #{inspect(name.())}")

    if not Binding.validate(qt, sql, bindings),
      do: raise("Invalid bind count detected for query #{inspect(id)}: #{inspect(bindings)}")
  end

  defp store_queries(queries, context, domain) do
    pt_table_name = {:db_sql_queries, {context, domain}}

    if :persistent_term.get(pt_table_name, :not_found) != :not_found,
      do: Logger.warning("Recompiling queries for the \"#{domain}\" domain")

    :persistent_term.put(pt_table_name, queries)
  end
end
