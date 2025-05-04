defmodule Feeb.DB do
  @moduledoc """
  NOTE: When calling Feeb.DB functions, we assume a previous setup step has been
  made. More specifically, we expect the process to hold a `:repo_pid` variable.
  """

  require Logger
  alias Feeb.DB.{LocalState, Query, Repo, Schema}

  ##################################################################################################
  # Transaction management
  ##################################################################################################

  @doc """
  Starts a transaction.

  It will fetch (or create) the Repo.Manager for the corresponding {context, shard_id}, and it will
  also fetch the Repo connection (upon availability; blocking until a connection is made available).
  Once returned, the caller process can perform requests against the shard.

  If `access_type` is `:env`, skip the set up process described above and rely on Context (Process
  state) instead.
  """
  def begin(context, shard_id, access_type, opts \\ []) do
    transaction_type = opts[:type] || :exclusive

    setup_env(context, shard_id, access_type, opts)

    # We can't BEGIN EXCLUSIVE in a read-only database
    txn_type = if(access_type == :read, do: :deferred, else: transaction_type)
    :ok = GenServer.call(get_pid!(), {:begin, txn_type})
  end

  @doc """
  Specifies the context (database context and shard_id) that the process should use. Changing
  contexts will allow the caller process to interact with multiple databases at the same time.

  Raises an error if the given context has not been registered yet. A context is registered when
  `begin/4` is called.
  """
  def set_context(context, shard_id) do
    LocalState.set_current_context(context, shard_id)
  end

  @doc """
  Allow `callback` to switch context but resume previous context when `callback` finishes executing.
  """
  def with_context(callback) when is_function(callback) do
    current_ctx = LocalState.get_current_context()
    result = callback.()
    current_ctx && LocalState.set_current_context(current_ctx.context, current_ctx.shard_id)
    result
  end

  @doc """
  Commits a transaction.

  It will also release the lock in the Repo connection, allowing other processes to grab it.
  """
  def commit do
    :ok = GenServer.call(get_pid!(), {:commit})
    # TODO: Close via repomanager
    # :ok = GenServer.call(get_pid!(), {:close})
    delete_env()
    :ok
  end

  @doc """
  Rolls back a transaction.

  It will also release the lock in the Repo connection, allowing other processes to grab it.
  """
  def rollback do
    :ok = GenServer.call(get_pid!(), {:rollback})
    delete_env()
    :ok
  end

  ##################################################################################################
  # Queries
  ##################################################################################################

  def raw(sql, bindings \\ []) do
    GenServer.call(get_pid!(), {:raw, sql, bindings})
  end

  def raw!(sql, bindings \\ []) do
    {:ok, r} = raw(sql, bindings)
    r
  end

  def prepared_raw(sql, bindings, schema) do
    opts = [schema: schema]
    GenServer.call(get_pid!(), {:prepared_raw, sql, bindings, opts})
  end

  def one(partial_or_full_query_id, bindings \\ [], opts \\ [])

  def one({domain, :fetch}, bindings, opts) when is_list(bindings) do
    target_fields = opts[:select] || [:*]

    {get_context!(), domain, :__fetch}
    |> Query.get_templated_query_id(target_fields, %{})
    |> one(bindings, opts)
  end

  def one({domain, :fetch}, value, opts), do: one({domain, :fetch}, [value], opts)

  def one({domain, query_name}, bindings, opts) when is_list(bindings) do
    {get_context!(), domain, query_name}
    |> get_query_id_for_select_query(opts)
    |> one(bindings, opts)
  end

  def one({domain, query_name}, value, opts), do: one({domain, query_name}, [value], opts)

  def one({_, domain, query_name}, bindings, opts) when is_list(bindings) do
    case GenServer.call(get_pid!(), {:query, :one, {domain, query_name}, bindings, opts}) do
      {:ok, r} -> r
      {:error, :multiple_results} -> raise "MultipleResultsError"
    end
  end

  def one!(query_id, bindings \\ [], opts \\ []) do
    r = one(query_id, bindings, opts)
    true = not is_nil(r)
    r
  end

  def all(partial_or_full_query_id, bindings \\ [], opts \\ [])

  def all(schema, _bindings, opts) when is_atom(schema) do
    target_fields = opts[:select] || [:*]

    {get_context!(), schema.__table__(), :__all}
    |> Query.get_templated_query_id(target_fields, %{})
    |> all([], opts)
  end

  def all({domain, query_name}, bindings, opts) when is_list(bindings) do
    {get_context!(), domain, query_name}
    |> get_query_id_for_select_query(opts)
    |> all(bindings, opts)
  end

  def all({domain, query_name}, value, opts), do: all({domain, query_name}, [value], opts)

  def all({_, domain, query_name}, bindings, opts) do
    case GenServer.call(get_pid!(), {:query, :all, {domain, query_name}, bindings, opts}) do
      {:ok, rows} -> rows
      {:error, reason} -> raise reason
    end
  end

  def insert(%schema{} = struct, opts \\ []) do
    {get_context!(), schema.__table__(), :__insert}
    |> Query.get_templated_query_id([:*], %{schema: schema})
    |> insert_sql(struct, opts)
  end

  def insert!(struct) do
    {:ok, r} = insert(struct)
    r
  end

  defp insert_sql({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :application == struct.__meta__.origin

    if struct.__meta__.valid? do
      bindings = get_bindings(full_query_id, struct)
      GenServer.call(get_pid!(), {:query, :insert, {domain, query_name}, bindings, opts})
    else
      {:error, "Cast error: #{inspect(struct.__meta__.errors)}"}
    end
  end

  def update(%schema{} = struct, opts \\ []) do
    {get_context!(), schema.__table__(), :__update}
    |> Query.get_templated_query_id(struct.__meta__.target, %{})
    |> update_sql(struct, Keyword.merge(opts, returning: true))
  end

  def update!(struct) do
    {:ok, r} = update(struct)
    r
  end

  defp update_sql({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :update, {domain, query_name}, bindings, opts})
  end

  def update_all(partial_or_full_query_id, bindings, opts \\ [])

  def update_all({domain, query_name}, bindings, opts) do
    update_all({get_context!(), domain, query_name}, bindings, opts)
  end

  def update_all({_, domain, query_name}, bindings, opts) do
    GenServer.call(get_pid!(), {:query, :update_all, {domain, query_name}, bindings, opts})
  end

  def update_all!(query_id, params, opts \\ []) do
    {:ok, r} = update_all(query_id, params, opts)
    r
  end

  def delete(%schema{} = struct, opts \\ []) do
    {get_context!(), schema.__table__(), :__delete}
    |> Query.get_templated_query_id([], %{})
    |> delete_sql(struct, Keyword.merge([returning: true], opts))
  end

  def delete!(struct) do
    {:ok, r} = delete(struct)
    r
  end

  defp delete_sql({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :delete, {domain, query_name}, bindings, opts})
  end

  def delete_all(partial_or_full_query_id, bindings, opts \\ [])

  def delete_all({domain, query_name}, bindings, opts) do
    delete_all({get_context!(), domain, query_name}, bindings, opts)
  end

  def delete_all({_, domain, query_name}, bindings, opts) do
    GenServer.call(get_pid!(), {:query, :delete_all, {domain, query_name}, bindings, opts})
  end

  def delete_all!(query_id, params, opts \\ []) do
    {:ok, r} = delete_all(query_id, params, opts)
    r
  end

  def reload(%schema{} = struct) do
    bindings =
      Enum.map(schema.__primary_keys__() || [], fn col_name ->
        Map.fetch!(struct, col_name)
      end)

    one({schema.__table__(), :fetch}, bindings)
  end

  def reload(schemas) when is_list(schemas),
    do: Enum.map(schemas, &reload/1)

  def reload!(%schema{} = struct) do
    case reload(struct) do
      %^schema{} = result ->
        result

      nil ->
        # TODO: Only log the PK of the struct
        raise "Unable to reload; entry not found: #{inspect(struct)}"
    end
  end

  def reload!(schemas) when is_list(schemas),
    do: Enum.map(schemas, &reload!/1)

  ##################################################################################################
  # Private
  ##################################################################################################

  defp setup_env(context, shard_id, type, opts) when type in [:write, :read] do
    {:ok, manager_pid} = Repo.Manager.Registry.fetch_or_create(context, shard_id)
    {:ok, repo_pid} = Repo.Manager.fetch_connection(manager_pid, type, opts)

    LocalState.add_context(context, shard_id, {manager_pid, repo_pid, type})
  end

  defp delete_env do
    state = LocalState.get_current_context!()
    :ok = Repo.Manager.release_connection(state.manager_pid, state.repo_pid)

    LocalState.remove_current_context()
  end

  defp get_pid! do
    LocalState.get_current_context!().repo_pid
  end

  defp get_context! do
    LocalState.get_current_context!().context
  end

  defp get_query_id_for_select_query(original_query_id, []), do: original_query_id

  defp get_query_id_for_select_query(original_query_id, opts) do
    target_fields = opts[:select] || [:*]

    if target_fields == [:*] do
      original_query_id
    else
      Query.compile_adhoc_query(original_query_id, target_fields)
    end
  end

  defp get_bindings(query_id, struct) do
    {_, {_, params_bindings}, _} = Query.fetch!(query_id)

    # Ensure we are handling all and only the casted fields
    # validate_bindings!(query_id, struct, params_bindings)

    Enum.map(params_bindings, fn field_name ->
      Schema.dump(struct, field_name)
    end)
  end

  # defp validate_bindings!(query_id, struct, bindings) do
  #   schema = struct.__struct__.__schema__()

  #   on_failure = fn ->
  #     expected =
  #       if struct.__meta__.target == :all do
  #         Map.keys(schema)
  #       else
  #         struct.__meta__.target
  #       end

  #     diff1 = MapSet.difference(MapSet.new(expected), MapSet.new(bindings))
  #     diff2 = MapSet.difference(MapSet.new(bindings), MapSet.new(expected))

  #     "Invalid bindings for #{inspect query_id}. Expected: #{inspect expected} " <> \
  #       "but got: #{inspect bindings}.\nDiff: #{inspect diff1} #{inspect diff2}" <> \
  #       "\nMaybe you need to update the SQL query?"
  #     |> raise()
  #   end

  #   case struct.__meta__.target do
  #     :all ->
  #       if map_size(schema) != length(bindings),
  #         do: on_failure.()

  #     tgt_fields when is_list(tgt_fields) ->
  #       if Enum.sort(tgt_fields) != Enum.sort(bindings),
  #         do: on_failure.()
  #   end
  # end
end
