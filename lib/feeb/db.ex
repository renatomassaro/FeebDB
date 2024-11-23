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
  def begin(context, shard_id, access_type, transaction_type \\ :exclusive) do
    setup_env(context, shard_id, access_type)
    set_context(context, shard_id)

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
    current_ctx = LocalState.get_current_context!()
    result = callback.()
    LocalState.set_current_context(current_ctx.context, current_ctx.shard_id)
    result
  end

  @doc """
  Same as `with_context/1`, but explicitly sets the "temporary" context. You will want to use this
  function when resuming to a transaction that has already begun.
  """
  def with_context(context, shard_id, callback) when is_function(callback) do
    current_ctx = LocalState.get_current_context!()
    LocalState.set_current_context(context, shard_id)
    result = callback.()
    LocalState.set_current_context(current_ctx.context, current_ctx.shard_id)
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

  # TODO: See Feeb.DB, I also support `custom_fields`
  def one({domain, :fetch}, bindings, opts) when is_list(bindings) do
    {get_context!(), domain, :__fetch}
    |> Query.get_templated_query_id([], %{})
    |> one(bindings, opts)
  end

  def one({domain, :fetch}, value, opts), do: one({domain, :fetch}, [value], opts)

  def one({domain, query_name}, bindings, opts) when is_list(bindings) do
    one({get_context!(), domain, query_name}, bindings, opts)
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
    {get_context!(), schema.__table__(), :__all}
    |> Query.get_templated_query_id(:all, %{})
    |> all([], opts)
  end

  def all({domain, query_name}, bindings, opts) do
    all({get_context!(), domain, query_name}, bindings, opts)
  end

  def all({_, domain, query_name}, bindings, opts) do
    bindings = if is_list(bindings), do: bindings, else: [bindings]

    case GenServer.call(get_pid!(), {:query, :all, {domain, query_name}, bindings, opts}) do
      {:ok, rows} -> rows
      {:error, reason} -> raise reason
    end
  end

  def insert(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__insert}
    |> Query.get_templated_query_id(:all, %{schema: schema})
    |> insert(struct, [])
  end

  def insert(partial_or_full_query_id, struct, opts \\ [])

  def insert({domain, query_name}, %_{} = struct, opts) do
    insert({get_context!(), domain, query_name}, struct, opts)
  end

  def insert({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :application == struct.__meta__.origin

    if struct.__meta__.valid? do
      bindings = get_bindings(full_query_id, struct)
      GenServer.call(get_pid!(), {:query, :insert, {domain, query_name}, bindings, opts})
    else
      {:error, "Cast error: #{inspect(struct.__meta__.errors)}"}
    end
  end

  def insert!(struct) do
    {:ok, r} = insert(struct)
    r
  end

  def insert!(query, struct, opts \\ []) do
    {:ok, r} = insert(query, struct, opts)
    r
  end

  # TODO Test
  def update(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__update}
    |> Query.get_templated_query_id(struct.__meta__.target, %{})
    |> update(struct, [])
  end

  def update(partial_or_full_query_id, struct, opts \\ [])

  def update({domain, query_name}, %_{} = struct, opts) do
    update({get_context!(), domain, query_name}, struct, opts)
  end

  def update({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :update, {domain, query_name}, bindings, opts})
  end

  def update!(struct) do
    {:ok, r} = update(struct)
    r
  end

  def update!(query_id, struct, opts \\ []) do
    {:ok, r} = update(query_id, struct, opts)
    r
  end

  # TODO: Test
  def delete(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__delete}
    |> Query.get_templated_query_id([], %{})
    |> update(struct, [])
  end

  def delete(partial_or_full_query_id, struct, opts \\ [])

  def delete({domain, query_name}, %_{} = struct, opts) do
    delete({get_context!(), domain, query_name}, struct, opts)
  end

  def delete({_, domain, query_name} = full_query_id, %_{} = struct, opts) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :delete, {domain, query_name}, bindings, opts})
  end

  def delete!(struct) do
    {:ok, r} = delete(struct)
    r
  end

  def delete!(query_id, struct, opts \\ []) do
    {:ok, r} = delete(query_id, struct, opts)
    r
  end

  ##################################################################################################
  # Private
  ##################################################################################################

  defp setup_env(context, shard_id, type) when type in [:write, :read] do
    {:ok, manager_pid} = Repo.Manager.Registry.fetch_or_create(context, shard_id)

    # TODO: Handle busy
    {:ok, repo_pid} = Repo.Manager.fetch_connection(manager_pid, type)

    LocalState.add_entry(context, shard_id, {manager_pid, repo_pid, type})
  end

  defp delete_env do
    state = LocalState.get_current_context!()
    :ok = Repo.Manager.release_connection(state.manager_pid, state.repo_pid)
    LocalState.remove_entry(state.context, state.shard_id)
    LocalState.unset_current_context()
  end

  defp get_pid! do
    LocalState.get_current_context!().repo_pid
  end

  defp get_context! do
    LocalState.get_current_context!().context
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
