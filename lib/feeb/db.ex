defmodule Feeb.DB do
  @moduledoc """
  NOTE: When calling Feeb.DB functions, we assume a previous setup step has been
  made. More specifically, we expect the process to hold a `:repo_pid` variable.
  """

  require Logger
  alias Feeb.DB.{Query, Repo, Schema}

  def begin(context, shard_id, access_type, transaction_type \\ :exclusive) do
    # We can't BEGIN EXCLUSIVE in a read-only database
    txn_type = if(access_type == :read, do: :deferred, else: transaction_type)
    setup_env(context, shard_id, access_type)
    :ok = GenServer.call(get_pid!(), {:begin, txn_type})
  end

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

  def one(partial_or_full_query_id, bindings \\ [])

  # TODO: See Feeb.DB, I also support `custom_fields`
  def one({domain, :fetch}, bindings) when is_list(bindings) do
    {get_context!(), domain, :__fetch}
    |> Query.get_templated_query_id([], %{})
    |> one(bindings)
  end

  def one({domain, :fetch}, value), do: one({domain, :fetch}, [value])

  def one({domain, query_name}, bindings) when is_list(bindings) do
    one({get_context!(), domain, query_name}, bindings)
  end

  def one({domain, query_name}, value), do: one({domain, query_name}, [value])

  def one({_, domain, query_name}, bindings) when is_list(bindings) do
    case GenServer.call(get_pid!(), {:query, :one, {domain, query_name}, bindings}) do
      {:ok, r} -> r
      {:error, :multiple_results} -> raise "MultipleResultsError"
    end
  end

  def one!(query_id, bindings \\ []) do
    r = one(query_id, bindings)
    true = not is_nil(r)
    r
  end

  def all(partial_or_full_query_id, bindings \\ [])

  def all(schema, _bindings) when is_atom(schema) do
    {get_context!(), schema.__table__(), :__all}
    |> Query.get_templated_query_id(:all, %{})
    |> all([])
  end

  def all({domain, query_name}, bindings) do
    all({get_context!(), domain, query_name}, bindings)
  end

  def all({_, domain, query_name}, bindings) do
    case GenServer.call(get_pid!(), {:query, :all, {domain, query_name}, bindings}) do
      {:ok, rows} -> rows
      {:error, reason} -> raise reason
    end
  end

  def insert(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__insert}
    |> Query.get_templated_query_id(:all, %{schema: schema})
    |> insert(struct)
  end

  def insert({domain, query_name}, %_{} = struct) do
    insert({get_context!(), domain, query_name}, struct)
  end

  def insert({_, domain, query_name} = full_query_id, %_{} = struct) do
    # TODO: Make it more friendly
    true = :application == struct.__meta__.origin

    if struct.__meta__.valid? do
      bindings = get_bindings(full_query_id, struct)
      GenServer.call(get_pid!(), {:query, :insert, {domain, query_name}, bindings})
    else
      {:error, "Cast error: #{inspect(struct.__meta__.errors)}"}
    end
  end

  def insert!(struct) do
    {:ok, r} = insert(struct)
    r
  end

  def insert!(query, struct) do
    {:ok, r} = insert(query, struct)
    r
  end

  # TODO Test
  def update(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__update}
    |> Query.get_templated_query_id(struct.__meta__.target, %{})
    |> update(struct)
  end

  def update({domain, query_name}, %_{} = struct) do
    update({get_context!(), domain, query_name}, struct)
  end

  def update({_, domain, query_name} = full_query_id, %_{} = struct) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :update, {domain, query_name}, bindings})
  end

  def update!(struct) do
    {:ok, r} = update(struct)
    r
  end

  def update!(query_id, struct) do
    {:ok, r} = update(query_id, struct)
    r
  end

  # TODO: Test
  def delete(%schema{} = struct) do
    {get_context!(), schema.__table__(), :__delete}
    |> Query.get_templated_query_id([], %{})
    |> update(struct)
  end

  def delete({domain, query_name}, %_{} = struct) do
    delete({get_context!(), domain, query_name}, struct)
  end

  def delete({_, domain, query_name} = full_query_id, %_{} = struct) do
    # TODO: Make it more friendly
    true = :db == struct.__meta__.origin

    bindings = get_bindings(full_query_id, struct)
    GenServer.call(get_pid!(), {:query, :delete, {domain, query_name}, bindings})
  end

  def delete!(struct) do
    {:ok, r} = delete(struct)
    r
  end

  def delete!(query_id, struct) do
    {:ok, r} = delete(query_id, struct)
    r
  end

  def rollback do
    :ok = GenServer.call(get_pid!(), {:rollback})
    delete_env()
    :ok
  end

  def commit do
    :ok = GenServer.call(get_pid!(), {:commit})
    # TODO: Close via repomanager
    # :ok = GenServer.call(get_pid!(), {:close})
    delete_env()
    :ok
  end

  def assert_env! do
    get_pid!()
    get_manager_pid!()
    get_context!()
  end

  # def has_env?, do: Process.get(:repo_pid, false) && true

  defp setup_env(context, shard_id, type) when type in [:write, :read] do
    {:ok, manager_pid} = Repo.Manager.Registry.fetch_or_create(context, shard_id)

    # TODO: Handle busy
    {:ok, repo_pid} = Repo.Manager.fetch_connection(manager_pid, type)

    Process.put(:db_context, context)
    Process.put(:manager_pid, manager_pid)
    Process.put(:repo_pid, repo_pid)
  end

  defp delete_env do
    repo_pid = get_pid!()
    manager_pid = get_manager_pid!()
    :ok = Repo.Manager.release_connection(manager_pid, repo_pid)
    Process.delete(:repo_pid)
    Process.delete(:manager_pid)
    Process.delete(:db_context)
  end

  defp get_pid! do
    case Process.get(:repo_pid) do
      pid when is_pid(pid) ->
        pid

      nil ->
        Logger.error("DB env not set up for #{inspect(self())}")
        raise "DB env not set up for #{inspect(self())}"
    end
  end

  defp get_manager_pid! do
    pid = Process.get(:manager_pid)
    true = is_pid(pid)
    pid
  end

  defp get_context! do
    context = Process.get(:db_context)

    if is_nil(context),
      do: raise("Context for Feeb.DB not set")

    context
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
