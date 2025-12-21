defmodule Feeb.DB.Repo do
  @moduledoc false

  @struct_keys [:manager_pid, :context, :shard_id, :mode, :path, :conn, :transaction_id]
  @enforce_keys List.delete(@struct_keys, [:transaction_id])
  defstruct @struct_keys

  use GenServer
  require Logger
  alias Feeb.DB.{Config, Migrator, Query, Schema, SQLite}
  alias __MODULE__.RepoConfig

  @env Mix.env()
  @is_test_mode Application.compile_env(:feebdb, :is_test_mode, false)

  def get_path(context, shard_id),
    do: "#{Config.data_dir()}/#{context}/#{shard_id}.db"

  # Callbacks

  # Client API
  def start_link({_, _, _, _, _} = args),
    do: GenServer.start_link(__MODULE__, args)

  def begin(pid, txn_type) do
    with_telemetry(
      :begin,
      fn ->
        GenServer.call(pid, {:begin, txn_type, Logger.metadata()})
      end,
      %{type: txn_type}
    )
  end

  def commit(pid) do
    with_telemetry(:commit, fn -> GenServer.call(pid, {:commit, Logger.metadata()}) end)
  end

  def rollback(pid) do
    with_telemetry(:rollback, fn ->
      GenServer.call(pid, {:rollback, Logger.metadata()})
    end)
  end

  def one(pid, {domain, query_name}, bindings, opts),
    do: run_query(:one, pid, {domain, query_name}, bindings, opts)

  def all(pid, {domain, query_name}, bindings, opts),
    do: run_query(:all, pid, {domain, query_name}, bindings, opts)

  def insert(pid, {domain, query_name}, bindings, opts),
    do: run_query(:insert, pid, {domain, query_name}, bindings, opts)

  def update(pid, {domain, query_name}, bindings, opts),
    do: run_query(:update, pid, {domain, query_name}, bindings, opts)

  def update_all(pid, {domain, query_name}, bindings, opts),
    do: run_query(:update_all, pid, {domain, query_name}, bindings, opts)

  def delete(pid, {domain, query_name}, bindings, opts),
    do: run_query(:delete, pid, {domain, query_name}, bindings, opts)

  def delete_all(pid, {domain, query_name}, bindings, opts),
    do: run_query(:delete_all, pid, {domain, query_name}, bindings, opts)

  def raw(pid, sql, bindings) do
    with_telemetry(
      :query,
      fn -> GenServer.call(pid, {:raw, sql, bindings}) end,
      %{query_type: :raw}
    )
  end

  def close(pid),
    do: GenServer.call(pid, {:close})

  @doc """
  Used by the Repo.Manager to notify once the Repo has been released. Useful to resetting internal
  counters, transaction_id etc.
  """
  def notify_release(pid),
    do: GenServer.call(pid, {:mgt_connection_released})

  defp run_query(query_type, pid, {domain, query_name}, bindings, opts) do
    with_telemetry(
      :query,
      fn ->
        GenServer.call(
          pid,
          {:query, query_type, {domain, query_name}, bindings, opts, Logger.metadata()}
        )
      end,
      %{query_type: query_type, domain: domain, query_name: query_name}
    )
  end

  defp with_telemetry(name, cb, attrs \\ %{}) do
    start = System.monotonic_time()
    :telemetry.execute([:feebdb, name, :start], %{}, attrs)

    try do
      result = cb.()

      :telemetry.execute(
        [:feebdb, name, :stop],
        %{duration: System.monotonic_time() - start},
        attrs
      )

      result
    catch
      kind, reason ->
        :telemetry.execute(
          [:feebdb, name, :exception],
          %{duration: System.monotonic_time() - start},
          Map.merge(attrs, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  # Server API

  def init({context, shard_id, path, mode, manager_pid}) do
    Logger.metadata(context: context, shard_id: shard_id, path: path, mode: mode)
    Logger.info("Starting #{mode} repo for shard #{shard_id}@#{context}")
    true = mode in [:readwrite, :readonly]

    case SQLite.open(path) do
      {:ok, conn} ->
        state = %__MODULE__{
          manager_pid: manager_pid,
          context: context,
          shard_id: shard_id,
          mode: mode,
          path: path,
          conn: conn,
          transaction_id: nil
        }

        # `repo_config` is metadata that the Schema has access to when building virtual fields
        repo_config = RepoConfig.from_state(state)
        Process.put(:repo_config, repo_config)

        {:ok, state, {:continue, :bootstrap}}

      {:error, :database_open_failed} ->
        reason =
          case File.stat(path) do
            {:error, :enoent} ->
              "Database file #{path} does not exist"

            _ ->
              "Unknown reason"
          end

        raise "Unable to open database #{inspect({context, shard_id, path, mode})}: #{reason}"
    end
  end

  # Bootstrap
  def handle_continue(:bootstrap, state) do
    # TODO: This should be a hook that is implemented by the user of the library.

    # NOTE: There's room for optimization here by executing all pragmas in a
    # single query. I *think* that will speed up the bootstrap process, but
    # of course needs proper benchmarking. Also, consider running the pragmas
    # with synchronous=0, and then, once this is done, call synchronous=1
    # (Again, needs proper measurement to make sure it's a valid change)

    # We overwrite some defaults for testing. For prod, consider that synchronous=1
    conditional_pragma_based_on_env(state.conn, @env, @is_test_mode)

    # Note we always use WAL mode, even for tests. Memory journaling changes
    # the way that BEGIN EXCLUSIVE transactions affect read queries from
    # other transactions.
    :ok = SQLite.exec(state.conn, "PRAGMA journal_mode=wal")

    # In case of SQLITE_BUSY error, SQLite itself takes care of trying again after up to
    # `busy_timeout` milliseconds. Note that `esqlite3` also sets this value to 2s when
    # opening the connection, but I'll set it again for documentation purposes.
    # TODO: Consider possibility of setting this value to 0 (or a very small value) so
    # we move the handling of timeouts to the application layer. By doing so, we have
    # much better stats/logs representing how often transactions time out.
    :ok = SQLite.exec(state.conn, "PRAGMA busy_timeout=0")

    # Enforce FK constraints.
    :ok = SQLite.exec(state.conn, "PRAGMA foreign_keys=1")

    # Maximum number of pages to store in the cache.
    # TODO: Review this number
    :ok = SQLite.exec(state.conn, "PRAGMA cache_size=1200")

    if state.mode == :readonly do
      # Set the database itself to read-only. It's just an additional layer of protection, given we
      # already enforce read-only access at the application level.
      :ok = SQLite.exec(state.conn, "PRAGMA query_only=1")

      # Use memory-mapped IO on DB access. This setting is mostly useless for database changes (i.e.
      # INSERT/UPDATE/DELETE commands), which is why I'm enabling it only for read-only connections.
      # Additionally, there are some edge-cases / failure scenarios in which I'm not fully certain
      # the `esqlite` NIF would gracefully handle crashes. We'll have to find that out the hard way.
      # This may cause application crashes, but I don't think it can corrupt the database.
      # If this setting turns out to be safe, we can enable it on `readwrite` connections as well.
      # The hard-coded value is 256MiB.
      # :ok = SQLite.exec(state.conn, "PRAGMA mmap_size=268435456")
    end

    {:noreply, state, {:continue, :check_migrations}}
  end

  def handle_continue(:check_migrations, state) do
    # TODO: Wrap the migration in a transaction
    case Migrator.get_migration_status(state.conn, state.context, state.mode) do
      :migrated ->
        Logger.info("Shard #{state.shard_id} is already migrated")
        {:noreply, state}

      {:needs_migration, migrations} ->
        Logger.info("Needs migration on shard #{state.shard_id}")
        Migrator.migrate(state.conn, migrations)
        {:noreply, state}

      :needs_write_access ->
        Logger.info("Migrating shard #{state.shard_id} via write connection")
        Feeb.DB.begin(state.context, state.shard_id, :write)
        Feeb.DB.commit()
        {:noreply, state}
    end
  end

  def handle_call({:mgt_connection_released}, {caller_pid, _}, state) do
    # Make sure only the Repo.Manager can send mgt signals to the Repo.
    assert_release_signal_from_manager!(state.manager_pid, caller_pid)

    if not is_nil(state.transaction_id) do
      Logger.info("Connection released forcibly; rolling back transaction #{state.transaction_id}")
      :ok = SQLite.exec(state.conn, "ROLLBACK")
    end

    # Reset the GenServer state so we are ready to serve a new request
    {:reply, :ok, %{state | transaction_id: nil}}
  end

  def handle_call({:close}, _from, %{transaction_id: nil} = state) do
    Logger.info("Closing conn from repo #{inspect(self())}")
    :ok = SQLite.close(state.conn)
    {:stop, :normal, :ok, %{state | conn: nil}}
  end

  def handle_call({:close}, _from, %{transaction_id: _txn_id} = state) do
    Logger.error("Tried to close a Repo while in a transaction")
    {:reply, {:error, :cant_close_with_transaction}, state}
  end

  # BEGIN
  def handle_call({:begin, txn_type, log_meta}, _from, %{transaction_id: nil} = state) do
    start_custom_log_metadata_scope(log_meta)
    Logger.debug("BEGIN")

    {sql, _, _} = Query.fetch!({:begin, txn_type})

    case SQLite.exec(state.conn, sql) do
      :ok ->
        txn_id = gen_transaction_id()
        end_custom_log_metadata_scope()
        {:reply, :ok, %{state | transaction_id: txn_id}}

      {:error, r} ->
        Logger.error("Unable to BEGIN: #{inspect(r)}")
        end_custom_log_metadata_scope()
        {:reply, {:error, r}, state}
    end
  end

  def handle_call({:begin, _type, log_meta}, _from, state) do
    Logger.error("Tried to BEGIN when already in a transaction", log_meta)
    {:reply, {:error, :already_in_transaction}, state}
  end

  # COMMIT
  def handle_call({:commit, log_meta}, _from, %{transaction_id: nil} = state) do
    Logger.error("Tried to COMMIT when not in a transaction", log_meta)
    {:reply, {:error, :not_in_transaction}, state}
  end

  def handle_call({:commit, log_meta}, _from, state) do
    start_custom_log_metadata_scope(log_meta)

    sql = "COMMIT"
    Logger.debug("COMMIT")

    case SQLite.exec(state.conn, sql) do
      :ok ->
        end_custom_log_metadata_scope()
        {:reply, :ok, %{state | transaction_id: nil}}

      {:error, r} ->
        Logger.error("Error running #{sql}: #{inspect(r)}")
        end_custom_log_metadata_scope()
        {:reply, {:error, r}, state}
    end
  end

  # ROLLBACK
  def handle_call({:rollback, log_meta}, _from, %{transaction_id: nil} = state) do
    Logger.error("Tried to ROLBACK when not in a transaction", log_meta)
    {:reply, {:error, :not_in_transaction}, state}
  end

  def handle_call({:rollback, log_meta}, _from, state) do
    start_custom_log_metadata_scope(log_meta)
    sql = "ROLLBACK"

    case SQLite.exec(state.conn, sql) do
      :ok ->
        end_custom_log_metadata_scope()
        {:reply, :ok, %{state | transaction_id: nil}}

      {:error, r} ->
        Logger.error("Error running #{sql}: #{inspect(r)}")
        end_custom_log_metadata_scope()
        {:reply, {:error, r}, state}
    end
  end

  # Queries (SELECT/UPDATE/INSERT/DELETE)

  def handle_call(
        {:query, type, {domain, query_name}, bindings_values, opts, log_meta},
        _from,
        state
      ) do
    start_custom_log_metadata_scope(log_meta)
    query_id = {state.context, domain, query_name}
    {sql, _, _} = query = Query.fetch!(query_id, opts)

    bindings_values = normalize_bindings_values(bindings_values)

    Logger.debug("Query: #{inspect(sql)}. Bindings: #{inspect(bindings_values)}")

    with {:ok, {stmt, stmt_sql}} <- prepare_query(state, query_id, sql),
         true = stmt_sql == sql,
         :ok <- SQLite.bind(stmt, bindings_values),
         {:ok, rows} <- SQLite.all(state.conn, stmt) do
      attrs = %{
        format: opts[:format] || :schema,
        returning: opts[:returning] || false
      }

      result = format_result(type, query_id, query, rows, bindings_values, attrs)
      end_custom_log_metadata_scope()
      {:reply, result, state}
    else
      {:error, _} = err ->
        Logger.error("Query error: #{inspect(err)}")
        # TODO: Rollback?
        end_custom_log_metadata_scope()
        {:reply, err, state}
    end
  end

  def handle_call({:raw, raw_sql, bindings}, _from, state) do
    check_implicit_transaction(state.transaction_id)
    sql = Query.inline_bind(raw_sql, bindings)

    case SQLite.raw(state.conn, sql) do
      {:ok, result} -> {:reply, {:ok, result}, state}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  defp format_result(:one, _, _, [row], _, %{format: :raw}), do: {:ok, row}

  defp format_result(:one, query_id, query, [row], _, %{format: :map}),
    do: {:ok, create_maps_from_rows(query_id, query, [row]) |> List.first()}

  defp format_result(:one, query_id, query, [row], _, _),
    do: {:ok, create_schema_from_rows(query_id, query, [row]) |> List.first()}

  defp format_result(:one, _, _, [], _, _), do: {:ok, nil}
  defp format_result(:one, _, _, [_ | _], _, _), do: {:error, :multiple_results}

  defp format_result(:all, _, _, [], _, _), do: {:ok, []}
  defp format_result(:all, _, _, rows, _, %{format: :raw}), do: {:ok, rows}

  defp format_result(:all, query_id, query, rows, _, %{format: :map}),
    do: {:ok, create_maps_from_rows(query_id, query, rows)}

  defp format_result(:all, query_id, query, rows, _, %{format: :schema}),
    do: {:ok, create_schema_from_rows(query_id, query, rows)}

  defp format_result(:insert, _, _, [], _, %{format: format}) when format in [:raw, :map],
    do: {:ok, nil}

  defp format_result(:insert, query_id, query, [], bindings, %{format: :schema}) do
    # Insert without RETURNING. We assume the bindings match the value in the DB. This is mostly
    # true, except for any data transformations that happen at the SQLite layer. If the caller is
    # getting different results than expected, they should include the `returning: true` flag.
    # For optimization purposes, this is the default.
    {:ok, create_schema_from_rows(query_id, query, [bindings]) |> List.first()}
  end

  defp format_result(:insert, query_id, query, [row], _, %{format: :schema}),
    do: {:ok, create_schema_from_rows(query_id, query, [row]) |> List.first()}

  defp format_result(:update, _, _, [], _bindings, %{returning: true}),
    do: {:error, :not_found}

  defp format_result(:update, query_id, query, [row], _bindings, %{returning: true}),
    do: {:ok, create_schema_from_rows(query_id, query, [row]) |> List.first()}

  defp format_result(:update_all, _, _, [], _, %{returning: false}),
    do: {:ok, nil}

  defp format_result(:update_all, _, _, rows, _, %{returning: true}),
    do: {:ok, length(rows)}

  defp format_result(:delete, _, _, [], _, %{returning: true}),
    do: {:error, :not_found}

  defp format_result(:delete, query_id, query, [row], _, %{returning: true}),
    do: {:ok, create_schema_from_rows(query_id, query, [row]) |> List.first()}

  defp format_result(:delete, _, _, [], _, %{returning: false}),
    do: {:ok, nil}

  defp format_result(:delete_all, _, _, [], _, %{returning: false}),
    do: {:ok, nil}

  defp format_result(:delete_all, _, _, rows, _, %{returning: true}),
    do: {:ok, length(rows)}

  defp create_schema_from_rows({_, :pragma, _}, _, rows), do: rows

  defp create_schema_from_rows(query_id, {_, {fields_bindings, _}, :select}, rows) do
    model = Schema.get_model_from_query_id(query_id)
    Enum.map(rows, fn row -> Schema.from_row(model, fields_bindings, row) end)
  end

  defp create_schema_from_rows(query_id, {_, {_, params_bindings}, :insert}, rows) do
    model = Schema.get_model_from_query_id(query_id)

    Enum.map(rows, fn row -> Schema.from_row(model, params_bindings, row) end)
  end

  defp create_schema_from_rows(query_id, {_, _, :update}, rows) do
    model = Schema.get_model_from_query_id(query_id)
    Enum.map(rows, fn row -> Schema.from_row(model, model.__cols__(), row) end)
  end

  defp create_schema_from_rows(query_id, {_, _, :delete}, rows) do
    model = Schema.get_model_from_query_id(query_id)
    Enum.map(rows, fn row -> Schema.from_row(model, model.__cols__(), row) end)
  end

  defp create_maps_from_rows(query_id, {_, {fields_bindings, _}, :select} = query, rows) do
    # Performance-wise, not the best solution, but I'd rather keep the code readable for a bit
    # longer. Simply create the full schema and use only the fields the user selected
    create_schema_from_rows(query_id, query, rows)
    |> Enum.map(fn full_result -> Map.take(full_result, fields_bindings) end)
  end

  defp prepare_query(state, _query_id, sql) do
    # NOTE: On Feeb.DB.Connection I'm using ETS to cache stmt. Measure it.
    with {:ok, stmt} <- SQLite.prepare(state.conn, sql) do
      {:ok, {stmt, sql}}
    end
  end

  defp check_implicit_transaction(nil),
    do: Logger.warning("Running a query with an implicit transaction")

  defp check_implicit_transaction(_), do: :ok

  # TODO: Move to util
  defp gen_transaction_id do
    :rand.uniform()
    |> Kernel.*(1_000_000)
    |> trunc()
  end

  # TODO: These pragma functions will be removed once that gets turned into a hook
  def conditional_pragma_based_on_env(conn, :test, _), do: custom_pragma_for_test(conn)
  def conditional_pragma_based_on_env(conn, _, true), do: custom_pragma_for_test(conn)
  def conditional_pragma_based_on_env(conn, _, _), do: custom_pragma_for_prod(conn)

  defp custom_pragma_for_test(conn) do
    # Note you want to do this at the beginning so it doesn't slow down the
    # test suite by executing some pragmas synchronously.
    :ok = SQLite.exec(conn, "PRAGMA synchronous=0")
  end

  defp custom_pragma_for_prod(conn) do
    :ok = SQLite.exec(conn, "PRAGMA synchronous=1")
  end

  defp normalize_bindings_values(raw_values) when is_list(raw_values) do
    Enum.map(raw_values, fn
      # If a struct was passed as variable, we expect it to implement String.Chars
      %_{} = struct_value ->
        to_string(struct_value)

      other_value ->
        other_value
    end)
  end

  defp assert_release_signal_from_manager!(manager_pid, manager_pid), do: :ok

  if @env == :test do
    defp assert_release_signal_from_manager!(nil, _caller_pid), do: :ok
  end

  defp assert_release_signal_from_manager!(manager_pid, other_pid) do
    "Repo can only be released by its Manager (#{inspect(manager_pid)}, got #{inspect(other_pid)})"
    |> raise()
  end

  # Logger metadata handling

  defp start_custom_log_metadata_scope(custom_metadata) do
    Process.put({:feebdb, :original_scope_metadata}, Logger.metadata())
    Logger.metadata(custom_metadata)
  end

  defp end_custom_log_metadata_scope do
    Logger.reset_metadata(Process.get({:feebdb, :original_scope_metadata}))
  end
end
