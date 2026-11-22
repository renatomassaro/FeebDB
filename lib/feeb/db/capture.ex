defmodule Feeb.DB.Capture do
  @moduledoc """
  When capture is enabled, all inserts, updates or deletes performed in a transaction are tracked.
  The changes are added to the LocalState and handed over to the after_commit hook, which allows
  an application or library to do whatever it wants to with the data (journal, audit log, cache
  invalidation etc).

  ## Turning it on

  Capture is off by default. You turn it on per context:

      config :feebdb, contexts: %{accounts: %{shard_type: :dedicated, capture: true}}

  To skip it for a single transaction, begin it with `capture: false`. Transactions skipping hooks
  (via `hooks: false`) don't capture either, since the changes would never be delivered to the
  application or library.

  ## What is captured

  Every `insert`, `update`, `delete`, `update_all` and `delete_all` that goes through FeebDB's query
  API is captured. Anything FeebDB doesn't see isn't captured: `DB.raw/2`, FK cascades, triggers
  and migrations.

  The hooks receive the changes in the order the statements ran. Each change looks like this:

      %{
        "t" => "posts",
        "op" => "insert" | "update" | "delete",
        "pk" => %{"id" => 1},
        "row" => %{"id" => 1, "title" => "Hello", ...}
      }

  `row` is the full row as it was written, or, for a delete, as it was before being deleted. Values
  are as SQLite stores them. If the schema has no primary key, `pk` is the whole row.

  In a capturing transaction, an insert returns the row as SQLite stored it (for example with the
  id SQLite assigned).

  Limitations and known issues, to be addressed eventually:

  - An upsert is always recorded as an `"insert"`.
  - A hand-written RETURNING clause other than `RETURNING *` will not work as expected.
  - INSERT OR REPLACE / REPLACE INTO are not supported.

  ## Requirements

  To capture a change, FeebDB needs to know which row was touched. In practice, this means:

  - The schema's `@primary_keys` must match the table's.
  - The schema's fields must be in the same order as the table's columns.

  Both are checked at boot time. When a change can't be captured, the statement raises
  `Feeb.DB.CaptureError`. By then the statement has already run; it is the caller responsibility to
  roll back the transaction. If the caller rescues the error and commits anyways, `Feeb.DB.commit/0`
  rolls the transaction back instead and returns `{:error, :capture_failed}`.
  """

  alias Feeb.DB.{CaptureError, LocalState, Repo, Schema}

  @write_types [:insert, :update, :update_all, :delete, :delete_all]

  @doc false
  def opts(type, opts) when type in @write_types do
    case LocalState.get_current_context() do
      %{access_type: :write, capture?: true} ->
        Keyword.put(opts, :feebdb_capture, true)

      _ ->
        opts
    end
  end

  def opts(_, opts), do: opts

  @doc false
  def changes(repo_pid, type, {_, table, _} = query_id, rows) do
    model = Schema.get_model_from_query_id(query_id)

    Enum.map(rows, fn row ->
      # Captured statements return `RETURNING *`, in the schema's column order. This assumption is
      # verified at boot time
      row = model.__cols__() |> Enum.zip(row) |> Map.new()

      %{
        "t" => Atom.to_string(table),
        "op" => op(type),
        "pk" => pk(model, row),
        "row" => stringify(row)
      }
    end)
  rescue
    # If something goes wrong here, we are in a vulnerable position: the transaction is still open,
    # the changes already got executed, we were asked to capture them but couldn't.
    # We will reraise the error, so the caller dies too (and once it dies, Process.monitor at the
    # Repo.Manager will roll back the transaction accordingly). However, if the application decides
    # to catch the exception and commit anyways, the capture is lost. Hence `flag_capture_failed/1`,
    # so the Repo knows not to accept any COMMITs from that transaction.
    e ->
      Repo.flag_capture_failed(repo_pid)
      reraise e, __STACKTRACE__
  end

  defp op(:insert), do: "insert"
  defp op(t) when t in [:update, :update_all], do: "update"
  defp op(t) when t in [:delete, :delete_all], do: "delete"

  defp pk(model, row) do
    case model.__primary_keys__() do
      nil ->
        stringify(row)

      keys ->
        pk = Map.new(keys, &{Atom.to_string(&1), Map.get(row, &1)})

        # Raise if we can't identify the row (PK is missing or NULL)
        if Enum.any?(Map.values(pk), &is_nil/1),
          do: raise(CaptureError, "#{inspect(model)}: change without primary key: #{inspect(pk)}")

        pk
    end
  end

  defp stringify(row), do: Map.new(row, fn {k, v} -> {Atom.to_string(k), v} end)
end
