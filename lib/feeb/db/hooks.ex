defmodule Feeb.DB.Hooks do
  @moduledoc """
  Hooks allow an application or library to run custom code as a response to events that happen to
  a shard or a transaction.

  ## Registering a hook

      Feeb.DB.Hooks.register(:after_commit, :my_app_wake, &MyApp.Outbox.on_commit/1)

  - Several hooks per type, each under an id. They run in registration order. Registering an id
    again replaces the hook.
  - A hook is only applied after it is registered.
  - Hooks run in the calling process.
  - A hook that opens a transaction must commit or roll it back itself, including when it raises:
    FeebDB doesn't clean it up. Left open, it stays the current context, and the next commit or
    rollback acts on it instead.

  ## `:before_commit`

  Runs for every write transaction committed with `DB.commit/0`, except those passing the
  `hooks: false` flag. It runs inside the transaction, after the caller's last statement and before
  COMMIT.

  - It may run queries on the shard; the transaction is still the current context.
  - Nothing it writes is captured: capture is turned off before the hooks run.
  - It may call `Feeb.DB.assign/2`, for example to hand the id of a row it wrote to an
    `:after_commit` hook.
  - Every before_commit hook gets the same payload. Assigns set by an earlier hook are visible
    through `Feeb.DB.assigns/0`, not in the payload's `assigns`.
  - It must not commit or roll back the transaction, nor modify to a different context.
  - If it raises, throws or exits, the error propagates from `Feeb.DB.commit/0` and the transaction
    stays open, as after a failed query. It is up to the caller to roll it back.
  - Currently only supported for write transactions.

  Payload:

      %{
        context: atom,
        shard_id: integer,
        capture?: boolean,
        changes: [map],
        assigns: map
      }

  ## `:after_commit`

  Runs after COMMIT and after the connection is released (so it doesn't hold the shard's write
  lock). Writes made in this hook are, naturally, a new transaction, and might trigger other hooks.

  - If COMMIT itself fails, `Feeb.DB.commit/0` rolls back and returns the error, and the
    `:after_commit` hooks don't run.
  - Currently only supported for write transactions. This might change in the future, just waiting
    for an use-case to show up.

  ## `:on_shard_boot`

  Runs once per shard during `Feeb.DB.Boot.run/0`, inside the boot's write transaction on that
  shard, after its migrations.

  In the event of errors, the boot transaction still commits with what the hook wrote on it before
  failing.

  Payload: `%{context: atom, shard_id: integer, capture?: boolean}`
  """

  require Logger

  @types [:before_commit, :after_commit, :on_shard_boot]

  @doc """
  Registers `callback` as the `type` hook under `id`.

  If `id` is new, the hook will be registered after previously registered ones under the same
  `type`. Otherwise, it will replace the position of the previously registered `id`.
  """
  @spec register(:before_commit | :after_commit | :on_shard_boot, term(), (map() -> term())) ::
          :ok
  def register(type, id, callback) when type in @types and is_function(callback, 1) do
    :persistent_term.put(
      {__MODULE__, type},
      List.keystore(get_hooks(type), id, 0, {id, callback})
    )
  end

  @doc false
  def run(:before_commit, payload), do: run_raising(:before_commit, payload)
  def run(type, payload), do: run_rescuing(type, payload)

  defp run_raising(type, payload),
    do: Enum.each(get_hooks(type), fn {_id, callback} -> callback.(payload) end)

  defp run_rescuing(type, payload) do
    Enum.each(get_hooks(type), fn {id, callback} ->
      try do
        callback.(payload)
      catch
        kind, reason ->
          error = Exception.format(kind, reason, __STACKTRACE__)
          Logger.error("FeebDB #{type} hook #{inspect(id)} failed: #{error}")
      end
    end)
  end

  defp get_hooks(type), do: :persistent_term.get({__MODULE__, type}, [])
end
