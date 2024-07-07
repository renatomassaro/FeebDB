defmodule Test.Setup.Shared do
  alias Feeb.DB, as: DB

  def with_db(%{shard_id: shard_id, db_context: db_context}, mode \\ :write) do
    # context =
    #   if db_context in [:sample, :raw] do
    #     :test
    #   else
    #     db_context
    #   end

    context = db_context

    case Process.get(:repo_pid) do
      pid when is_pid(pid) -> :noop
      nil -> DB.begin(context, shard_id, mode)
    end

    :ok
  end

  def with_lobby_db(%{shard_id: shard_id, db_context: :lobby}) do
    DB.begin(:lobby, shard_id, :write)
    :ok
  end

  def with_events(_ \\ []) do
    Process.put(:emit_events, true)
    :ok
  end

  # def with_db_listener(_ \\ []) do
  #   repo_pid = Process.get(:repo_pid)
  #   conn = :sys.get_state(repo_pid).conn
  #   {:ok, listener_pid} = Test.DB.Listener.start_link(self())
  #   :ok = Exqlite.Sqlite3.set_update_hook(conn, listener_pid)
  # end
end
