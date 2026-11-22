defmodule Test.Feeb.DB do
  alias Feeb.DB.{Config, Repo}

  def on_start do
    delete_all_dbs()
    File.mkdir_p!(props_path())
    File.mkdir_p!(test_dbs_path())

    contexts = Config.contexts()

    Enum.each(contexts, fn context ->
      File.mkdir_p!("#{test_dbs_path()}/#{context.name}")
    end)

    # Create all the props, which will be re-used for each test
    Enum.each(contexts, &Test.Feeb.DB.Prop.create/1)
  end

  def on_finish do
    delete_all_dbs()
  end

  @doc """
  Given the following shard_id, make sure it is migrated
  """
  def ensure_migrated(context, shard_id) do
    path = Repo.get_path(context, shard_id)

    # The repo will automatically set up and migrate (if needed)
    {:ok, pid} = Repo.start_link({context, shard_id, path, :readwrite, nil})

    # We need to close it in order to synchronously finish the migration
    GenServer.call(pid, {:close})
  end

  @doc """
  Runs `fun` in a transaction on `{context, shard_id}` and commits it. On a raise, throw or exit,
  rolls it back and re-raises. Raises a `MatchError` if the commit returns an error.
  """
  def with_write({context, shard_id}, fun), do: transaction(context, shard_id, :write, fun)
  def with_read({context, shard_id}, fun), do: transaction(context, shard_id, :read, fun)

  defp transaction(context, shard_id, type, fun) do
    Feeb.DB.with_context(fn ->
      Feeb.DB.begin(context, shard_id, type)

      {:ok, result} =
        try do
          result = fun.()

          with :ok <- Feeb.DB.commit(), do: {:ok, result}
        catch
          kind, reason ->
            Feeb.DB.rollback()
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      result
    end)
  end

  @doc """
  Removes every registered hook.
  """
  def reset_hooks do
    Enum.each([:before_commit, :after_commit, :on_shard_boot], fn type ->
      :persistent_term.erase({Feeb.DB.Hooks, type})
    end)
  end

  def props_path, do: "#{Test.tmp_path()}/db_props"
  def test_dbs_path, do: "#{Config.data_dir()}"
  def test_queries_path, do: "priv/test/queries"

  def random_autoincrement(schema) do
    table = schema.__table__()
    rand = :rand.uniform() |> Kernel.*(1_000_000) |> trunc()
    Feeb.DB.raw!("insert into #{table} (id) values (#{rand})")
    Feeb.DB.raw!("delete from #{table} where id = #{rand}")
  end

  @doc """
  Every test has the capability to create its own shard, which is fully isolated from any other
  test. As such, it's very common for me to not close/release/commit connections that were opened
  during the test, resulting in these shards being effectively locked.

  That's not a problem, since these shards are never to be used inside another test. However, when
  triggering `feeb_db.migrate`, we end up migrating every shard that is in the data directory,
  many of which are locked because the Repo has an open transaction.

  As a workaround, I'm simply deleting every shard (except the one passed as argument), which is why
  callers of this function need to run with `async: false`.

  Note, however, that _even_ if I closed the connections from every test, the callers of this
  function would probably have flakes if they were to run with `async: true`: it's possible (or,
  rather, certain) that some test would have its shard migrated midway, causing inconsistencies or
  unexpected locks.

  In the future, it may make sense to refactor the `feeb_db.migrate` task to accept arguments that
  specify a custom data directory. By using a custom data dir, I'd be able to "mock" the shards
  without affecting other tests, and then we could use `async: true` in such tests.
  """
  def delete_all_dbs_but_this_one(db) do
    "#{Config.data_dir()}/**/*.db"
    |> Path.wildcard()
    |> Enum.reject(fn path -> path == db end)
    |> Enum.each(fn path -> File.rm(path) end)
  end

  defp delete_all_dbs do
    path = test_dbs_path()
    false = String.contains?(path, "*")
    false = String.contains?(path, " ")

    "#{path}/**/*.db*"
    |> Path.wildcard()
    |> Stream.each(fn path -> File.rm(path) end)
    |> Stream.run()
  end
end
