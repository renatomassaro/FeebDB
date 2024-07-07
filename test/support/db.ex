defmodule Test.Feeb.DB do
  alias Feeb.DB.{Config, Repo}

  def on_start do
    delete_all_dbs()
    File.mkdir_p!(props_path())
    File.mkdir_p!(test_dbs_path())

    Enum.each(Config.contexts(), fn context ->
      File.mkdir_p!("#{test_dbs_path()}/#{context.name}")
    end)

    Test.Feeb.DB.Prop.ensure_props_are_created()
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
    {:ok, pid} = Repo.start_link({context, shard_id, path, :readwrite})

    # We need to close it in order to synchronously finish the migration
    GenServer.call(pid, {:close})
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

  defp delete_all_dbs do
    path = test_dbs_path()
    # TODO: Make this configurable please
    true = String.starts_with?(path, "/tmp/helix")

    "#{path}/**/*.{db,db-shm,db-wal}"
    |> Path.wildcard()
    |> Stream.each(fn path ->
      File.rm(path)
    end)
    |> Stream.run()
  end
end
