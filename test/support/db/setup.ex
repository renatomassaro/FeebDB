defmodule Test.Feeb.DB.Setup do
  alias Test.Feeb.DB.Prop
  alias Feeb.DB.{Query, SQLite}

  def test_conn(path) do
    {:ok, conn} = SQLite.open(path)
    "PRAGMA synchronous=OFF" |> SQLite.raw2!(conn)
    "PRAGMA journal_mode=memory" |> SQLite.raw2!(conn)
    conn
  end

  def new_test_db(context, opts \\ []) do
    prop_path = Prop.get_path(context)
    shard_id = Keyword.get(opts, :shard_id, gen_shard_id())

    base_test_db_path = "#{Test.Feeb.DB.test_dbs_path()}/#{context}"
    test_db_path = "#{base_test_db_path}/#{shard_id}.db"

    File.mkdir_p!(base_test_db_path)
    File.cp!(prop_path, test_db_path)
    {:ok, shard_id, test_db_path}
  end

  def compile_test_queries do
    domains = [:friends, :users, :posts]

    try_and_compile = fn domain ->
      try do
        Query.fetch_all!({:test, domain})
      rescue
        _ ->
          Query.compile("priv/test/queries/test/#{domain}.sql", {:test, domain})
      end
    end

    Enum.each(domains, fn domain -> try_and_compile.(domain) end)
  end

  defp gen_shard_id do
    # And here I am thinking we wouldn't ever hit different tests with the same shard_id. I was very
    # wrong. This happened when the test suite had 180 tests. Hence the usage of `strong_rand_bytes`
    :crypto.strong_rand_bytes(8)
    |> :binary.decode_unsigned()
    |> rem(1_000_000)
  end
end
