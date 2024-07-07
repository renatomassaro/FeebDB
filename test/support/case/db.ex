defmodule Test.Feeb.DBCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      import ExUnit.CaptureLog
      import Test.Setup.Shared
      import Test.Assertions
      import Test.Finders
      import Test.Utils

      alias Test.Setup
      alias Test.Samples
    end
  end

  setup tags do
    if Map.get(tags, :init_db, true) do
      do_setup(tags)
    else
      {:ok, %{}}
    end
  end

  def do_setup(tags) do
    # TODO: Skip setup on tests with `unit: true` tags
    context = Map.get(tags, :db, :test)

    {_, {:ok, shard_id, path}} = :timer.tc(fn -> Test.Feeb.DB.Setup.new_test_db(context) end)

    Test.Feeb.DB.Setup.compile_test_queries()

    {:ok, %{db: path, shard_id: shard_id, db_context: context}}
  end
end
