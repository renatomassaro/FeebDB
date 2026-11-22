defmodule Feeb.DB.BootTest do
  # Not async because migrate_shards/0 opens every shard (see delete_all_dbs_but_this_one/1)
  use Test.Feeb.DBCase, async: false
  alias Feeb.DB.{Boot, Hooks, LocalState, Migrator, SQLite}
  alias Feeb.DB, as: DB

  @moduletag db: :raw

  setup %{db: db} do
    {:ok, conn} = SQLite.open(db)
    SQLite.raw!(conn, "PRAGMA synchronous=0")
    {:ok, %{conn: conn}}
  end

  describe "boot" do
    @tag unit: true
    test "stores migrations in persistent term" do
      assert [:crm, :erp, :events, :lobby, :test, :will_fail] ==
               Migrator.get_all_migrations() |> Map.keys() |> Enum.sort()
    end

    @tag unit: true
    test "stores latest versions in persistent term" do
      all_migrations = Migrator.get_all_migrations()

      # Test migrations rarely change and thus can be hard-coded here (string because it gets
      # formatted to 123_456_789_012 otherwise, which is hard to read).
      expected_test_latest = "250426163848" |> String.to_integer()
      test_latest = Migrator.get_latest_version(:test)

      assert test_latest == expected_test_latest
      assert test_latest == Migrator.calculate_latest_version(:test, all_migrations)
    end
  end

  describe "migrate_shards/0" do
    test "runs the on_shard_boot hook once per shard",
         %{db: db, shard_id: shard_id} do
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)
      on_exit(&Test.Feeb.DB.reset_hooks/0)

      test_pid = self()

      Hooks.register(:on_shard_boot, :test, fn payload ->
        send(test_pid, {payload, LocalState.get_current_context!()})
      end)

      Hooks.register(:before_commit, :test, fn _ -> send(test_pid, :before_commit) end)
      Hooks.register(:after_commit, :test, fn _ -> send(test_pid, :after_commit) end)

      Boot.migrate_shards()

      # Commit hooks don't run during boot
      refute_received :before_commit
      refute_received :after_commit

      {:messages, messages} = Process.info(self(), :messages)
      shards = Enum.map(messages, fn {%{context: ctx, shard_id: id}, _} -> {ctx, id} end)

      assert shards == Enum.uniq(shards)

      # Global, setup and application shards
      assert {:lobby, 1} in shards
      assert {:test, -1} in shards
      assert {:raw, shard_id} in shards

      # The hook runs inside a write transaction, with capture and hooks disabled
      Enum.each(messages, fn {%{context: ctx, shard_id: id}, state} ->
        assert %{context: ^ctx, shard_id: ^id, access_type: :write} = state
        assert %{capture?: false, hooks?: false} = state
      end)

      # The payload tells whether the context has capture enabled
      assert [%{capture?: false}] =
               messages
               |> Enum.filter(&match?({%{context: :uncaptured}, _}, &1))
               |> Enum.map(&elem(&1, 0))

      assert [%{capture?: true}] =
               messages
               |> Enum.filter(&match?({%{context: :test, shard_id: -1}, _}, &1))
               |> Enum.map(&elem(&1, 0))
    end

    test "a failing on_shard_boot hook is logged and does not stop the boot",
         %{db: db, shard_id: shard_id} do
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)
      on_exit(&Test.Feeb.DB.reset_hooks/0)

      test_pid = self()
      Hooks.register(:on_shard_boot, :failing, fn _ -> raise "boom" end)

      Hooks.register(:on_shard_boot, :working, fn %{context: ctx, shard_id: id} ->
        send(test_pid, {:ran, ctx, id})
      end)

      log = capture_log(fn -> assert :ok == Boot.migrate_shards() end)

      assert log =~ "FeebDB on_shard_boot hook :failing failed"
      assert log =~ "boom"
      assert_received {:ran, :lobby, 1}
      assert_received {:ran, :test, -1}
      assert_received {:ran, :raw, ^shard_id}
    end
  end

  describe "run/0" do
    # Boot.run/0 warns while it recompiles the queries
    @tag capture_log: true
    test "compiles the queries before migrating, so on_shard_boot hooks can use them",
         %{db: db} do
      Test.Feeb.DB.delete_all_dbs_but_this_one(db)
      on_exit(&Test.Feeb.DB.reset_hooks/0)

      # Shard -1 of the test context is a setup shard, so the boot migrates it
      test_pid = self()

      Hooks.register(:on_shard_boot, :test, fn
        # Use a hand-written query, since templated ones are compiled on demand
        %{context: :test, shard_id: -1} ->
          send(test_pid, {:ran, DB.one({:friends, :get_by_id}, [1])})

        _ ->
          :ok
      end)

      # Pretend the queries were never compiled
      :persistent_term.erase({:db_sql_queries, {:test, :friends}})

      Boot.run()

      assert_received {:ran, _}
    end
  end

  describe "validate_database/2" do
    @tag db: :test
    test "raises when the column order does not match the schema",
         %{shard_id: shard_id} do
      DB.begin(:test, shard_id, :write)

      # The uncaptured context includes the test domain, so Friend gets checked too
      assert :ok == Boot.validate_database(Boot.get_all_models(), :uncaptured)

      DB.raw!("DROP TABLE friends")
      DB.raw!("CREATE TABLE friends (name TEXT, id INTEGER PRIMARY KEY, sibling_count INTEGER)")

      e =
        assert_raise RuntimeError, fn ->
          Boot.validate_database(Boot.get_all_models(), :uncaptured)
        end

      assert e.message ==
               "Column order mismatch: [:name, :id, :sibling_count]/[:id, :name, :sibling_count] " <>
                 "@ Elixir.Sample.Friend"

      DB.rollback()
    end

    @tag db: :test
    test "raises when the primary key does not match the schema", %{shard_id: shard_id} do
      DB.begin(:test, shard_id, :write)
      DB.raw!("DROP TABLE friends")
      DB.raw!("CREATE TABLE friends (id INTEGER, name TEXT, sibling_count INTEGER)")

      e =
        assert_raise RuntimeError, fn ->
          Boot.validate_database(Boot.get_all_models(), :test)
        end

      assert e.message == ~s(Primary key mismatch: ["id"]/[] @ Elixir.Sample.Friend)

      # Only contexts with capture enabled check the primary key
      assert :ok == Boot.validate_database(Boot.get_all_models(), :uncaptured)

      # A UNIQUE index on the schema's keys works in place of a primary key
      DB.raw!("CREATE UNIQUE INDEX friends_id_idx ON friends (id)")
      assert :ok == Boot.validate_database(Boot.get_all_models(), :test)

      DB.rollback()
    end

    @tag skip: true
    @tag db: :lobby
    test "crashes if the model does not match the code", %{shard_id: shard_id} do
      all_models = Boot.get_all_models()
      context = :saas_prod_one

      DB.begin(context, shard_id, :write)

      # Reason for skipping: schema should support @context and @domain module attrs
      # And then, we group every relevant model in the given context based on the
      # domains used by the context.
      # For example, the saas_prod_one should group every model from the :events and
      # :crm domains
      # In fact, I actually think that the Schema should define EITHER @context or @domain
      # If it defines the @domain, it means that domain is actually shared. If it defines
      # the context, then it is used in a "single-domain context".

      # Let's insert a column in `crm_contacts`
      {:ok, _} = DB.raw("ALTER TABLE crm_contacts ADD COLUMN should_not_exist_1 TEXT;")

      IO.inspect(all_models)

      e =
        assert_raise RuntimeError, fn ->
          Boot.validate_database(all_models, context)
        end

      assert e.message =~ "fields do not match: [:should_not_exist_1]"

      DB.rollback()

      # With the changed rolled back, it works as expected
      DB.begin(context, shard_id, :read)
      Boot.validate_database(all_models, context)
      DB.commit()

      # Now we'll insert a column in `events_dql` (from the `events` domain)
      DB.begin(context, shard_id, :write)
      {:ok, _} = DB.raw("ALTER TABLE events_dlq ADD COLUMN should_not_exist_2 TEXT;")

      e =
        assert_raise RuntimeError, fn ->
          Boot.validate_database(all_models, context)
        end

      # This ensures that every domain from a context is being checked on boot
      assert e.message =~ "fields do not match: [:should_not_exist_2]"

      DB.rollback()
    end
  end
end
