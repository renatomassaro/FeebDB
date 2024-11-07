defmodule Feeb.DB.BootTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB.{Boot, Migrator, SQLite}
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
      expected_test_latest = "241020150400" |> String.to_integer()
      test_latest = Migrator.get_latest_version(:test)

      assert test_latest == expected_test_latest
      assert test_latest == Migrator.calculate_latest_version(:test, all_migrations)
    end
  end

  describe "validate_database/2" do
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
