defmodule Feeb.DB.Type.BooleanTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  @db_index_boolean 0
  @db_index_boolean_nullable 1

  describe "boolean type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = %{
        boolean: true,
        boolean_nullable: false
      }

      # Booleans are saved correctly in the Schema
      all_types = AllTypes.new(params)
      assert all_types.boolean == true
      assert all_types.boolean_nullable == false

      # Values are returned as boolean from database
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.boolean == true
      assert db_all_types.boolean_nullable == false

      # Values are stored as integer in the database
      [row] = DB.raw!("select * from all_types")
      assert Enum.at(row, @db_index_boolean) == 1
      assert Enum.at(row, @db_index_boolean_nullable) == 0
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = %{
        boolean: false,
        boolean_nullable: nil
      }

      # We can cast! the value
      all_types = AllTypes.new(params)
      assert all_types.boolean_nullable == nil

      # We can dump! and load! the value
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.boolean_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = %{
        boolean: nil
      }

      assert_raise FunctionClauseError, fn ->
        AllTypes.new(params)
      end
    end

    test "warns if non-nullable field returned a null value", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # Insert incorrect value directly into the DB (to bypass application-level checks)
      DB.raw!("insert into all_types (boolean) values (null);")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value from non-nullable field"
      assert log =~ "boolean@"
      assert log =~ "AllTypes\n"
    end
  end
end
