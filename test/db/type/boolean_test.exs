defmodule Feeb.DB.Type.BooleanTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "boolean type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{boolean: true, boolean_nullable: false})

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
      assert [[1, 0]] == DB.raw!("select boolean, boolean_nullable from all_types")
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{boolean: false, boolean_nullable: nil})

      # We can cast! the value
      all_types = AllTypes.new(params)
      assert all_types.boolean_nullable == nil

      # We can dump! and load! the value
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.boolean_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{boolean: nil})

      assert_raise FunctionClauseError, fn ->
        AllTypes.new(params)
      end
    end

    test "warns if non-nullable field returned a null value", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # Insert an AllTypes that is valid
      AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert!()

      # Update incorrect value directly into the DB (to bypass application-level checks)
      DB.raw!("update all_types set boolean = null;")

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
