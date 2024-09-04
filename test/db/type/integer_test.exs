defmodule Feeb.DB.Type.IntegerTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "integer type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{integer: 10, integer_nullable: -50})

      # Integers are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.integer == 10
      assert all_types.integer_nullable == -50

      # Integers are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.integer == 10
      assert db_all_types.integer_nullable == -50

      # Values are stored as integer in the database
      assert [[10, -50]] == DB.raw!("select integer, integer_nullable from all_types")
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{integer_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.integer_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.integer_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{integer: nil})

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

      # Update incorrect value directly into the DB
      DB.raw!("update all_types set integer = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "integer@"
    end
  end
end
