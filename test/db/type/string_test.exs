defmodule Feeb.DB.Type.StringTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "string type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{string: "Hello", string_nullable: "Joe"})

      # Strings are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.string == "Hello"
      assert all_types.string_nullable == "Joe"

      # Strings are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.string == "Hello"
      assert db_all_types.string_nullable == "Joe"

      # Values are stored as text in the database
      assert [["Hello", "Joe"]] == DB.raw!("select string, string_nullable from all_types")
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{string: "Hi", string_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.string_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.string_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{string: nil})

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
      DB.raw!("update all_types set string = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "string@"
    end
  end
end
