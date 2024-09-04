defmodule Feeb.DB.Type.ListTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "list type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      input_1 = [1, "2", true, false]
      input_2 = []

      params = AllTypes.creation_params(%{list: input_1, list_nullable: input_2})

      # Lists are cast correctly
      all_types = AllTypes.new(params)
      assert all_types.list == input_1
      assert all_types.list_nullable == input_2

      # Lists are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.list == input_1
      assert db_all_types.list_nullable == input_2

      # Values are stored as text in the database
      assert [["[1,\"2\",true,false]", "[]"]] ==
               DB.raw!("select list, list_nullable from all_types")
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{list_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.list_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.list_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{list: nil})

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
      DB.raw!("update all_types set list = null;")

      log = capture_log(fn -> DB.all(AllTypes) end)
      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "list@"
    end
  end
end
