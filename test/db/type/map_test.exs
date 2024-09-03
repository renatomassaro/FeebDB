defmodule Feeb.DB.Type.MapTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "map type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{map: %{"foo" => "bar"}, map_nullable: %{}})

      # Maps are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.map == %{"foo" => "bar"}
      assert all_types.map_nullable == %{}

      # Maps are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.map == %{"foo" => "bar"}
      assert db_all_types.map_nullable == %{}

      # Values are stored as text in the database
      assert [["{\"foo\":\"bar\"}", "{}"]] == DB.raw!("select map, map_nullable from all_types")
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{map_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.map_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.map_nullable == nil
    end

    test "casts/loads the map based on the `keys` configuration", %{shard_id: shard_id} do
      input = %{foo: %{bar: "baz"}}
      input_str = %{"foo" => %{"bar" => "baz"}}

      params =
        %{
          map_keys_atom: input,
          map_keys_safe_atom: input,
          map_keys_string: input,
          map_keys_default: input
        }
        |> AllTypes.creation_params()

      # Maps were properly cast
      all_types = AllTypes.new(params)
      assert all_types.map_keys_atom == input
      assert all_types.map_keys_safe_atom == input
      assert all_types.map_keys_string == input_str
      assert all_types.map_keys_default == input_str

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # Schema created from the database fields has same values as casted above
      assert db_all_types.map_keys_atom == input
      assert db_all_types.map_keys_safe_atom == input
      assert db_all_types.map_keys_string == input_str
      assert db_all_types.map_keys_default == input_str

      # In the database, these fields are stored as text with the exact same value
      expected_value = "{\"foo\":{\"bar\":\"baz\"}}"
      assert [[expected_value]] == DB.raw!("select map_keys_atom from all_types")
      assert [[expected_value]] == DB.raw!("select map_keys_safe_atom from all_types")
      assert [[expected_value]] == DB.raw!("select map_keys_string from all_types")
      assert [[expected_value]] == DB.raw!("select map_keys_default from all_types")
    end

    test "crashes if an unsafe atom is passed to keys: safe_atom" do
      # `jchqushi` will not exist in atom form
      params = AllTypes.creation_params(%{map_keys_safe_atom: %{"jchqushi" => "foo"}})

      %{message: error} =
        assert_raise ArgumentError, fn ->
          AllTypes.new(params)
        end

      assert error =~ "not an already existing atom"
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{map: nil})

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
      DB.raw!("update all_types set map = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "map@"
    end
  end
end
