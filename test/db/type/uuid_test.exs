defmodule Feeb.DB.Type.UuidTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "uuid type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      uuid = Utils.UUID.random()
      uuid_nullable = Utils.UUID.random()
      params = AllTypes.creation_params(%{uuid: uuid, uuid_nullable: uuid_nullable})

      # Uuids are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.uuid == uuid
      assert all_types.uuid_nullable == uuid_nullable

      # Uuids are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.uuid == uuid
      assert db_all_types.uuid_nullable == uuid_nullable

      # Values are stored as text in the database
      assert [[uuid, uuid_nullable]] == DB.raw!("select uuid, uuid_nullable from all_types")
    end

    test "is case insensitive", %{shard_id: shard_id} do
      uuid = "FEEBfeeb-fEEb-feeb-FeeB-FeebFeebFeeb"
      params = AllTypes.creation_params(%{uuid: uuid})

      DB.begin(@context, shard_id, :write)
      all_types = AllTypes.new(params)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.uuid == uuid

      assert [[uuid]] == DB.raw!("select uuid from all_types")
    end

    test "rejects invalid UUIDs" do
      attempt_with = fn extra_param ->
        extra_param
        |> AllTypes.creation_params()
        |> AllTypes.new()
      end

      assert_raise RuntimeError, fn -> attempt_with.(%{uuid: ""}) end
      assert_raise RuntimeError, fn -> attempt_with.(%{uuid: "food"}) end

      assert_raise RuntimeError, fn ->
        # Correct number of characters but has non-hexadecimal in it
        attempt_with.(%{uuid: "Zeebfeeb-feeb-feeb-feeb-feebfeebfeeb"})
      end

      assert_raise RuntimeError, fn ->
        # No dashes. For now at least, this is not supported
        attempt_with.(%{uuid: "feebfeebfeebfeebfeebfeebfeebfeeb"})
      end
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{uuid_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.uuid_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.uuid_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{uuid: nil})

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
      DB.raw!("update all_types set uuid = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "uuid@"
    end
  end
end
