defmodule DB.SchemaTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB, as: DB
  alias Sample.{AllTypes, Friend}

  @context :test

  describe "generated: __virtual_cols__/0" do
    test "includes all virtual fields" do
      assert [:divorce_count] == Friend.__virtual_cols__()
      assert [] == AllTypes.__virtual_cols__()
    end
  end

  describe "basic functionalities" do
    test "schema is created correctly when selecting data", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      assert %Friend{id: 1, name: "Phoebe", __meta__: meta} = DB.one({:friends, :get_by_id}, [1])

      assert meta.origin == :db
    end

    test "inserting with query definition", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      friend = Friend.new(%{id: 7, name: "Mike"})
      assert {:ok, inserted_friend} = DB.insert({:friends, :insert}, friend)

      # The data was inserted correctly
      assert inserted_friend.id == friend.id
      assert inserted_friend.name == friend.name

      # They have different metadata though
      assert inserted_friend.__meta__.origin == :db
      assert friend.__meta__.origin == :application
    end
  end

  describe "virtual fields" do
    test "virtual fields are computed and added to the result", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      ross = DB.one({:friends, :get_by_name}, "Ross")
      phoebe = DB.one({:friends, :get_by_name}, "Phoebe")
      monica = DB.one({:friends, :get_by_name}, "Monica")

      assert ross.divorce_count == 3
      assert phoebe.divorce_count == 1
      assert monica.divorce_count == 0
    end
  end
end
