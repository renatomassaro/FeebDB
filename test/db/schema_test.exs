defmodule DB.SchemaTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB, as: DB
  alias Sample.{Friend}

  @context :test

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
end
