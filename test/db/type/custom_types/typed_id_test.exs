defmodule Feeb.DB.Type.CustomTypes.TypedIDTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB
  alias Sample.CustomTypes
  alias Sample.Types.TypedID

  @context :test

  describe "TypedID custom type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = CustomTypes.creation_params(%{typed_id: 50})
      custom_types = CustomTypes.new(params)

      # TypedID is cast as a struct
      assert custom_types.typed_id == %TypedID{id: 50}

      # It is dumped and loaded correctly
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_custom_types} = DB.insert(custom_types)
      assert db_custom_types.typed_id == %TypedID{id: 50}

      # Value is stored as integer in the database
      assert [[50]] == DB.raw!("select typed_id from custom_types")

      # Reading it loads the type correctly
      assert [row] = DB.all(CustomTypes)
      assert row.typed_id == %TypedID{id: 50}
    end
  end
end
