defmodule DB.SchemaTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB, as: DB
  alias Sample.{AllTypes, Friend}

  @context :test

  describe "generated: __schema__/0" do
    # `__schema__/0` contains information about fields and their types
    schema = Friend.__schema__()

    assert Map.has_key?(schema, :id)
    assert {id_type, id_opts, id_mod} = schema.id
    assert id_type == Feeb.DB.Type.Integer
    assert id_opts == %{}
    assert id_mod == nil
  end

  describe "generated: __table__/0" do
    assert :friends == Friend.__table__()
    assert :all_types == AllTypes.__table__()
  end

  describe "generated: __context__/0" do
    assert :test == Friend.__context__()
    assert :test == AllTypes.__context__()
  end

  describe "generated: __cols__/0" do
    test "includes all non-virtual fields in order" do
      assert [:id, :name] == Friend.__cols__()

      assert [
               :boolean,
               :boolean_nullable,
               :string,
               :string_nullable,
               :integer,
               :integer_nullable,
               :atom,
               :atom_nullable,
               :uuid,
               :uuid_nullable,
               :datetime_utc,
               :datetime_utc_nullable,
               :datetime_utc_precision_second,
               :datetime_utc_precision_millisecond,
               :datetime_utc_precision_microsecond,
               :datetime_utc_precision_default,
               :map,
               :map_nullable,
               :map_keys_atom,
               :map_keys_safe_atom,
               :map_keys_string,
               :map_keys_default,
               :list,
               :list_nullable,
               :enum,
               :enum_nullable,
               :enum_safe_atom
             ] == AllTypes.__cols__()
    end
  end

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
