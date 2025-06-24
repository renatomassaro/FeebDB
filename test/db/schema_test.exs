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
      assert [:id, :name, :sibling_count] == Friend.__cols__()

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
               :map_load_structs,
               :map_nullable,
               :map_keys_atom,
               :map_keys_safe_atom,
               :map_keys_string,
               :map_keys_default,
               :list,
               :list_nullable,
               :enum,
               :enum_nullable,
               :enum_safe_atom,
               :enum_fn
             ] == AllTypes.__cols__()
    end
  end

  describe "generated: __virtual_cols__/0" do
    test "includes all virtual fields" do
      assert [:divorce_count, :repo_config] == Friend.__virtual_cols__() |> Enum.sort()
      assert [:virtual, :virtual_with_after_read] == AllTypes.__virtual_cols__() |> Enum.sort()
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
      assert {:ok, inserted_friend} = DB.insert(friend)

      # The data was inserted correctly
      assert inserted_friend.id == friend.id
      assert inserted_friend.name == friend.name

      # They have different metadata though
      assert inserted_friend.__meta__.origin == :db
      assert friend.__meta__.origin == :application
    end
  end

  describe "virtual fields" do
    test "virtual fields hold the `nil` value by default", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      all_types =
        %{integer: 666}
        |> AllTypes.creation_params()
        |> AllTypes.new()

      # Both `virtual` and `virtual_with_after_read` are `nil`. This is the default and nothing has
      # been read so far
      assert nil == all_types.virtual
      assert nil == all_types.virtual_with_after_read

      # After read, we do have a value for `virtual_with_after_read`, but the rest remains intact
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert nil == db_all_types.virtual
      assert 666 == db_all_types.virtual_with_after_read

      assert [db_all_types] == DB.all(AllTypes)
    end

    test "virtual fields are computed and added to the result", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      ross = DB.one({:friends, :get_by_name}, "Ross")
      phoebe = DB.one({:friends, :get_by_name}, "Phoebe")
      rachel = DB.one({:friends, :get_by_name}, "Rachel")
      monica = DB.one({:friends, :get_by_name}, "Monica")

      assert ross.divorce_count == 3
      assert phoebe.divorce_count == 1
      assert rachel.divorce_count == 1
      assert monica.divorce_count == 0

      expected_repo_config =
        %DB.Repo.RepoConfig{
          context: @context,
          shard_id: shard_id,
          mode: :readonly,
          path: DB.Repo.get_path(@context, shard_id)
        }

      assert ross.repo_config == expected_repo_config
      assert phoebe.repo_config == expected_repo_config
      assert rachel.repo_config == expected_repo_config
      assert monica.repo_config == expected_repo_config
    end

    test "virtual fields are completely ignored when inserting data", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # Mike had a divorce before marrying Phoebe
      mike = Friend.new(%{id: 7, name: "Mike", divorce_count: 1})
      assert mike.divorce_count == 1

      # The virtual field `divorce_count` played no role in the insert. When the row was read, we
      # resort to the `after_read` logic (defined at `Sample.Friend.get_divorce_count/3`)
      assert {:ok, db_mike} = DB.insert(mike)
      assert db_mike.divorce_count == 0
    end
  end

  describe "after_read" do
    test "columns with after_read are post-processed", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      joey = DB.one({:friends, :get_by_name}, "Joey")
      rachel = DB.one({:friends, :get_by_name}, "Rachel")
      pheebs = DB.one({:friends, :get_by_name}, "Phoebe")

      assert joey.sibling_count == 7
      assert rachel.sibling_count == 2
      assert pheebs.sibling_count == 1
    end
  end
end
