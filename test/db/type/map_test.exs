defmodule Feeb.DB.Type.MapTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "map type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params =
        AllTypes.creation_params(%{
          map: %{foo: "bar"},
          map_keys_string: %{foo: "bar"},
          map_nullable: %{1 => nil}
        })

      # Maps are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.map == %{foo: "bar"}
      assert all_types.map_keys_string == %{"foo" => "bar"}
      assert all_types.map_nullable == %{1 => nil}

      # Maps are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.map == %{foo: "bar"}
      assert db_all_types.map_keys_string == %{"foo" => "bar"}
      assert db_all_types.map_nullable == %{"1": nil}

      # Values are stored as text in the database
      assert [["{\"foo\":\"bar\"}", "{\"1\":null}"]] ==
               DB.raw!("select map, map_nullable from all_types")
    end

    test "stores and loads structs (inside maps)", %{shard_id: shard_id} do
      version = %Version{major: 4, minor: 2, patch: 0}
      uri = URI.new!("foo")

      map = %{uri: uri, version: version}
      stringified_map = Utils.Map.stringify_keys(map)

      # By default, structs are *not* loaded (since loading may cause performance issues and isn't
      # always desirable). When a struct is retrieved in a `load_structs: false` map, we'll simply
      # return the :__struct__ key as is
      map_with_structs_unloaded =
        map
        |> put_in([:uri, Access.key!(:__struct__)], "Elixir.URI")
        |> put_in([:version, Access.key!(:__struct__)], "Elixir.Version")

      params =
        AllTypes.creation_params(%{
          map: map,
          map_load_structs: map,
          map_keys_string: map
        })

      # Structs are parsed correctly
      all_types = AllTypes.new(params)
      assert all_types.map == map
      assert all_types.map_load_structs == map
      assert all_types.map_keys_string == stringified_map

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.map == map_with_structs_unloaded
      assert db_all_types.map_load_structs == map
      assert db_all_types.map_keys_string == stringified_map

      # Structs are stored next to the map, under the __struct__ key, except in the stringified map
      assert [[raw_map, raw_map_load_structs, raw_map_keys_string]] =
               DB.raw!("select map, map_load_structs, map_keys_string from all_types")

      assert raw_map =~ "\"__struct__\":\"Elixir.URI\""
      assert raw_map_load_structs =~ "\"__struct__\":\"Elixir.URI\""
      refute raw_map_keys_string =~ "\"__struct__\":\"Elixir.URI\""
    end

    test "stores and loads struct (at the top level)", %{shard_id: shard_id} do
      version = %Version{major: 4, minor: 2, patch: 0, build: nil, pre: []}

      params =
        AllTypes.creation_params(%{
          map: version,
          map_load_structs: version,
          map_keys_string: version,
          map_keys_safe_atom: version
        })

      stringified_map =
        version
        |> Map.from_struct()
        |> Utils.Map.stringify_keys()

      # Structs are parsed correctly. Note that for map with `keys: :string`, the struct is removed
      # entirely and instead we end up with the raw map with stringified keys
      all_types = AllTypes.new(params)
      assert all_types.map == version
      assert all_types.map_load_structs == version
      assert all_types.map_keys_safe_atom == version
      assert all_types.map_keys_string == stringified_map

      # We can store/load the struct in the database (when keys is atom or safe_atom)
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.map == Map.put(version, :__struct__, "Elixir.Version")
      assert db_all_types.map_load_structs == version
      assert db_all_types.map_keys_safe_atom == Map.put(version, :__struct__, "Elixir.Version")
      assert db_all_types.map_keys_string == stringified_map
    end

    test "stores and loads struct (struct inside struct)", %{shard_id: shard_id} do
      # `v2` has `v1` within it (nested struct)
      v1 = %Version{major: 1, minor: 0, patch: 0, build: nil, pre: []}
      v2 = %Version{major: 2, minor: 0, patch: 0, build: v1, pre: []}

      stringified_v1 = Utils.Map.stringify_keys(v1)
      stringified_v2 = Utils.Map.stringify_keys(v2)

      params =
        AllTypes.creation_params(%{
          map: v2,
          map_load_structs: v2,
          map_keys_string: v2
        })

      all_types = AllTypes.new(params)

      # `map` will keep the struct as-is because it hasn't been serialized to/from JSON yet
      assert all_types.map == v2
      assert all_types.map_load_structs == v2
      assert all_types.map_load_structs.build == v1
      assert all_types.map_keys_string == stringified_v2
      assert all_types.map_keys_string["build"] == stringified_v1

      # We can store/load the struct in the database (when keys are atomified)
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # After loaded, `map` will return the "naive" map whereas `%{load_structs: true}` will iterate
      # over each value in the map and convert any structs it finds along the way
      assert db_all_types.map.__struct__ == "Elixir.Version"
      assert db_all_types.map.build == Map.put(v1, :__struct__, "Elixir.Version")
      assert db_all_types.map_load_structs == v2
      assert db_all_types.map_load_structs.build == v1
      assert db_all_types.map_keys_string == stringified_v2
      assert db_all_types.map_keys_string["build"] == stringified_v1
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
      assert all_types.map_keys_default == input

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # Schema created from the database fields has same values as casted above
      assert db_all_types.map_keys_atom == input
      assert db_all_types.map_keys_safe_atom == input
      assert db_all_types.map_keys_string == input_str
      assert db_all_types.map_keys_default == input

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

  describe "overwrite_opts/3" do
    test "keeps `:keys` if one is given" do
      opts = %{keys: :atom}
      assert opts == DB.Type.Map.overwrite_opts(opts, nil, nil)
    end

    test "adds default `:keys` if none is given" do
      assert %{keys: :atom} == DB.Type.Map.overwrite_opts(%{}, nil, nil)
    end
  end
end
