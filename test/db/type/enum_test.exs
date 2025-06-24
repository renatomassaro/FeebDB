defmodule Feeb.DB.Type.EnumTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "enum type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{enum: :one, enum_nullable: "baz", enum_fn: :function})

      # Enums are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.enum == :one
      assert all_types.enum_nullable == "baz"
      assert all_types.enum_fn == :function

      # Enums are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.enum == :one
      assert db_all_types.enum_nullable == "baz"
      assert db_all_types.enum_fn == :function

      # Values are stored as text in the database
      assert [["one", "baz", "function"]] ==
               DB.raw!("select enum, enum_nullable, enum_fn from all_types")
    end

    test "crashes if input is not a possible enum value" do
      attempt_with = fn extra_param ->
        extra_param
        |> AllTypes.creation_params()
        |> AllTypes.new()
      end

      %{message: error_1} =
        assert_raise RuntimeError, fn ->
          AllTypes.new(attempt_with.(%{enum: :four}))
        end

      assert error_1 =~ "Value four is invalid for enum at enum@"
      assert error_1 =~ "Accepted values: [:one, :two, :three]"

      %{message: error_2} =
        assert_raise RuntimeError, fn ->
          AllTypes.new(attempt_with.(%{enum_nullable: "bax"}))
        end

      assert error_2 =~ "Value bax is invalid for enum at enum_nullable@"
      assert error_2 =~ "Accepted values: [\"foo\", \"bar\", \"baz\"]"
    end

    test "casts if atom or string input is passed on string or atom enum" do
      params = AllTypes.creation_params(%{enum: "two", enum_nullable: :foo})

      # Inputs were casted to the corresponding type
      all_types = AllTypes.new(params)
      assert all_types.enum == :two
      assert all_types.enum_nullable == "foo"
    end

    test "safe_atom casting crashes if passed atom does not already exist" do
      # It casts a previously existing atom (any atom that is part of the enum)
      params = AllTypes.creation_params(%{enum_safe_atom: "safe"})
      all_types = AllTypes.new(params)
      assert all_types.enum_safe_atom == :safe

      # It crashes, but it crashes because it's not part of the enum, not because atom doesn't exist
      params = AllTypes.creation_params(%{enum_safe_atom: "shard_id"})

      %{message: error} =
        assert_raise RuntimeError, fn ->
          AllTypes.new(params)
        end

      assert error =~ "Value shard_id is invalid for enum"

      # It fails before converting to atom if atom doesn't exist
      params = AllTypes.creation_params(%{enum_safe_atom: "xyzabcfooxxx"})

      %{message: error} =
        assert_raise ArgumentError, fn ->
          AllTypes.new(params)
        end

      assert error =~ "not an already existing atom"
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{enum_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.enum_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.enum_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{enum: nil})

      assert_raise RuntimeError, fn ->
        AllTypes.new(params)
      end
    end

    @tag capture_log: true
    test "crashes if invalid enum value is returned (loaded) from db", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert!()

      # Update incorrect value directly into the DB
      DB.raw!("update all_types set enum = 'five';")
      DB.commit()

      # Because the exception happens in another process, we need to trap exits here and assert in a
      # different way. We can't use `assert_raise`, unfortunately.
      Process.flag(:trap_exit, true)

      spawn_link(fn ->
        DB.begin(@context, shard_id, :read)
        DB.all(AllTypes)
      end)

      # We get an exception because the value "five" is not a possible enum value
      receive do
        {:EXIT, _, {{%RuntimeError{message: error}, _}, _}} ->
          assert error =~ "Loaded value five that is not part of enum values [:one, :two, :three]"
      after
        1_000 ->
          flunk("No error received")
      end
    end

    @tag capture_log: true
    test "safe_atom loading crashes if atom does not already exist", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      AllTypes.creation_params(%{enum_safe_atom: :safe})
      |> AllTypes.new()
      |> DB.insert!()

      # Update incorrect value directly into the DB
      DB.raw!("update all_types set enum_safe_atom = 'szichqviuh';")
      DB.commit()

      # Because the exception happens in another process, we need to trap exits here and assert in a
      # different way. We can't use `assert_raise`, unfortunately.
      Process.flag(:trap_exit, true)

      spawn_link(fn ->
        DB.begin(@context, shard_id, :read)
        DB.all(AllTypes)
      end)

      # We get an exception because the value "five" is not a possible enum value
      receive do
        {:EXIT, _, {{:badarg, stacktrace}, _}} ->
          assert [{:erlang, :binary_to_existing_atom, _, _} | _] = stacktrace
      after
        1_000 ->
          flunk("No error received")
      end
    end

    test "warns if non-nullable field returned a null value", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # Insert an AllTypes that is valid
      AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert!()

      # Update incorrect value directly into the DB
      DB.raw!("update all_types set enum = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "enum@"
    end
  end

  describe "overwrite_opts/3" do
    test "keeps `format` opt if a valid one is set" do
      opts_1 = %{values: [:a, :b], format: :atom}
      opts_2 = %{values: [:a, :b], format: :safe_atom}
      opts_3 = %{values: ["a", "b'"], format: :string}

      assert opts_1 == DB.Type.Enum.overwrite_opts(opts_1, nil, nil)
      assert opts_2 == DB.Type.Enum.overwrite_opts(opts_2, nil, nil)
      assert opts_3 == DB.Type.Enum.overwrite_opts(opts_3, nil, nil)
    end

    test "crashes if an invalid `format` is given" do
      assert_raise MatchError, fn ->
        %{values: [:a, :b], format: :pizza}
        |> DB.Type.Enum.overwrite_opts(nil, nil)
      end
    end

    test "infers type if no `format` is set" do
      opts_atom = %{values: [:a, :b, :c]}
      opts_string = %{values: ["a", "b", "c"]}

      new_opts_atom = DB.Type.Enum.overwrite_opts(opts_atom, nil, nil)
      assert new_opts_atom.format == :atom

      new_opts_string = DB.Type.Enum.overwrite_opts(opts_string, nil, nil)
      assert new_opts_string.format == :string
    end

    test "crashes if `values` have mixed types" do
      opts = %{values: [:a, "b"]}

      %{message: error} =
        assert_raise RuntimeError, fn ->
          DB.Type.Enum.overwrite_opts(opts, nil, nil)
        end

      assert error =~ "Multiple types in enum"
    end

    test "supports a function as value generator" do
      atom_values_fn = fn -> [:a, :b, :c] end
      str_values_fn = fn -> ["x", "y", "z"] end

      opts = DB.Type.Enum.overwrite_opts(%{values: atom_values_fn, format: :safe_atom}, nil, nil)
      assert opts.values == [:a, :b, :c]
      assert opts.format == :safe_atom

      opts = DB.Type.Enum.overwrite_opts(%{values: atom_values_fn}, nil, nil)
      assert opts.values == [:a, :b, :c]
      assert opts.format == :atom

      opts = DB.Type.Enum.overwrite_opts(%{values: str_values_fn}, nil, nil)
      assert opts.values == ["x", "y", "z"]
      assert opts.format == :string
    end
  end
end
