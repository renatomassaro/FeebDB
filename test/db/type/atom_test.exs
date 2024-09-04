defmodule Feeb.DB.Type.AtomTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "atom type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      # Fun fact: `true == :true`
      params = AllTypes.creation_params(%{atom: :foo, atom_nullable: :bar})

      # Atoms are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.atom == :foo
      assert all_types.atom_nullable == :bar

      # Atoms are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.atom == :foo
      assert db_all_types.atom_nullable == :bar

      # Values are stored as text in the database
      assert [["foo", "bar"]] == DB.raw!("select atom, atom_nullable from all_types")
    end

    test "does not accept `true`, `false` and `nil`" do
      # `true`, `false` and `nil` are, technically, atoms. We don't want to support them to avoid
      # silent errors. If the user wants to store true/false, use `:boolean` type. If `nil`, it
      # must have the `nullable` flag.
      attempt_with = fn extra_param ->
        extra_param
        |> AllTypes.creation_params()
        |> AllTypes.new()
      end

      assert_raise FunctionClauseError, fn -> attempt_with.(%{atom: true}) end
      assert_raise FunctionClauseError, fn -> attempt_with.(%{atom: false}) end
      assert_raise FunctionClauseError, fn -> attempt_with.(%{atom: nil}) end

      # It does not raise in this case because `atom_nullable` has the `nullable` flag
      AllTypes.new(%{atom_nullable: nil})
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{atom_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.atom_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.atom_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{atom: nil})

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
      DB.raw!("update all_types set atom = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "atom@"
    end
  end
end
