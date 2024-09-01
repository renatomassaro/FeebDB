defmodule Feeb.DB.Type.DateTimeUTCTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Sample.AllTypes

  @context :test

  describe "datetime_utc type" do
    test "stores and loads correctly", %{shard_id: shard_id} do
      utc = DateTime.utc_now()

      params = AllTypes.creation_params(%{datetime_utc: utc, datetime_utc_nullable: utc})

      # Datetimes are correctly casted
      all_types = AllTypes.new(params)
      assert all_types.datetime_utc == utc
      assert all_types.datetime_utc_nullable == utc

      # Datetimes are correctly dumped and loaded
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.datetime_utc == utc
      assert db_all_types.datetime_utc_nullable == utc

      # Values are stored as text in the database
      assert [[db_utc_value_1, db_utc_value_2]] =
               DB.raw!("select datetime_utc, datetime_utc_nullable from all_types")

      assert db_utc_value_1 == db_utc_value_2
      assert db_utc_value_1 == DateTime.to_string(utc)
    end

    test "casts down to the correct precision", %{shard_id: shard_id} do
      now = ~U[2024-09-01 21:00:00.123456Z]

      params =
        %{
          datetime_utc_precision_default: now,
          datetime_utc_precision_second: now,
          datetime_utc_precision_millisecond: now,
          datetime_utc_precision_microsecond: now
        }
        |> AllTypes.creation_params()

      # Precisions were truncated based on schema configuration
      all_types = AllTypes.new(params)
      assert all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.123Z]
      assert all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.123456Z]
      assert all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.123456Z]

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # Schema created from the database fields has same values as casted above
      assert db_all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert db_all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.123Z]
      assert db_all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.123456Z]
      assert db_all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.123456Z]

      # In the database, these fields are stored as text with the truncated precision
      assert_precision_in_db("datetime_utc_precision_second", "2024-09-01 21:00:00Z")
      assert_precision_in_db("datetime_utc_precision_millisecond", "2024-09-01 21:00:00.123Z")
      assert_precision_in_db("datetime_utc_precision_microsecond", "2024-09-01 21:00:00.123456Z")
      assert_precision_in_db("datetime_utc_precision_default", "2024-09-01 21:00:00.123456Z")
    end

    test "casts 'up' to the correct precision (pads missing values)", %{shard_id: shard_id} do
      now = ~U[2024-09-01 21:00:00Z]

      params =
        %{
          datetime_utc_precision_default: now,
          datetime_utc_precision_second: now,
          datetime_utc_precision_millisecond: now,
          datetime_utc_precision_microsecond: now
        }
        |> AllTypes.creation_params()

      # Precisions were padded based on schema configuration
      all_types = AllTypes.new(params)
      assert all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.000Z]
      assert all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.000000Z]
      assert all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.000000Z]

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # Schema created from the database fields has same values as casted above
      assert db_all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert db_all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.000Z]
      assert db_all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.000000Z]
      assert db_all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.000000Z]

      # In the database, precision contains padded zeroes
      assert_precision_in_db("datetime_utc_precision_second", "2024-09-01 21:00:00Z")
      assert_precision_in_db("datetime_utc_precision_millisecond", "2024-09-01 21:00:00.000Z")
      assert_precision_in_db("datetime_utc_precision_microsecond", "2024-09-01 21:00:00.000000Z")
      assert_precision_in_db("datetime_utc_precision_default", "2024-09-01 21:00:00.000000Z")
    end

    test "casts ISO8601 string into datetime", %{shard_id: shard_id} do
      now = "2024-09-01 21:00:00.123456Z"

      params =
        %{
          datetime_utc_precision_default: now,
          datetime_utc_precision_second: now,
          datetime_utc_precision_millisecond: now,
          datetime_utc_precision_microsecond: now
        }
        |> AllTypes.creation_params()

      # Strings were converted and truncated based on schema configuration
      all_types = AllTypes.new(params)
      assert all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.123Z]
      assert all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.123456Z]
      assert all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.123456Z]

      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)

      # They are loaded correctly
      assert db_all_types.datetime_utc_precision_second == ~U[2024-09-01 21:00:00Z]
      assert db_all_types.datetime_utc_precision_millisecond == ~U[2024-09-01 21:00:00.123Z]
      assert db_all_types.datetime_utc_precision_microsecond == ~U[2024-09-01 21:00:00.123456Z]
      assert db_all_types.datetime_utc_precision_default == ~U[2024-09-01 21:00:00.123456Z]

      # And they are stored correctly
      assert_precision_in_db("datetime_utc_precision_second", "2024-09-01 21:00:00Z")
      assert_precision_in_db("datetime_utc_precision_millisecond", "2024-09-01 21:00:00.123Z")
      assert_precision_in_db("datetime_utc_precision_microsecond", "2024-09-01 21:00:00.123456Z")
      assert_precision_in_db("datetime_utc_precision_default", "2024-09-01 21:00:00.123456Z")
    end

    test "crashes if invalid string is provided" do
      params = AllTypes.creation_params(%{datetime_utc: "foo"})

      %{message: error_msg} =
        assert_raise RuntimeError, fn ->
          AllTypes.new(params)
        end

      assert error_msg =~ "Invalid DateTime string: foo"
    end

    test "supports nullable", %{shard_id: shard_id} do
      params = AllTypes.creation_params(%{datetime_utc_nullable: nil})

      # It casts
      all_types = AllTypes.new(params)
      assert all_types.datetime_utc_nullable == nil

      # It dumps and loads
      DB.begin(@context, shard_id, :write)
      assert {:ok, db_all_types} = DB.insert(all_types)
      assert db_all_types.datetime_utc_nullable == nil
    end

    test "crashes if null value is passed into non-nullable field" do
      params = AllTypes.creation_params(%{datetime_utc: nil})

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
      DB.raw!("update all_types set datetime_utc = null;")

      log =
        capture_log(fn ->
          DB.all(AllTypes)
        end)

      assert log =~ "[warning]"
      assert log =~ "Loaded `nil` value"
      assert log =~ "datetime_utc@"
    end
  end

  defp assert_precision_in_db(field, expected_value) do
    assert [[expected_value]] == DB.raw!("select #{field} from all_types")
  end
end
