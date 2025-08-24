defmodule Feeb.DB.SQLiteTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB.SQLite

  setup %{db: path} do
    conn = Test.Feeb.DB.Setup.test_conn(path)
    {:ok, %{c: conn}}
  end

  describe "prepare/2" do
    test "prepares an statement", %{c: c} do
      assert {:ok, _} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = ?")
      assert {:ok, _} = SQLite.prepare(c, "BEGIN")
      assert {:ok, _} = SQLite.prepare(c, "PRAGMA journal_mode")
      assert {:error, _} = SQLite.prepare(c, "invalidquery")
    end
  end

  describe "bind/3" do
    test "binds variables to a prepared statement", %{c: c} do
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = ?")
      assert :ok = SQLite.bind(stmt, [1])
      {:ok, stmt} = SQLite.prepare(c, "BEGIN")
      assert :ok = SQLite.bind(stmt, [])
    end

    @tag :capture_log
    test "fails when the number of arguments is wrong", %{c: c} do
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = ?")
      assert {:error, :arguments_wrong_length} = SQLite.bind(stmt, [1, 2, 3])
      assert {:error, :arguments_wrong_length} = SQLite.bind(stmt, [])
    end
  end

  describe "one/2" do
    test "returns an entry if found", %{c: c} do
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = ?")
      :ok = SQLite.bind(stmt, [1])
      assert {:ok, [1, "Phoebe", nil]} == SQLite.one(c, stmt)
    end

    test "returns nil if not found", %{c: c} do
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = ?")
      :ok = SQLite.bind(stmt, [999])
      assert {:ok, nil} == SQLite.one(c, stmt)
    end

    test "raises if multiple entries are found", %{c: c} do
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id <= 2")

      assert_raise RuntimeError, fn ->
        SQLite.one(c, stmt)
      end
    end
  end

  describe "all/2" do
    test "always works", %{c: c} do
      # With zero results
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = 0")
      assert {:ok, []} == SQLite.all(c, stmt)

      # With one result
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends WHERE id = 1")
      assert {:ok, [_]} = SQLite.all(c, stmt)

      # With multiple results
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends LIMIT 3")
      assert {:ok, [_, _, _]} = SQLite.all(c, stmt)
    end
  end

  describe "raw/2" do
    test "works as expected", %{c: c} do
      assert {:ok, [[1]]} = SQLite.raw(c, "SELECT id FROM friends WHERE id = 1")
      assert {:ok, [[0]]} = SQLite.raw(c, "PRAGMA synchronous")
      assert {:error, _} = SQLite.raw(c, "invalidquery")
    end
  end

  describe "raw!/2" do
    test "works as expected", %{c: c} do
      assert [[1], [2]] = SQLite.raw!(c, "SELECT id FROM friends WHERE id <= 2")
      assert [["memory"]] = SQLite.raw!(c, "PRAGMA journal_mode")

      # Raises if something goes wrong
      assert_raise MatchError, fn ->
        SQLite.raw!(c, "invalidquery")
      end
    end
  end

  describe "perform/2" do
    test "works as expected", %{c: c} do
      # This makes no sense, but `perform` is meant for queries with no results
      {:ok, stmt} = SQLite.prepare(c, "SELECT * FROM friends LIMIT 3")
      assert :ok = SQLite.perform(c, stmt)

      # No results here
      {:ok, stmt} = SQLite.prepare(c, "UPDATE friends SET name = ? WHERE id = ?")

      SQLite.bind(stmt, ["Jessie", 1])
      assert :ok = SQLite.perform(c, stmt)

      # But the update did go through
      {:ok, stmt} = SQLite.prepare(c, "SELECT name FROM friends WHERE id = 1")
      assert {:ok, ["Jessie"]} == SQLite.one(c, stmt)
    end
  end

  describe "exec/2" do
    test "works as expected", %{c: c} do
      # NOTE: `exec/2` is the same as `perform/2` but with raw queries instead
      assert :ok == SQLite.exec(c, "SELECT * FROM friends LIMIT 3")

      # No results here
      assert :ok == SQLite.exec(c, "UPDATE friends SET name = 'X' WHERE id = 1")

      # But the update did go through
      {:ok, stmt} = SQLite.prepare(c, "SELECT name FROM friends WHERE id = 1")
      assert {:ok, ["X"]} == SQLite.one(c, stmt)
    end
  end
end
