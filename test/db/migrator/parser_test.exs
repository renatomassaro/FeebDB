defmodule Feeb.DB.Migrator.ParserTest do
  use ExUnit.Case, async: true

  alias Feeb.DB.Migrator.Parser

  describe "queries_from_sql_lines/1" do
    test "filters out comments" do
      [sql_1, sql_2, sql_3] =
        """
        -- Bunch
        -- of comments
        -- at the beginning of the $I(!#*$(!#*&%(*&%^)!(#$*---)-82398@)) file
        --

        --
        --

        -- CREATE TABLE wont_create_this_one
        -- !


        CREATE TABLE foo (
          -- nah
          id INTEGER PRIMARY KEY, -- always use a primary key in your tables
          -- brat
          food TEXT--;
          -- we done;;;--
        ) STRICT; -- strict is important in sqlite
        --


        CREATE TABLE bar (     id INTEGER); -- no primary key?;;;;
        CREATE TABLE foo_bars ( id
        INTEGER, entries -- what is this
        TEXT );
        """
        |> Parser.queries_from_sql_lines()

      assert sql_1 == "CREATE TABLE foo ( id INTEGER PRIMARY KEY, food TEXT ) STRICT"
      assert sql_2 == "CREATE TABLE bar ( id INTEGER)"
      assert sql_3 == "CREATE TABLE foo_bars ( id INTEGER, entries TEXT )"
    end
  end
end
