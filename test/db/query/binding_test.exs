defmodule Feeb.DB.Query.BindingTest do
  use ExUnit.Case, async: true

  alias Feeb.DB.Query.Binding

  describe "parse_params/3" do
    test "returns the expected params bindings (SELECT)" do
      [
        {"select * from users where id = ?;", [:id]},
        {"select * from users where id = ? and name = ?;", [:id, :name]},
        {"select * from users where inserted_at >= ?", [:inserted_at]},
        {"select * from foo where id > ? and id < ?", [:id, :id]},
        {"select * from foo where bar <= ? or id = ?", [:bar, :id]}
      ]
      |> Enum.each(fn {sql, expected_bindings} ->
        assert expected_bindings == Binding.parse_params(:select, sql, [])
      end)
    end

    test "returns explicit bindings when provided" do
      sql = "select * from users where id = ?;"
      explicit_bindings = [:user_id]
      assert explicit_bindings == Binding.parse_params(:select, sql, explicit_bindings)
    end
  end

  describe "parse_fields/3" do
    test "returns [:*] for SELECT * queries" do
      assert [:*] == Binding.parse_fields(:select, "select * from users;", [])
    end

    test "returns parsed fields for explicit column selection" do
      sql = "select id, name, email from users;"
      assert [:id, :name, :email] == Binding.parse_fields(:select, sql, [])
    end

    test "returns explicit bindings when provided (for JOIN queries)" do
      # When using table aliases like `SELECT c.*`, the parser returns `:"c.*"` which is
      # incorrect. The `@fields` annotation allows overriding this behavior.
      sql = "select c.* from chains c join chain_tunnels ct on c.id = ct.chain_id;"
      explicit_bindings = [:*]
      assert explicit_bindings == Binding.parse_fields(:select, sql, explicit_bindings)
    end

    test "returns empty list for non-SELECT queries" do
      assert [] == Binding.parse_fields(:insert, "insert into users (id) values (?);", [])
      assert [] == Binding.parse_fields(:update, "update users set name = ?;", [])
      assert [] == Binding.parse_fields(:delete, "delete from users where id = ?;", [])
    end
  end

  describe "parse_atstring/1" do
    test "parses field bindings" do
      assert [:*] == Binding.parse_atstring("*]")
      assert [:id, :name] == Binding.parse_atstring("id, name]")
      assert [:foo, :bar, :baz] == Binding.parse_atstring("foo, bar, baz]")
    end
  end
end
