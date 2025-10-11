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
  end
end
