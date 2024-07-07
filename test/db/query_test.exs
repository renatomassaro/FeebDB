defmodule Feeb.DB.QueryTest do
  use ExUnit.Case, async: true

  alias Feeb.DB.{Query}

  @queries_path "priv/test/queries"
  @chaos_path "#{@queries_path}/test/chaos.sql"

  # TODO: Add a test case where I actually compile all the real queries
  describe "compile/2" do
    test "handles chaos.sql file" do
      Query.compile(@chaos_path, {:test, :chaos})

      [
        :get,
        :create_user,
        :update_password,
        :update_password2,
        :delete,
        :delete2
      ]
      |> Enum.each(fn query_name ->
        assert_chaos_query(
          query_name,
          Query.fetch!({:test, :chaos, query_name})
        )
      end)
    end
  end

  defp assert_chaos_query(:get, query) do
    {sql, {fields_b, params_b}, qt} = query
    assert sql == "select * from accounts limit 1;"
    assert fields_b == [:*]
    assert params_b == []
    assert qt == :select
  end

  defp assert_chaos_query(:create_user, query) do
    {sql, {fields_b, params_b}, qt} = query

    assert sql ==
             "insert into accounts ( id, username, email ) values ( ?, ?, ? );"

    assert fields_b == []
    assert params_b == [:id, :username, :email]
    assert qt == :insert
  end

  defp assert_chaos_query(:update_password, query) do
    {sql, {fields_b, params_b}, qt} = query
    assert sql == "update accounts set password = ? where id = ?;"
    assert fields_b == []
    assert params_b == [:password, :id]
    assert qt == :update
  end

  defp assert_chaos_query(:update_password2, query) do
    {sql, {fields_b, params_b}, qt} = query
    assert sql == "update accounts set password = ? where id = ?;"
    assert fields_b == []
    assert params_b == [:pwd, :account_id]
    assert qt == :update
  end

  defp assert_chaos_query(:delete, query) do
    {sql, {fields_b, params_b}, qt} = query
    assert sql == "delete from accounts where id = ? and email = ?;"
    assert fields_b == []
    assert params_b == [:id, :email]
    assert qt == :delete
  end

  defp assert_chaos_query(:delete2, query) do
    {sql, {fields_b, params_b}, qt} = query
    assert sql == "delete from accounts where id = ? and email = ?;"
    assert fields_b == []
    assert params_b == [:acc_id, :email_address]
    assert qt == :delete
  end
end
