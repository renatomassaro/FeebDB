defmodule Feeb.DB.QueryTest do
  # Reason for `async: false`: this test suite interacts directly with the compiled queries cache.
  # While it's technically possible to adapt the cache to be per-test, I don't think it's worth the
  # added complexity. As such, I'd rather have this test suite running separately from the rest.
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias Feeb.DB.{Query}

  @queries_path "priv/test/queries"
  @chaos_path "#{@queries_path}/test/chaos.sql"
  @friends_path "#{@queries_path}/test/friends.sql"
  @order_items_path "#{@queries_path}/test/order_items.sql"
  @all_types_path "#{@queries_path}/test/all_types.sql"

  setup do
    # Ensure that each test starts with a "clean slate"
    erase_all_query_caches()
    :ok
  end

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
        assert_chaos_query(query_name, Query.fetch!({:test, :chaos, query_name}))
      end)
    end

    test "raises a warning if the same .sql file is compiled multiple times" do
      Query.compile(@chaos_path, {:test, :chaos})

      log =
        capture_log(fn ->
          Query.compile(@chaos_path, {:test, :chaos})
        end)

      assert log =~ "[warning] Recompiling queries for the \"chaos\" domain"
    end
  end

  describe "get_templated_query_id/3" do
    test ":__all" do
      Query.compile(@friends_path, {:test, :friends})

      # Ensures the :__fetch is compiled (due to it being an "ad-hoc" query)
      query_id = {:test, :friends, :__all}
      Query.get_templated_query_id(query_id, [:*])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :select
      assert target_fields == [:*]
      assert bindings == []
      assert sql == "SELECT * FROM friends;"
    end

    test ":__all - with custom target fields" do
      Query.compile(@friends_path, {:test, :friends})

      query_id = Query.get_templated_query_id({:test, :friends, :__all}, [:name])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :select
      assert target_fields == [:name]
      assert bindings == []
      assert sql == "SELECT name FROM friends;"
    end

    test ":__fetch" do
      Query.compile(@friends_path, {:test, :friends})

      query_id = {:test, :friends, :__fetch}
      Query.get_templated_query_id(query_id, [:*])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :select
      assert target_fields == [:*]
      assert bindings == [:id]
      assert sql == "SELECT * FROM friends WHERE id = ?;"
    end

    test ":__fetch  - with composite PK" do
      Query.compile(@order_items_path, {:test, :order_items})

      query_id = {:test, :order_items, :__fetch}
      Query.get_templated_query_id(query_id, [:*])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :select
      assert target_fields == [:*]
      assert bindings == [:order_id, :product_id]
      assert sql == "SELECT * FROM order_items WHERE order_id = ? AND product_id = ?;"
    end

    test ":__fetch - with custom target fields" do
      Query.compile(@order_items_path, {:test, :order_items})

      query_id = Query.get_templated_query_id({:test, :order_items, :__fetch}, [:quantity, :price])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert sql == "SELECT quantity, price FROM order_items WHERE order_id = ? AND product_id = ?;"
      assert target_fields == [:quantity, :price]
      assert bindings == [:order_id, :product_id]
      assert query_type == :select
    end

    test ":__fetch - raises when invalid fields are selected" do
      Query.compile(@order_items_path, {:test, :order_items})

      %{message: error} =
        assert_raise RuntimeError, fn ->
          Query.get_templated_query_id({:test, :order_items, :__fetch}, [:i_dont_exist])
        end

      assert error =~ "Can't select :i_dont_exist; not a valid field for Elixir.Sample.OrderItems"
    end

    test ":__fetch - raises when schema has no PK" do
      Query.compile(@all_types_path, {:test, :all_types})

      query_id = {:test, :all_types, :__fetch}

      %{message: error} =
        assert_raise RuntimeError, fn ->
          Query.get_templated_query_id(query_id, [:*])
        end

      assert error =~ "Can't generate adhoc query"
      assert error =~ ":__fetch"
      assert error =~ "because Sample.AllTypes has no PKs"
    end

    test ":__insert - targeting :all fields" do
      Query.compile(@friends_path, {:test, :friends})

      query_id = {:test, :friends, :__insert}
      Query.get_templated_query_id(query_id, [:*])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :insert
      assert target_fields == []
      assert bindings == [:id, :name, :sibling_count]
      assert sql == "INSERT INTO friends ( id, name, sibling_count ) VALUES ( ?, ?, ? );"
    end

    test ":__update" do
      Query.compile(@friends_path, {:test, :friends})

      query_id = Query.get_templated_query_id({:test, :friends, :__update}, [:name])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :update
      assert target_fields == []
      assert bindings == [:name, :id]
      assert sql == "UPDATE friends SET name = ? WHERE id = ?;"
    end

    test ":__update - multiple fields updated at once" do
      Query.compile(@friends_path, {:test, :friends})

      query_id =
        Query.get_templated_query_id({:test, :friends, :__update}, [:name, :sibling_count])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :update
      assert target_fields == []
      assert bindings == [:name, :sibling_count, :id]
      assert sql == "UPDATE friends SET name = ?, sibling_count = ? WHERE id = ?;"

      # Target fields are sorted, so the generated query_id is always the same
      assert query_id ==
               Query.get_templated_query_id({:test, :friends, :__update}, [:sibling_count, :name])
    end

    test ":__update  - with composite PK" do
      Query.compile(@order_items_path, {:test, :order_items})

      query_id = Query.get_templated_query_id({:test, :order_items, :__update}, [:quantity])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :update
      assert target_fields == []
      assert bindings == [:quantity, :order_id, :product_id]
      assert sql == "UPDATE order_items SET quantity = ? WHERE order_id = ? AND product_id = ?;"
    end

    test ":__update - raises when schema has no PK" do
      Query.compile(@all_types_path, {:test, :all_types})

      query_id = {:test, :all_types, :__update}

      %{message: error} =
        assert_raise RuntimeError, fn ->
          Query.get_templated_query_id(query_id, [:boolean])
        end

      assert error =~ "Can't generate adhoc query"
      assert error =~ ":\"__update$boolean\""
      assert error =~ "because Sample.AllTypes has no PKs"
    end

    test ":__delete" do
      Query.compile(@friends_path, {:test, :friends})

      query_id = {:test, :friends, :__delete}
      Query.get_templated_query_id(query_id, [])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :delete
      assert target_fields == []
      assert bindings == [:id]
      assert sql == "DELETE FROM friends WHERE id = ?;"
    end

    test ":__delete - with composite PK" do
      Query.compile(@order_items_path, {:test, :order_items})

      query_id = {:test, :order_items, :__delete}
      Query.get_templated_query_id(query_id, [])

      assert {sql, {target_fields, bindings}, query_type} = Query.fetch!(query_id)
      assert query_type == :delete
      assert target_fields == []
      assert bindings == [:order_id, :product_id]
      assert sql == "DELETE FROM order_items WHERE order_id = ? AND product_id = ?;"
    end

    test ":__delete - raises when schema has no PK" do
      Query.compile(@all_types_path, {:test, :all_types})

      query_id = {:test, :all_types, :__delete}

      %{message: error} =
        assert_raise RuntimeError, fn ->
          Query.get_templated_query_id(query_id, [])
        end

      assert error =~ "Can't generate adhoc query"
      assert error =~ ":__delete"
      assert error =~ "because Sample.AllTypes has no PKs"
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

    assert sql == "insert into accounts ( id, username, email ) values ( ?, ?, ? );"
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

  defp erase_all_query_caches do
    erase_query_cache({:test, :friends})
    erase_query_cache({:test, :chaos})
    erase_query_cache({:test, :order_items})
    erase_query_cache({:test, :all_types})
  end

  defp erase_query_cache({context, domain}),
    do: :persistent_term.erase({:db_sql_queries, {context, domain}})
end
