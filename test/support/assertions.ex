# NOTE: No longer used, but I'm keeping it here for future reference
defmodule Test.Assertions do
  defmacro assert_event_completed() do
    quote do
      assert_db_delete("events")
      assert_db_insert("events_archive")
    end
  end

  defmacro assert_event_completed(event_id) do
    quote do
      assert_db_delete("events", unquote(event_id))
      assert_db_insert("events_archive", unquote(event_id))
    end
  end

  defmacro assert_db_insert(table) do
    quote do
      assert_receive({:insert, "main", unquote(table), _})
    end
  end

  defmacro assert_db_insert(table, expected_id) do
    quote do
      assert_receive({:insert, "main", unquote(table), actual_id})
      assert actual_id == unquote(expected_id)
    end
  end

  defmacro assert_db_update(table) do
    quote do
      assert_receive({:update, "main", unquote(table), _})
    end
  end

  defmacro assert_db_update(table, expected_id) do
    quote do
      assert_receive({:update, "main", unquote(table), actual_id})
      assert actual_id == unquote(expected_id)
    end
  end

  defmacro assert_db_delete(table) do
    quote do
      assert_receive({:delete, "main", unquote(table), _})
    end
  end

  defmacro assert_db_delete(table, expected_id) do
    quote do
      assert_receive({:delete, "main", unquote(table), actual_id})
      assert actual_id == unquote(expected_id)
    end
  end
end
