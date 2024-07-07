defmodule Feeb.DB.Query.DynamicTest do
  use ExUnit.Case, async: true

  alias Feeb.DB.Query.Dynamic

  describe "build/5" do
    test "builds expected query" do
      assoc_map = %{
        crm_contacts: "c",
        crm_contact_tags: {"ct", :id, :contact_id},
        crm_contact_visits: {"cv", :id, :contact_id}
      }

      filter_map = [
        crm_contacts: [
          :noop,
          {:full_name, {:likep, "Ren"}},
          {:first_name, {:eq, "Renato"}}
        ],
        crm_contact_tags: [
          :noop,
          {:tag_id, {:eq, "1"}},
          {:tag_id, {:in, [99, 100, 101]}}
        ]
      ]

      sort_map = [
        crm_contacts: [
          {:updated_at, :desc}
        ],
        crm_contact_tags: [
          {:tag_name, :asc}
        ]
      ]

      pagination_map = %{
        limit: 50,
        offset: 0
      }

      search_input = %{
        filter: filter_map,
        page: pagination_map,
        sort: sort_map,
        opts: []
      }

      schema_mock = %{__table__: :crm_contacts}

      assert {query, bindings} = Dynamic.build(schema_mock, assoc_map, search_input)

      expected_query =
        "SELECT c.* FROM crm_contacts c " <>
          "JOIN crm_contact_tags ct ON c.id = ct.contact_id " <>
          "WHERE c.full_name LIKE ? || '%' AND c.first_name = ? " <>
          "AND ct.tag_id = ? AND ct.tag_id IN (?, ?, ?) " <>
          "ORDER BY c.updated_at DESC, ct.tag_name ASC " <> "LIMIT 50 OFFSET 0"

      assert query == expected_query
      assert bindings == ["Ren", "Renato", "1", 99, 100, 101]
    end
  end
end
