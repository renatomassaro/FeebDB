defmodule Feeb.DB.Query.Dynamic.BuilderTest do
  use ExUnit.Case, async: true

  alias Feeb.DB.Query.Dynamic.Builder, as: B

  @assoc_map %{
    imoveis: "i",
    imovel_tags: {"it", :id, :imovel_id},
    imovel_attrs: {"ia", :id, :imovel_id}
  }

  describe "build_select/2" do
    test "builds regular select" do
      assert "SELECT i.*" == B.build_select("i", [])
    end

    test "builds COUNT select" do
      assert "SELECT COUNT(*)" == B.build_select("i", count: true)
    end
  end

  describe "build_wheres/2" do
    test "with one where and one assoc" do
      filter_map = [imoveis: [{:address, {:eq, "Rua um"}}]]
      assert {q, t, w, b} = B.build_wheres(@assoc_map, filter_map)
      assert "WHERE i.address = ?" == q
      assert [:imoveis] == t
      assert ["i.address = ?"] == w
      assert ["Rua um"] == b
    end

    test "with two wheres and one assoc" do
      filter_map = [
        imoveis: [{:address, {:eq, "Rua um"}}, {:valor, {:gte, 1000}}]
      ]

      assert {q, _, w, b} = B.build_wheres(@assoc_map, filter_map)
      assert "WHERE i.address = ? AND i.valor >= ?" == q
      assert ["i.address = ?", "i.valor >= ?"] == w
      assert ["Rua um", 1000] == b
    end

    test "with two assocs" do
      filter_map = [
        imoveis: [{:address, {:eq, "Rua um"}}, {:valor, {:gte, 1000}}],
        imovel_tags: [{:tag_id, {:in, [1, 2, 3]}}]
      ]

      assert {q, t, w, b} = B.build_wheres(@assoc_map, filter_map)

      assert "WHERE i.address = ? AND i.valor >= ? AND it.tag_id IN (?, ?, ?)" ==
               q

      assert Enum.sort([:imoveis, :imovel_tags]) == Enum.sort(t)
      assert ["i.address = ?", "i.valor >= ?", "it.tag_id IN (?, ?, ?)"] == w
      assert ["Rua um", 1000, 1, 2, 3] == b
    end

    test "filters out noops" do
      filter_map = [
        imoveis: [:noop, :noop, {:address, {:eq, "Rua um"}}, :noop],
        imovel_tags: [{:tag_id, {:eq, 1}}, :noop, :noop, :noop],
        imovel_attrs: [:noop, :noop, :noop]
      ]

      assert {q, t, w, b} = B.build_wheres(@assoc_map, filter_map)
      assert "WHERE i.address = ? AND it.tag_id = ?" == q
      assert Enum.sort([:imoveis, :imovel_tags]) == Enum.sort(t)
      assert ["i.address = ?", "it.tag_id = ?"] == w
      assert ["Rua um", 1] == b
    end

    test "preserves input order" do
      filter_map = [
        imoveis: [{:address, {:eq, 1}}, {:valor, {:eq, 2}}, {:city, {:eq, 3}}],
        imovel_attrs: [
          {:num_quartos, {:eq, 4}},
          {:num_vagas, {:in, [5, 6, 7, 8]}}
        ],
        imovel_tags: [
          {:tag_id, {:eq, 9}},
          {:tag_id, {:eq, 10}},
          {:tag_id, {:eq, 11}}
        ]
      ]

      assert {_, _, w, b} = B.build_wheres(@assoc_map, filter_map)

      assert [
               "i.address = ?",
               "i.valor = ?",
               "i.city = ?",
               "ia.num_quartos = ?",
               "ia.num_vagas IN (?, ?, ?, ?)",
               "it.tag_id = ?",
               "it.tag_id = ?",
               "it.tag_id = ?"
             ] == w

      assert [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] == b
    end

    test "returns empty string when no filters are set" do
      assert {"", [], [], []} == B.build_wheres(@assoc_map, %{})
    end
  end

  describe "build_joins/3" do
    test "with no assocs" do
      assoc_map = @assoc_map |> Map.drop([:imovel_tags, :imovel_attrs])

      assert "FROM imoveis i" ==
               B.build_joins(:imoveis, "i", assoc_map, Map.keys(assoc_map))
    end

    test "with two assocs" do
      assoc_map = @assoc_map |> Map.drop([:imovel_tags])
      wheres = Map.keys(assoc_map)

      assert "FROM imoveis i JOIN imovel_attrs ia ON i.id = ia.imovel_id" ==
               B.build_joins(:imoveis, "i", assoc_map, wheres)
    end

    test "with three assocs" do
      q =
        "FROM imoveis i JOIN imovel_tags it ON i.id = it.imovel_id " <>
          "JOIN imovel_attrs ia ON i.id = ia.imovel_id"

      assert q == B.build_joins(:imoveis, "i", @assoc_map, Map.keys(@assoc_map))
    end
  end

  describe "build_sorts/2" do
    test "builds the expected clause" do
      sort_map = [
        imoveis: [{:updated_at, :desc}],
        imovel_tags: [{:id, :desc}, {:name, :asc}],
        imovel_attrs: [{:beds, :desc}, {:baths, :asc}, {:rooms, :desc}]
      ]

      expected_q =
        "ORDER BY i.updated_at DESC, it.id DESC, it.name ASC, " <>
          "ia.beds DESC, ia.baths ASC, ia.rooms DESC"

      assert expected_q == B.build_sorts(@assoc_map, sort_map)
    end

    test "returns empty string when no sort_map is provided" do
      assert "" == B.build_sorts(@assoc_map, [])
    end
  end

  describe "build_limits/1" do
    test "builds limit and offset" do
      assert "LIMIT 10 OFFSET 0" == B.build_limits(%{limit: 10, offset: 0})
      assert "LIMIT 100 OFFSET 50" == B.build_limits(%{limit: 100, offset: 50})
    end
  end

  describe "where operators" do
    test "basic operators" do
      [
        {:eq, "x", "= ?"},
        {:gt, 1, "> ?"},
        {:gte, 1, ">= ?"},
        {:lt, 1, "< ?"},
        {:lte, 1, "<= ?"}
      ]
      |> Enum.each(fn {op, v, expected} ->
        filter_map = [imoveis: [{:id, {op, v}}]]
        assert {_, _, [w], [^v]} = B.build_wheres(@assoc_map, filter_map)
        assert "i.id #{expected}" == w
      end)
    end

    test "in - zero elements" do
      filter_map = [imoveis: [{:id, {:in, []}}]]
      assert {_, _, [], []} = B.build_wheres(@assoc_map, filter_map)
    end

    test "in - one elements" do
      filter_map = [imoveis: [{:id, {:in, [1]}}]]

      assert {_, _, ["i.id IN (?)"], [1]} = B.build_wheres(@assoc_map, filter_map)
    end

    test "in - multiple elements" do
      filter_map = [imoveis: [{:id, {:in, [1, 2]}}]]

      assert {_, _, ["i.id IN (?, ?)"], [1, 2]} = B.build_wheres(@assoc_map, filter_map)

      filter_map = [imoveis: [{:id, {:in, [1, 2, 3, 4, 5]}}]]

      assert {_, _, ["i.id IN (?, ?, ?, ?, ?)"], [1, 2, 3, 4, 5]} =
               B.build_wheres(@assoc_map, filter_map)
    end

    test "likep" do
      filter_map = [imoveis: [{:address, {:likep, "Rua"}}]]

      assert {_, _, ["i.address LIKE ? || '%'"], ["Rua"]} = B.build_wheres(@assoc_map, filter_map)
    end

    test "plikep" do
      filter_map = [imoveis: [{:address, {:plikep, "Rua"}}]]

      assert {_, _, ["i.address LIKE '%' || ? || '%'"], ["Rua"]} =
               B.build_wheres(@assoc_map, filter_map)
    end

    test "fragment - no args" do
      filter_map = [imoveis: [{:deleted_at, {:fragment, "IS NULL"}}]]

      assert {_, _, ["i.deleted_at IS NULL"], []} = B.build_wheres(@assoc_map, filter_map)
    end
  end
end
