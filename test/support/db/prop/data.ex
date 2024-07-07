defmodule Test.Feeb.DB.Prop.Data do
  alias Feeb.DB.{SQLite}

  def generate!(db, c) do
    Process.put(:conn, c)
    do_generate(db)
  end

  defp do_generate(:test) do
    friends_data()
  end

  defp do_generate(_), do: :noop

  defp friends_data do
    [
      {1, "Phoebe"},
      {2, "Joey"},
      {3, "Chandler"},
      {4, "Monica"},
      {5, "Ross"},
      {6, "Rachel"}
    ]
    |> Enum.each(fn {id, name} ->
      "INSERT INTO friends (id, name) VALUES (#{id}, '#{name}')"
      |> run()
    end)
  end

  defp run(sql) do
    c = Process.get(:conn)
    SQLite.raw!(c, sql)
  end
end
