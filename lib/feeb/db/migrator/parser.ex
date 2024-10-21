defmodule Feeb.DB.Migrator.Parser do
  @spec queries_from_sql_lines(String.t()) ::
          [parsed_query :: String.t()]
  def queries_from_sql_lines(lines) when is_binary(lines) do
    lines
    |> String.split("\n")
    |> Enum.reverse()
    |> Enum.reduce([], fn line, acc ->
      case filter_out_comments_from_line(line) do
        "" -> acc
        line -> [line | acc]
      end
    end)
    |> Enum.join(" ")
    |> String.split(";")
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&String.trim/1)
    |> Enum.map(&remove_redundant_spaces/1)
  end

  defp filter_out_comments_from_line(line) do
    if Regex.match?(~r/^s*--/, line) do
      ""
    else
      case String.split(line, "--") do
        [line] ->
          line

        [line | _commented_out_part_of_the_line] ->
          line
      end
    end
  end

  defp remove_redundant_spaces(line) do
    Regex.replace(~r/\s+/, line, " ")
  end
end
