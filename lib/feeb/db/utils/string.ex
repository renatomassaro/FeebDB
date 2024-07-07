defmodule Utils.String do
  # DOCME
  def count(str, needle) do
    # TODO: Benchmark against alternative `String.graphemes` implementation
    str
    |> String.split(needle)
    |> length()
    |> Kernel.-(1)
  end
end
