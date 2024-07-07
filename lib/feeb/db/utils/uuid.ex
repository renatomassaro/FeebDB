defmodule Utils.UUID do
  def is_valid?(uuid) do
    with true <- 36 == String.length(uuid) do
      case String.split(uuid, "-") do
        [a, b, c, d, e] ->
          with true <- 8 == String.length(a),
               true <- 4 == String.length(b),
               true <- 4 == String.length(c),
               true <- 4 == String.length(d),
               true <- 12 == String.length(e),
               true <- Regex.match?(~r/^[a-zA-Z0-9-]+$/, uuid) do
            true
          end

        _ ->
          false
      end
    end
  end
end
