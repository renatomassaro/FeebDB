defmodule Utils.UUID do
  @doc """
  Validates that the UUID is somewhat correct. By no means it's supposed to validate according to
  the specifications. Just make sure it has 36 hexadecimal characters + dashes, with the usual
  format one would expect in an UUID.
  """
  def is_valid?(uuid) do
    with true <- 36 == String.length(uuid) do
      case String.split(uuid, "-") do
        [a, b, c, d, e] ->
          with true <- 8 == String.length(a),
               true <- 4 == String.length(b),
               true <- 4 == String.length(c),
               true <- 4 == String.length(d),
               true <- 12 == String.length(e),
               true <- Regex.match?(~r/^[a-fA-F0-9-]+$/, uuid) do
            true
          end

        _ ->
          false
      end
    end
  end

  # TODO: Replace with Renatils
  @doc """
  Just a stupid simple UUID generator.
  """
  def random do
    :crypto.strong_rand_bytes(16)
    |> :binary.encode_hex()
    |> format_uuid()
    |> String.downcase()
  end

  defp format_uuid(hex) do
    part1 = String.slice(hex, 0, 8)
    part2 = String.slice(hex, 8, 4)
    part3 = String.slice(hex, 12, 4)
    part4 = String.slice(hex, 16, 4)
    part5 = String.slice(hex, 20, 12)
    "#{part1}-#{part2}-#{part3}-#{part4}-#{part5}"
  end
end
