defmodule Feeb.DB.Type.Map do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger
  alias Utils.JSON

  def sqlite_type, do: :text

  @doc """
  When casting, we need to guarantee that the output follows the `keys` specified in the column
  opts. For example, if a field has `keys: :atom` and receives %{"foo" => :bar} as value, we need to
  cast it to %{foo: :bar}.
  """
  def cast!(v, opts, _) when is_map(v) do
    cond do
      opts[:keys] == :atom -> Utils.Map.atomify_keys(v)
      opts[:keys] == :safe_atom -> Utils.Map.safe_atomify_keys(v)
      true -> Utils.Map.stringify_keys(v)
    end
  end

  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(v, _, _) when is_map(v), do: JSON.encode!(v)
  def dump!(nil, _, _), do: nil

  def load!(v, opts, _) when is_binary(v) do
    cond do
      opts[:keys] == :atom -> v |> JSON.decode!() |> Utils.Map.atomify_keys()
      opts[:keys] == :safe_atom -> v |> JSON.decode!() |> Utils.Map.safe_atomify_keys()
      true -> JSON.decode!(v)
    end
    |> load_structs(opts)
  end

  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end

  defp load_structs(map, %{load_structs: false}), do: map
  defp load_structs(map, _), do: Utils.Map.load_structs(map)
end
