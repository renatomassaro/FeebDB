defmodule Feeb.DB.Type.Map do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger
  alias Utils.JSON

  @allowed_keys [:string, :atom, :safe_atom]

  def sqlite_type, do: :text

  @doc """
  Adds default `:keys` to the column `opts` if none is set. Default `:keys` value is `:atom`.
  """
  def overwrite_opts(%{keys: keys} = opts, _, _) when keys in @allowed_keys, do: opts
  def overwrite_opts(opts, _, _), do: Map.put(opts, :keys, :atom)

  @doc """
  When casting, we need to guarantee that the output follows the `keys` specified in the column
  opts. For example, if a field has `keys: :atom` and receives %{"foo" => :bar} as value, we need to
  cast it to %{foo: :bar}.

  We *must* convert every key based on the `:keys` information set in `opts`; we cannot keep the
  input as-is. Imagine someone passes an hybrid `%{a: :map, "with" => "strings"}` map with both
  atoms and strings as keys. It's impossible to know which keys were atom and which ones were string
  when we `load!/3` the data from disk, since this information is lost during `dump!/3`.
  """
  def cast!(v, opts, _) when is_map(v) do
    cond do
      opts[:keys] == :string -> Utils.Map.stringify_keys(v)
      opts[:keys] == :safe_atom -> Utils.Map.safe_atomify_keys(v)
      opts[:keys] == :atom -> Utils.Map.atomify_keys(v)
    end
  end

  def cast!(nil, %{nullable: true}, _), do: nil

  def dump!(v, _, _) when is_map(v), do: JSON.encode!(v)
  def dump!(nil, _, _), do: nil

  def load!(v, opts, _) when is_binary(v) do
    cond do
      opts[:keys] == :string -> JSON.decode!(v)
      opts[:keys] == :safe_atom -> v |> JSON.decode!() |> Utils.Map.safe_atomify_keys()
      opts[:keys] == :atom -> v |> JSON.decode!() |> Utils.Map.atomify_keys()
    end
    |> load_structs(opts)
  end

  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {schema, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{schema}")
    nil
  end

  defp load_structs(map, %{load_structs: true}), do: Utils.Map.load_structs(map)
  defp load_structs(map, _), do: map
end
