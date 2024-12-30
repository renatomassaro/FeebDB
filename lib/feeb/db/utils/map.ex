# TODO: This will come from Renatils library
defmodule Utils.Map do
  def atomify_keys(map, opts \\ [])

  def atomify_keys(struct, opts) when is_struct(struct) do
    case Keyword.get(opts, :on_struct, :keep) do
      :keep -> struct
      :convert -> Map.from_struct(struct)
    end
  end

  def atomify_keys(map, opts) when is_map(map) do
    atomify_fun =
      if Keyword.get(opts, :with_existing_atom, false),
        do: &String.to_existing_atom/1,
        else: &String.to_atom/1

    Enum.reduce(map, %{}, fn {k, v}, acc ->
      cond do
        is_atom(k) ->
          Map.put(acc, k, atomify_keys(v, opts))

        is_binary(k) ->
          Map.put(acc, atomify_fun.(k), atomify_keys(v, opts))

        true ->
          Map.put(acc, k, atomify_keys(v, opts))
      end
    end)
  end

  def atomify_keys(v, _), do: v

  # DOCME
  def safe_atomify_keys(map),
    do: atomify_keys(map, with_existing_atom: true)

  # DOCME
  def stringify_keys(struct) when is_struct(struct),
    do: struct |> Map.from_struct() |> stringify_keys()

  def stringify_keys(map) when is_map(map) do
    Enum.reduce(map, %{}, fn {k, v}, acc ->
      cond do
        is_atom(k) or is_number(k) ->
          Map.put(acc, "#{k}", stringify_keys(v))

        is_binary(k) ->
          Map.put(acc, k, stringify_keys(v))

        true ->
          Map.put(acc, k, stringify_keys(v))
      end
    end)
  end

  def stringify_keys(v), do: v

  def load_structs(map) when is_map(map) do
    Enum.reduce(map, %{}, fn
      {k, v}, acc when is_struct(v) ->
        Map.put(acc, k, v)

      {k, v}, acc when is_map(v) ->
        new_v =
          if Map.has_key?(v, :__struct__) or Map.has_key?(v, "__struct__") do
            convert_to_struct(v[:__struct__] || v["__struct__"], v)
          else
            load_structs(v)
          end

        Map.put(acc, k, new_v)

      {k, v}, acc ->
        Map.put(acc, k, load_structs(v))
    end)
    |> maybe_convert_top_level_struct()
  end

  def load_structs(v), do: v

  defp maybe_convert_top_level_struct(%{__struct__: struct_mod} = entries),
    do: convert_to_struct(struct_mod, entries)

  defp maybe_convert_top_level_struct(%{"__struct__" => struct_mod} = entries),
    do: convert_to_struct(struct_mod, entries)

  defp maybe_convert_top_level_struct(map),
    do: map

  defp convert_to_struct(raw_struct_mod, entries) when is_binary(raw_struct_mod),
    do: convert_to_struct(String.to_existing_atom(raw_struct_mod), safe_atomify_keys(entries))

  defp convert_to_struct(struct_mod, entries) when is_atom(struct_mod) do
    entries = Map.drop(entries, [:__struct__, "__struct__"])
    struct(struct_mod, load_structs(entries))
  end
end
