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
      {k, v}, acc ->
        {new_k, new_v} =
          if k == :__struct__ or k == "__struct__" do
            {:__struct__, String.to_existing_atom(v)}
          else
            {k, load_structs(v)}
          end

        Map.put(acc, new_k, new_v)
    end)
  end

  def load_structs(v), do: v
end
