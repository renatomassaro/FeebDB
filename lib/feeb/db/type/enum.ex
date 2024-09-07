defmodule Feeb.DB.Type.Enum do
  @behaviour Feeb.DB.Type.Behaviour

  require Logger

  def sqlite_type, do: :text

  @doc """
  If a valid `format` is specified, keep as is. However, if one was not specified by the user, infer
  it based on the contents of the `values` entry. We assume all entries within `values` will share
  the same type (and we will crash otherwise).
  """
  def overwrite_opts(%{format: format} = opts, _, _) do
    true = format in [:atom, :safe_atom, :string]
    opts
  end

  def overwrite_opts(%{values: values} = opts, _, identifier) do
    value_types =
      values
      |> Enum.map(fn value ->
        cond do
          is_atom(value) -> :atom
          is_binary(value) -> :string
        end
      end)
      |> Enum.group_by(fn x -> x end)

    if map_size(value_types) > 1,
      do: raise("Multiple types in enum at #{inspect(identifier)}: #{inspect(value_types)}")

    [format] = Map.keys(value_types)
    Map.put(opts, :format, format)
  end

  def cast!(nil, %{nullable: true}, _), do: nil

  def cast!(nil, _, {mod, field}),
    do: raise("Attempted to cast `nil` value at non-nullable field #{field}@#{mod}")

  def cast!(value, %{format: :atom} = o, m) when is_binary(value),
    do: cast!(String.to_atom(value), o, m)

  def cast!(value, %{format: :safe_atom} = o, m) when is_binary(value),
    do: cast!(String.to_existing_atom(value), o, m)

  def cast!(value, %{format: :string} = o, m) when is_atom(value),
    do: cast!("#{value}", o, m)

  def cast!(v, %{values: values}, {mod, field}) do
    if v in values do
      v
    else
      raise "Value #{v} is invalid for enum at #{field}@#{mod}. Accepted values: #{inspect(values)}"
    end
  end

  def dump!(nil, %{nullable: true}, _), do: nil
  def dump!(v, _, _) when is_binary(v), do: v
  def dump!(v, _, _) when is_atom(v), do: "#{v}"

  def load!(nil, %{nullable: true}, _), do: nil

  def load!(nil, _, {mod, field}) do
    Logger.warning("Loaded `nil` value from non-nullable field: #{field}@#{mod}")
    nil
  end

  def load!(v, %{format: :atom, values: values}, m),
    do: verify_and_load(String.to_atom(v), values, m)

  def load!(v, %{format: :safe_atom, values: values}, m),
    do: verify_and_load(String.to_existing_atom(v), values, m)

  def load!(v, %{format: :string, values: values}, m), do: verify_and_load(v, values, m)

  defp verify_and_load(v, values, {mod, field}) do
    if v in values do
      v
    else
      "Loaded value #{v} that is not part of enum values #{inspect(values)}) for #{field}@#{mod}"
      |> raise()
    end
  end
end
