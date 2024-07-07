defmodule Feeb.DB.Type do
  # def get_module(:datetime), do: __MODULE__.Datetime
  def get_module(:boolean), do: __MODULE__.Boolean
  def get_module(:string), do: __MODULE__.String
  def get_module(:datetime_utc), do: __MODULE__.DateTimeUTC
  def get_module(:integer), do: __MODULE__.Integer
  def get_module(:uuid), do: __MODULE__.UUID
  def get_module(:atom), do: __MODULE__.Atom
  def get_module(:map), do: __MODULE__.Map
  def get_module(:list), do: __MODULE__.List
end

defmodule Feeb.DB.Type.Boolean do
  def sqlite_type, do: :integer
  def cast!(v, _) when is_boolean(v), do: v
  def dump!(true, _), do: 1
  def dump!(false, _), do: 0
  def load!(v, _) when is_boolean(v), do: v
  def load!(1, _), do: true
  def load!(0, _), do: false
end

defmodule Feeb.DB.Type.String do
  def sqlite_type, do: :text
  def cast!(v, _) when is_binary(v), do: v
  def dump!(v, _) when is_binary(v), do: v
  def load!(v, _) when is_binary(v), do: v
  # TODO: Support nullable opt and warn if casting `nil` on non-nullable field
  # def load!(:undefined, _), do: nil
end

defmodule Feeb.DB.Type.Atom do
  def sqlite_type, do: :text
  def cast!(nil, %{nullable: true}), do: nil
  def cast!(v, _) when is_atom(v) and not (is_nil(v) or is_boolean(v)), do: v
  def cast!(v, _) when is_binary(v), do: String.to_atom(v)

  def dump!(v, _) when is_atom(v), do: "#{v}"

  def load!(v, _) when is_binary(v), do: String.to_atom(v)
  def load!(nil, %{nullable: true}), do: nil
end

defmodule Feeb.DB.Type.DateTimeUTC do
  def sqlite_type, do: :text

  def cast!(nil, %{nullable: true}),
    do: nil

  def cast!(%DateTime{} = dt, %{precision: precision}),
    do: DateTime.truncate(dt, precision)

  def cast!(%DateTime{} = dt, _), do: dt

  def dump!(nil, %{nullable: true}), do: nil
  def dump!(%DateTime{} = dt, _), do: DateTime.to_string(dt)

  # NOTE: We used to need a FastParse here (see old HE code 09/2021), but
  # apparently this is no longer necessary for recent Elixir versions.,
  def load!(nil, %{nullable: true}), do: nil
  def load!(v, _) when is_binary(v), do: DateTime.from_iso8601(v) |> elem(1)
end

defmodule Feeb.DB.Type.Integer do
  def sqlite_type, do: :integer
  def cast!(v, _) when is_integer(v), do: v
  def dump!(v, _) when is_integer(v), do: v
  def load!(v, _) when is_integer(v), do: v
end

defmodule Feeb.DB.Type.Map do
  def sqlite_type, do: :text

  def cast!(v, o) when is_map(v), do: v |> dump!(o) |> load!(o)
  def cast!(nil, %{nullable: true}), do: nil

  # TODO: Use native decoder now :)
  # def dump!(v, _) when is_map(v), do: Jason.encode!(v)
  def dump!(nil, _), do: nil

  # TODO: Use native decoder now :)
  # def load!(v, %{keys: :string}) when is_binary(v), do: Jason.decode!(v)
  # def load!(v, %{keys: :atom}) when is_binary(v), do: Jason.decode!(v, keys: :atoms)
  # def load!(v, _) when is_binary(v), do: Jason.decode!(v, keys: :atoms)
  def load!(nil, %{nullable: true}), do: nil
end

defmodule Feeb.DB.Type.List do
  def sqlite_type, do: :text

  def cast!(v, o) when is_list(v), do: v |> dump!(o) |> load!(o)
  def cast!(nil, %{default: v}), do: v
  def cast!(nil, %{nullable: true}), do: nil

  # TODO: Use native decoder now :)
  # def dump!(v, _) when is_list(v), do: Jason.encode!(v)
  # def dump!(nil, %{default: v}) when is_list(v), do: Jason.encode!(v)
  def dump!(nil, %{default: nil}), do: nil
  def dump!(nil, %{nullable: true}), do: nil

  # TODO: Use native decoder now :)
  # def load!(v, _) when is_binary(v), do: Jason.decode!(v)
  def load!(nil, %{default: v}), do: v
  def load!(nil, %{nullable: true}), do: nil
end

defmodule Feeb.DB.Type.UUID do
  def sqlite_type, do: :text

  def cast!(v, _) when is_binary(v) do
    if not Utils.UUID.is_valid?(v), do: raise("Invalid UUID value: #{v}")
    v
  end

  def cast!(nil, _), do: nil

  def dump!(v, _) when is_binary(v), do: v
  def dump!(nil, _), do: nil

  def load!(v, _) when is_binary(v), do: v
  def load!(nil, _), do: nil
end
