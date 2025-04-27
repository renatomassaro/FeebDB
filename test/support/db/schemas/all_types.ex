defmodule Sample.AllTypes do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :test
  @table :all_types

  @primary_keys nil

  @schema [
    {:boolean, :boolean},
    {:boolean_nullable, {:boolean, nullable: true}},
    {:string, :string},
    {:string_nullable, {:string, nullable: true}},
    {:integer, :integer},
    {:integer_nullable, {:integer, nullable: true}},
    {:atom, :atom},
    {:atom_nullable, {:atom, nullable: true}},
    {:uuid, :uuid},
    {:uuid_nullable, {:uuid, nullable: true}},
    {:datetime_utc, :datetime_utc},
    {:datetime_utc_nullable, {:datetime_utc, nullable: true}},
    {:datetime_utc_precision_second, {:datetime_utc, precision: :second, nullable: true}},
    {:datetime_utc_precision_millisecond, {:datetime_utc, precision: :millisecond, nullable: true}},
    {:datetime_utc_precision_microsecond, {:datetime_utc, precision: :microsecond, nullable: true}},
    {:datetime_utc_precision_default, {:datetime_utc, nullable: true}},
    {:map, :map},
    {:map_load_structs, {:map, load_structs: true, nullable: true}},
    {:map_nullable, {:map, nullable: true}},
    {:map_keys_atom, {:map, keys: :atom, nullable: true}},
    {:map_keys_safe_atom, {:map, keys: :safe_atom, nullable: true}},
    {:map_keys_string, {:map, keys: :string, nullable: true}},
    {:map_keys_default, {:map, nullable: true}},
    {:list, :list},
    {:list_nullable, {:list, nullable: true}},
    {:enum, {:enum, values: [:one, :two, :three]}},
    {:enum_nullable, {:enum, values: ["foo", "bar", "baz"], nullable: true}},
    {:enum_safe_atom, {:enum, values: [:safe, :atom], format: :safe_atom, nullable: true}},
    {:virtual, {:integer, virtual: true}},
    {:virtual_with_after_read, {:integer, virtual: true, after_read: :load_virtual_integer}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def creation_params(overwrites \\ %{}) do
    %{
      boolean: true,
      string: "Some Value",
      integer: 42,
      atom: :pizza,
      uuid: "feebfeeb-feeb-feeb-feeb-feebfeebfeeb",
      datetime_utc: DateTime.utc_now(),
      map: %{foo: "bar"},
      list: [1, 2, 3],
      enum: :one
    }
    |> Map.merge(overwrites)
  end

  def load_virtual_integer(_field, row, _repo_config), do: row.integer
end
