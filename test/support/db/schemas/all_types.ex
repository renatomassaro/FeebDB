defmodule Sample.AllTypes do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :test
  @table :all_types

  @schema [
    {:boolean, :boolean},
    {:boolean_nullable, {:boolean, nullable: true}},
    {:string, :string},
    {:string_nullable, {:string, nullable: true}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def creation_params(overwrites \\ %{}) do
    %{
      boolean: true,
      string: "Some Value"
    }
    |> Map.merge(overwrites)
  end
end
