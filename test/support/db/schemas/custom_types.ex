defmodule Sample.CustomTypes do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema
  alias Sample.Types.TypedID

  @context :test
  @table :custom_types

  @schema [
    {:typed_id, TypedID}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def creation_params(overwrites \\ %{}) do
    %{
      typed_id: 1
    }
    |> Map.merge(overwrites)
  end
end
