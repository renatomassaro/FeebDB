defmodule Sample.Friend do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :test
  @table :friends

  @schema [
    {:id, :integer},
    {:name, :string}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end
end
