defmodule Sample.Friend do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :test
  @table :friends

  @schema [
    {:id, :integer},
    {:name, :string},
    {:divorce_count, {:integer, virtual: :get_divorce_count}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def get_divorce_count(%{name: name}) do
    case name do
      "Ross" ->
        3

      # Did Phoebe get a second divorce in Las Vegas???
      "Phoebe" ->
        1

      "Rachel" ->
        1

      _ ->
        0
    end
  end
end
