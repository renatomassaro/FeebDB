defmodule Sample.Friend do
  use Feeb.DB.Schema
  alias Feeb.DB
  alias Feeb.DB.Schema

  @context :test
  @table :friends

  @schema [
    {:id, :integer},
    {:name, :string},
    {:divorce_count, {:integer, virtual: true, after_read: :get_divorce_count}},
    {:sibling_count, {:integer, nullable: true, after_read: :get_sibling_count}},
    {:repo_config, {:map, virtual: true, after_read: :get_repo_config}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def update(%_{} = row, changes) do
    row
    |> Schema.update(changes)
  end

  def get_repo_config(_field, _row, %DB.Repo.RepoConfig{} = repo_config),
    do: repo_config

  def get_divorce_count(_, %{name: name}, _) do
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

  def get_sibling_count(_, %{name: name}, _) do
    case name do
      "Joey" ->
        7

      "Rachel" ->
        2

      _ ->
        1
    end
  end
end
