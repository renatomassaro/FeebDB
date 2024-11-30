defmodule Sample.Friend do
  use Feeb.DB.Schema
  alias Feeb.DB
  alias Feeb.DB.Schema

  @context :test
  @table :friends

  @schema [
    {:id, :integer},
    {:name, :string},
    {:divorce_count, {:integer, virtual: :get_divorce_count}},
    {:repo_config, {:map, virtual: :get_repo_config}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def get_repo_config(_row, %DB.Repo.RepoConfig{} = repo_config),
    do: repo_config

  def get_divorce_count(%{name: name}, _) do
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
