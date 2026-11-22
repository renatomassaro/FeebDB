defmodule Sample.User do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :lobby
  @table :users

  @schema [
    {:id, :integer},
    {:external_id, :string},
    {:username, :string},
    {:email, :string},
    {:password, :string},
    {:inserted_at, :string}
  ]

  # Assigned by SQLite: the hand-written `:__insert` (`priv/test/queries/lobby/users.sql`) leaves it
  # out and gets it back with `RETURNING *`
  @derived_fields [:id]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end
end
