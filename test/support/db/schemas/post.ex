defmodule Sample.Post do
  use Feeb.DB.Schema
  alias Feeb.DB.Schema

  @context :test
  @table :posts

  @schema [
    {:id, :integer},
    {:title, :string},
    {:body, :string},
    {:is_draft, {:boolean, nullable: true}},
    {:inserted_at, {:datetime_utc, [precision: :millisecond], mod: :inserted_at}},
    {:updated_at, {:datetime_utc, [precision: :millisecond], mod: :updated_at}}
  ]

  def new(params) do
    params
    |> Schema.cast(:all)
    |> Schema.create()
  end

  def change_title(%_{} = post, new_title) do
    %{title: new_title}
    |> Schema.update(post)
  end
end
