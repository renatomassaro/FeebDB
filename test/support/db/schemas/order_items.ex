defmodule Sample.OrderItems do
  use Feeb.DB.Schema

  @context :test
  @table :order_items

  @primary_keys [:order_id, :product_id]

  @schema [
    {:order_id, :integer},
    {:product_id, :integer},
    {:quantity, :integer},
    {:price, :integer},
    {:inserted_at, {:datetime_utc, [], mod: :inserted_at}},
    {:updated_at, {:datetime_utc, [], mod: :updated_at}}
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
end
