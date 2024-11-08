defmodule Sample.Types.TypedID do
  @behaviour Feeb.DB.Type.Behaviour

  defstruct [:id]

  @impl true
  def sqlite_type, do: :integer

  @impl true
  def cast!(v, _, _) when is_integer(v), do: %__MODULE__{id: v}

  @impl true
  def dump!(%__MODULE__{id: v}, _, _), do: v

  @impl true
  def load!(v, _, _) when is_integer(v), do: %__MODULE__{id: v}

  defimpl String.Chars do
    def to_string(%{id: id}), do: "#{id}"
  end
end
