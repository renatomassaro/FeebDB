defmodule Feeb.DB.Value.NotLoaded do
  defstruct []
  defimpl(Inspect, do: def(inspect(_, _), do: "#NotLoaded<>"))
end
