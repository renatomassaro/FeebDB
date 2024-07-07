# NOTE: No longer used, but I'm keeping it here for future reference.
defmodule Test.Finders do
  @moduledoc """
  Experimental. The idea is to provide a bunch of `find_*` which can make tests
  less dense.
  """

  alias Feeb.DB, as: DB

  def find_archived_event_with_handler(handler_fun) when is_atom(handler_fun) do
    Core.Event.Archive
    |> DB.all()
    |> Enum.find(&(&1.handler_fun == handler_fun))
  end
end
