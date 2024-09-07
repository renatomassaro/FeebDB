defmodule Feeb.DB.Type do
  # def get_module(:datetime), do: __MODULE__.Datetime
  def get_module(:boolean), do: __MODULE__.Boolean
  def get_module(:string), do: __MODULE__.String
  def get_module(:datetime_utc), do: __MODULE__.DateTimeUTC
  def get_module(:integer), do: __MODULE__.Integer
  def get_module(:uuid), do: __MODULE__.UUID
  def get_module(:atom), do: __MODULE__.Atom
  def get_module(:map), do: __MODULE__.Map
  def get_module(:list), do: __MODULE__.List
  def get_module(:enum), do: __MODULE__.Enum
end
