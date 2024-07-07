defmodule Feeb.DB.Mod.InsertedAt do
  def on_create(_, _, %{creation_time: creation_time}), do: {:ok, creation_time}
  def on_update(_, _, _), do: :noop
end

defmodule Feeb.DB.Mod.UpdatedAt do
  def on_create(_, _, %{creation_time: creation_time}), do: {:ok, creation_time}
  def on_update(_, _, _opts), do: {:ok, DateTime.utc_now()}
end

defmodule Feeb.DB.Mod.AutoIncrement do
  # TODO: Ver como implementaria esse pra definir melhor a API dos hooks
  # Talvez eu tenha que ter:
  # on_pre_create
  # ?on_post_create?
  # on_pre_insert
  # on_post_insert
  # on_pre_change
  # ?on_post_change?
  # on_pre_update
  # on_post_update
end

defmodule Feeb.DB.Mod do
  def get_module(nil), do: nil
  def get_module(:id), do: ID.Mod
  def get_module(:inserted_at), do: Feeb.DB.Mod.InsertedAt
  def get_module(:updated_at), do: Feeb.DB.Mod.UpdatedAt
end
