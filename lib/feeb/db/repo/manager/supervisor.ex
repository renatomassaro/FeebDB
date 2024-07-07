defmodule Feeb.DB.Repo.Manager.Supervisor do
  @moduledoc """
  This is a Dynamic Supervisor that acts as a parent for a Repo.Manager. Every {context, shard_id}
  tuple will have one (and only one) Repo.Manager, and its creation is coordinated by the Registry.
  """

  use DynamicSupervisor
  alias Feeb.DB.{Repo}

  def start_link(_) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def create(context, shard_id) do
    DynamicSupervisor.start_child(__MODULE__, {Repo.Manager, {context, shard_id}})
  end

  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
