defmodule Feeb.DB.Application do
  @moduledoc false
  use Application

  alias Feeb.DB.Repo

  def start(_type, _args) do
    children = [
      Repo.Manager.Supervisor,
      Repo.Manager.Registry,
      {Task, fn -> Feeb.DB.Boot.run() end}
    ]

    opts = [strategy: :one_for_one, name: Feeb.DB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
