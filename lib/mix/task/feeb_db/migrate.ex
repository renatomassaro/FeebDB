defmodule Mix.Tasks.FeebDb.Migrate do
  use Mix.Task
  require Logger
  alias Feeb.DB.{Boot, Repo}

  @impl Mix.Task
  def run(_args) do
    Repo.Manager.Supervisor.start_link([])
    Repo.Manager.Registry.start_link([])
    Boot.migrate_shards()
  end
end
