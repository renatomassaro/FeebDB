defmodule Feeb.DB.Repo.Manager.Registry do
  @moduledoc """
  ## Registry requirements

  When Registry.fetch_or_create is called, we expect a singleton-like behaviour, where the same PID
  is returned for the same (context, shard_id).

  One way to achieve this is allowing the caller to fetch directly from ETS and, if unsuccessful,
  then going through a Registry GenServer which coordinates reads and writes to the ETS table in a
  synchronous fashion.

  ## Bottleneck and performance considerations

  Following this pattern, it's natural that the Manager Registry becomes a bottleneck, since every
  write request must go through it. However, how often do we require writes from the Registry?
  Here's a list of all of them:

  - Every time a (context, shard_id) is open for the first time (globally).
  - Every time a (context, shard_id) is closed.

  Note that, in a regular webapp scenario, usually the very first request on (context, shard_id)
  after deployment will require opening the connection, and once that request is done we usually
  release the connection (flag it as no longer busy), without actually closing it!

  This kind of access pattern, coupled with the facts that 1) the Registry implementation is simple
  and doesn't block for a long time, and 2) the caller is doing heavier IO operations that require
  sequential nature per scheduler makes me believe that the single-threaded nature of the Registry
  will never impact performance in a significative way.

  As such, we should not change it unless it is proved to be impacting performance, for <insert the
  most famous programming quote here>.
  """

  use GenServer
  require Logger
  alias Feeb.DB.Repo.Manager

  @ets_table :feebdb_manager_registry

  # TODO: Test what happens if the manager dies and restarts
  def fetch_or_create(context, shard_id, table \\ @ets_table) do
    case lookup_manager(context, shard_id, table) do
      manager_pid when is_pid(manager_pid) ->
        if Process.alive?(manager_pid) do
          {:ok, manager_pid}
        else
          call_fetch_or_create(context, shard_id, table)
        end

      nil ->
        call_fetch_or_create(context, shard_id, table)
    end
  end

  defp call_fetch_or_create(context, shard_id, table),
    do: GenServer.call(__MODULE__, {:fetch_or_create, context, shard_id, table})

  # GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    :ets.new(@ets_table, [:set, :protected, :named_table])
    {:ok, %{}}
  end

  def handle_call({:fetch_or_create, context, shard_id, table}, _from, state) do
    manager_pid =
      case lookup_manager(context, shard_id, table) do
        # Manager exists in cache; just return it
        manager_pid when is_pid(manager_pid) ->
          if Process.alive?(manager_pid) do
            manager_pid
          else
            # It is unclear why this happened, therefore it's important to log so we can investigate
            "Manager #{inspect(manager_pid)} found dead for {#{context}, #{shard_id}}"
            |> Logger.warning()

            # The cached process is dead! Create a new Manager and replace the cached one
            instantiate_manager(context, shard_id, table)
          end

        nil ->
          instantiate_manager(context, shard_id, table)
      end

    {:reply, {:ok, manager_pid}, state}
  end

  defp instantiate_manager(context, shard_id, table) do
    {:ok, manager_pid} = Manager.Supervisor.create(context, shard_id)
    insert_manager(context, shard_id, manager_pid, table)
    manager_pid
  end

  defp insert_manager(context, shard_id, manager_pid, table) do
    Logger.debug("Inserting manager #{inspect(manager_pid)} for {#{context}, #{shard_id}}")
    :ets.insert(table, {{context, shard_id}, manager_pid})
  end

  defp lookup_manager(context, shard_id, table) do
    case :ets.lookup(table, {context, shard_id}) do
      [{_, manager_pid}] -> manager_pid
      [] -> nil
    end
  end
end
