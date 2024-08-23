defmodule Feeb.DB.LocalState do
  @moduledoc """
  This module is responsible for managing Process state.
  """

  require Logger

  @typep entry :: %{
           context: atom(),
           shard_id: integer(),
           manager_pid: pid(),
           repo_pid: pid(),
           access_type: :read | :write
         }

  def set_current_context(context, shard_id) do
    Process.put(:feebdb_current_context, {context, shard_id})
  end

  def unset_current_context do
    Process.put(:feebdb_current_context, nil)
  end

  @spec get_current_context!() :: entry
  def get_current_context! do
    {context, shard_id} = Process.get(:feebdb_current_context) || raise "Current context not set"
    state = Process.get(:feebdb_state)
    Map.fetch!(state, {context, shard_id})
  end

  def add_entry(context, shard_id, {manager_pid, repo_pid, access_type}) do
    true = is_pid(manager_pid)
    true = is_pid(repo_pid)
    true = access_type in [:read, :write]

    entry = %{
      context: context,
      shard_id: shard_id,
      manager_pid: manager_pid,
      repo_pid: repo_pid,
      access_type: access_type
    }

    key = {context, shard_id}

    feebdb_state = Process.get(:feebdb_state, %{})

    if Map.has_key?(feebdb_state, key),
      do: Logger.warning("Adding LocalState entry to a key that already exists: #{inspect(key)}")

    new_state = Map.put(feebdb_state, {context, shard_id}, entry)

    Process.put(:feebdb_state, new_state)
  end

  def remove_entry(context, shard_id) do
    state = Process.get(:feebdb_state) || raise "No LocalState found"

    if not Map.has_key?(state, {context, shard_id}) do
      "Attempted to delete #{inspect({context, shard_id})} from State but it no longer exists"
      |> Logger.warning()
    end

    new_state = Map.drop(state, [{context, shard_id}])
    Process.put(:feebdb_state, new_state)
  end
end
