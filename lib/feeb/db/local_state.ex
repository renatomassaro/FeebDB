defmodule Feeb.DB.LocalState do
  @moduledoc """
  This module is responsible for managing Process state.
  """

  require Logger

  alias Utils.Stack

  # TODO: Most of these types should be defined at `Feeb.DB`
  @typep context :: atom()
  @typep shard_id :: integer()
  @typep access_type :: :read | :write

  @typep entry :: %{
           context: context,
           shard_id: shard_id,
           manager_pid: pid(),
           repo_pid: pid(),
           access_type: access_type
         }

  @doc """
  Creates a new context.
  """
  @spec add_context(context, shard_id, {pid(), pid(), access_type}) ::
          :ok
  def add_context(context, shard_id, {manager_pid, repo_pid, access_type}) do
    true = is_pid(manager_pid)
    true = is_pid(repo_pid)
    true = access_type in [:read, :write]

    state = %{
      context: context,
      shard_id: shard_id,
      manager_pid: manager_pid,
      repo_pid: repo_pid,
      access_type: access_type
    }

    Process.put(:feebdb_contexts, Stack.push(contexts(), state))
  end

  @doc """
  Sets the current context to be the given one.

  The given context must exist.
  """
  @spec set_current_context(context, shard_id) ::
          :ok
  def set_current_context(context, shard_id) do
    if not context_exists?({context, shard_id}),
      do: raise("Attempted to set a context that doesn't exist: #{inspect({context, shard_id})}")

    {:ok, {stack, state}} =
      Stack.remove(contexts(), fn %{context: c, shard_id: s} ->
        c == context && s == shard_id
      end)

    Process.put(:feebdb_contexts, Stack.push(stack, state))
  end

  @doc """
  Removes the current context. Once removed, the new current context will be the topmost element in
  the stack (if any).
  """
  @spec remove_current_context() :: :ok
  def remove_current_context do
    true = not Stack.empty?(contexts()) || raise "Can't remove context from empty stack"
    {:ok, {new_contexts, _state}} = Stack.pop(contexts())
    Process.put(:feebdb_contexts, new_contexts)
  end

  @spec get_current_context!() :: entry | nil
  def get_current_context do
    case Stack.peek(contexts()) do
      {:ok, state} -> state
      {:error, :empty} -> nil
    end
  end

  @spec get_current_context!() :: entry | no_return
  def get_current_context! do
    case get_current_context() do
      %{} = state -> state
      nil -> raise "Current context not set"
    end
  end

  @spec has_current_context?() :: boolean
  def has_current_context?,
    do: not is_nil(get_current_context())

  @spec count_open_contexts() :: integer
  def count_open_contexts,
    do: Stack.size(contexts())

  defp context_exists?({context, shard_id}) do
    Stack.any?(contexts(), fn %{context: c, shard_id: s} -> c == context and s == shard_id end)
  end

  @spec contexts :: Stack.t(entry)
  defp contexts,
    do: Process.get(:feebdb_contexts, Stack.new())
end
