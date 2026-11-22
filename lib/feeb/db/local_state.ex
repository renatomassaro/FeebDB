defmodule Feeb.DB.LocalState do
  @moduledoc """
  This module is responsible for managing Process state.
  """

  require Logger

  # TODO: Most of these types should be defined at `Feeb.DB`
  @typep context :: atom()
  @typep shard_id :: integer()
  @typep access_type :: :read | :write

  # `context`, `shard_id`, `access_type` and `capture?` are public (see `Feeb.DB.Hooks`)
  @type entry :: %{
          context: context,
          shard_id: shard_id,
          manager_pid: pid(),
          repo_pid: pid(),
          access_type: access_type,
          capture?: boolean(),
          hooks?: boolean(),
          changes: [[map()]],
          assigns: map()
        }

  @doc """
  Creates a new context.
  """
  @spec add_context(context, shard_id, {pid(), pid(), access_type}, keyword()) :: :ok
  def add_context(context, shard_id, {manager_pid, repo_pid, access_type}, flags \\ []) do
    true = is_pid(manager_pid)
    true = is_pid(repo_pid)
    true = access_type in [:read, :write]

    state = %{
      context: context,
      shard_id: shard_id,
      manager_pid: manager_pid,
      repo_pid: repo_pid,
      access_type: access_type,
      capture?: Keyword.get(flags, :capture?, false),
      hooks?: Keyword.get(flags, :hooks?, false),
      changes: [],
      assigns: %{}
    }

    Process.put(:feebdb_contexts, Stack.push(contexts(), state))
  end

  @doc """
  Records the changes of one write query in the current context.
  """
  @spec add_changes([map()]) :: :ok
  def add_changes([]), do: :ok

  def add_changes(changes),
    do: update_current_context(&%{&1 | changes: [changes | &1.changes]})

  @doc """
  Set a new assign to the current context.
  """
  @spec put_assign(term(), term()) :: :ok
  def put_assign(key, value),
    do: update_current_context(&%{&1 | assigns: Map.put(&1.assigns, key, value)})

  @doc """
  Stops capturing changes in the current context.
  """
  @spec disable_capture() :: :ok
  def disable_capture,
    do: update_current_context(&%{&1 | capture?: false})

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

  defp update_current_context(fun) do
    {:ok, {stack, entry}} = Stack.pop(contexts())
    Process.put(:feebdb_contexts, Stack.push(stack, fun.(entry)))
    :ok
  end

  defp context_exists?({context, shard_id}) do
    Stack.any?(contexts(), fn %{context: c, shard_id: s} -> c == context and s == shard_id end)
  end

  @spec contexts :: Stack.t(entry)
  defp contexts,
    do: Process.get(:feebdb_contexts, Stack.new())
end
