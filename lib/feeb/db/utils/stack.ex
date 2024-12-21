defmodule Utils.Stack do
  # TODO: Move to its own library
  # TODO: Tests
  @enforce_keys [:entries]
  defstruct [:entries]

  @type t(value_type) :: %__MODULE__{entries: [value_type]}
  @typep generic_stack :: t(v)
  @typep v :: term

  @spec new() :: generic_stack
  def new, do: %__MODULE__{entries: []}

  @spec push(generic_stack, v) :: generic_stack
  def push(%__MODULE__{entries: entries}, entry), do: %__MODULE__{entries: [entry | entries]}

  @type pop(generic_stack) ::
          {:ok, {generic_stack, v}}
          | {:error, :empty}
  def pop(%__MODULE__{entries: []}), do: {:error, :empty}
  def pop(%__MODULE__{entries: [entry | rest]}), do: {:ok, {%__MODULE__{entries: rest}, entry}}

  @type pop!(generic_stack) ::
          {generic_stack, v}
          | no_return
  def pop!(stack) do
    {:ok, result} = pop(stack)
    result
  end

  @spec peek(generic_stack) ::
          {:ok, v}
          | {:error, :empty}
  def peek(%__MODULE__{entries: []}), do: {:error, :empty}
  def peek(%__MODULE__{entries: [entry | _]}), do: {:ok, entry}

  def peek!(stack) do
    {:ok, v} = peek(stack)
    v
  end

  @spec empty?(generic_stack) :: boolean
  def empty?(%__MODULE__{entries: []}), do: true
  def empty?(%__MODULE__{entries: _}), do: false

  @spec find(generic_stack, (v -> boolean) | v) ::
          v | nil
  def find(%__MODULE__{entries: entries}, finder_fn) when is_function(finder_fn) do
    entries
    |> Enum.reduce_while(nil, fn entry, acc ->
      if finder_fn.(entry) do
        {:halt, entry}
      else
        {:cont, acc}
      end
    end)
  end

  def find(%__MODULE__{} = stack, entry_to_find), do: find(stack, &(&1 == entry_to_find))

  @spec any?(generic_stack, (v -> boolean) | v) :: boolean
  def any?(stack, function_or_entry_to_find),
    do: not is_nil(find(stack, function_or_entry_to_find))

  @spec to_list(generic_stack) :: [v]
  def to_list(%__MODULE__{entries: entries}), do: Enum.reverse(entries)

  @spec size(generic_stack) :: integer
  def size(%__MODULE__{entries: entries}), do: length(entries)

  @spec remove(generic_stack, (v -> boolean) | v) ::
          {:ok, {generic_stack, v}}
          | {:error, :not_found}
  def remove(%__MODULE__{entries: entries}, remover_fn) when is_function(remover_fn) do
    entries
    |> Enum.reduce_while({entries, nil}, fn entry, {acc_entries, _} = acc ->
      if remover_fn.(entry) do
        {:halt, {List.delete(acc_entries, entry), {:ok, entry}}}
      else
        {:cont, acc}
      end
    end)
    |> case do
      {new_entries, {:ok, entry}} ->
        {:ok, {%__MODULE__{entries: new_entries}, entry}}

      {_, nil} ->
        {:error, :not_found}
    end
  end
end
