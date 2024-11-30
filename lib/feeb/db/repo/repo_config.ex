defmodule Feeb.DB.Repo.RepoConfig do
  @struct_keys [:context, :shard_id, :mode, :path]
  @enforce_keys @struct_keys
  defstruct @struct_keys

  @doc """
  Builds the initial (and only) RepoConfig from the Repo state
  """
  def from_state(state) do
    %__MODULE__{
      context: state.context,
      shard_id: state.shard_id,
      mode: state.mode,
      path: state.path
    }
  end
end
