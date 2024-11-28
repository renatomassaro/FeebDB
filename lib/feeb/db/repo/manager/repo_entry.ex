defmodule Feeb.DB.Repo.Manager.RepoEntry do
  @enforce_keys [:busy?]
  defstruct [:pid, :caller_pid, :timer_ref, :monitor_ref, busy?: false]

  @doc """
  Creates an initial RepoEntry
  """
  def on_start, do: %__MODULE__{busy?: false}

  @doc """
  Set a Repo pid to an initial RepoEntry
  """
  def on_establish(%__MODULE__{pid: nil, busy?: false}, repo_pid),
    do: %__MODULE__{pid: repo_pid, busy?: false}

  @doc """
  Lease a Repo (in an established RepoEntry) to a particular caller
  """
  def on_acquire(%__MODULE__{pid: pid, busy?: false}, {caller_pid, monitor_ref, timer_ref}) do
    %__MODULE__{
      pid: pid,
      busy?: true,
      caller_pid: caller_pid,
      monitor_ref: monitor_ref,
      timer_ref: timer_ref
    }
  end

  @doc """
  Release the Repo in a leased RepoEntry
  """
  def on_release(%__MODULE__{pid: repo_pid}) do
    %__MODULE__{
      pid: repo_pid,
      busy?: false
    }
  end

  @doc """
  Remove the Repo from an initial, established or released RepoEntry.
  """
  def on_close(%__MODULE__{pid: _, busy?: false}), do: on_start()
end
