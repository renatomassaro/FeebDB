defmodule Feeb.DB.Repo.Manager do
  @moduledoc """
  A `Repo.Manager` manages read and write connections for a given repo (database connection).

  ## Lifecycle description

  Let's say you want to open a write connection to a global database, Lobby. And let's imagine you
  are starting from a clean slate (no one ever opened a connection to Lobby before).

  First, DB.begin(:lobby, 1, :write) will call DB.setup_env/3, which will "fetch_or_create" the
  manager from the registry. This is a synchronous, free-of-race-conditions operation that will
  always return the same pid, even if multiple processes request a Manager at the same time.

  The PID returned by the "fetch_or_create" operation points to the GenServer at this module!

  Then, DB.setup_env/3 will proceed and ask for the connection via fetch_connection(pid, type).

  If you look at the state of this module, you'll see it can hold one write connection and up to
  two read connections. For each connection, we store its pid (if any) and whether it's busy.

  When a "fetch_connection" request comes in, we call fetch_or_create_connection/2. This will do
  precisely what the name says, with the risk that it may return :busy if none of the connections
  are available and no new connections can be created.

  Once a connection was created and/or returned, it will be flagged as :busy from now on, and the
  caller process proceed with sending queries to it.

  If the connection can't be created (because all of them are currently in use by other processes),
  this GenServer will enqueue the "fetch_connection" request and block the caller until a suitable
  connection is made available. When that happens, a response is returned with the connection pid.

  Finally, eventually the caller process will be finished with the operation and (hopefully) call
  DB.commit/0 or DB.rollback/0, both of which will trigger DB.delete_env/0, resulting in the
  "release_connection/2" function being called.

  The connection release flags that specific SQLite connection as no longer busy, but it remains
  open and ready to be re-used in a subsequent request! If the caller wishes to fully close the
  connection, they should call "close_connection/2" instead.
  """

  use GenServer
  require Logger
  alias Feeb.DB.Repo
  alias __MODULE__.RepoEntry

  @struct_keys [:context, :shard_id, :write_1, :read_1, :read_2, :write_queue, :read_queue]
  @enforce_keys @struct_keys
  defstruct @struct_keys

  # Time (in ms) after which we should issue a warning for requests waiting in the queue. This can
  # be overriden with the `:queue_warning_threshold` key in `opts` (accepts `:infinity`).
  @default_queue_warning_threshold 50

  # Default time (in ms) that a request can wait for an available connection. This can be overriden
  # with the `:queue_timeout` key in `opts` (accepts `:infinity`).
  @default_queue_timeout 2_000

  # Default time (in ms) that a request can hold the connection. Once that threadshold is hit, the
  # request will be killed and a timeout exception will be raised.
  @default_repo_timeout 1_000

  # Public API

  def start_link({context, shard_id}) do
    GenServer.start_link(__MODULE__, {context, shard_id})
  end

  def fetch_connection(manager_pid, type, opts \\ []) when type in [:write, :read] do
    # Custom timeout information must be set via `:queue_timeout`; default is 2s.
    GenServer.call(manager_pid, {:fetch, type, opts}, :infinity)
  end

  def release_connection(manager_pid, repo_pid) do
    GenServer.call(manager_pid, {:release, repo_pid})
  end

  def close_connection(manager_pid, repo_pid) do
    GenServer.call(manager_pid, {:close, repo_pid})
  end

  # Server API

  def init({context, shard_id}) do
    Logger.info("Starting repo manager for shard #{shard_id} #{inspect(self())}")

    state = %__MODULE__{
      context: context,
      shard_id: shard_id,
      write_1: RepoEntry.on_start(),
      read_1: RepoEntry.on_start(),
      read_2: RepoEntry.on_start(),
      write_queue: :queue.new(),
      read_queue: :queue.new()
    }

    {:ok, state}
  end

  def handle_call({:fetch, mode, opts}, caller, state) do
    case fetch_or_create_connection(mode, state, caller, opts) do
      {:ok, repo_pid, new_state} ->
        {:reply, {:ok, repo_pid}, new_state}

      {:busy, new_state} ->
        new_state = enqueue_request(new_state, mode, caller, opts)
        Logger.info("All #{mode} connections are busy; enqueueing caller")
        {:noreply, new_state}

      {:error, new_state} ->
        {:reply, :error, new_state}
    end
  end

  def handle_call({:release, repo_pid}, _from, state) do
    case do_release_connection(state, repo_pid) do
      {:ok, released_key, new_state} ->
        notify_enqueued_requests(new_state, released_key)
        {:reply, :ok, new_state}

      {:error, _} ->
        {:reply, :error, state}
    end
  end

  def handle_call({:close, repo_pid}, _from, state) do
    with {:ok, released_key, new_state} <- do_release_connection(state, repo_pid),
         {:ok, new_state} <- do_close_connection(new_state, repo_pid) do
      notify_enqueued_requests(new_state, released_key)
      {:reply, :ok, new_state}
    else
      e ->
        Logger.error("Unable to close connection from Repo.Manager: #{inspect(e)}")
        {:reply, :error, state}
    end
  end

  def handle_info({:released_connection, key}, state) do
    {:noreply, process_enqueued_callers(state, key)}
  end

  def handle_info({:repo_timeout, key, caller_pid}, state) do
    case fetch_repo_entry!(state, key) do
      %RepoEntry{pid: repo_pid, busy?: true, caller_pid: ^caller_pid} ->
        # Kill the caller
        Process.exit(caller_pid, :feebdb_repo_timeout)

        # Release the connection. It is supposed to always succeed
        {:ok, ^key, new_state} = do_release_connection(state, repo_pid)
        notify_enqueued_requests(new_state, key)

        {:noreply, new_state}

      %{caller_pid: _other_caller_or_nil} ->
        # Despite the timeout, the connection is no longer used (or used by a different caller)
        # Theoretically, this is a possible race condition. In this case, we just don't do anything
        {:noreply, state}
    end
  end

  def handle_info({:queue_timeout, caller, mode}, state) do
    # Tell the caller no connection was available within the specified timeout interval
    GenServer.reply(caller, :timeout)

    # Remove caller from the queue
    queue_key = if mode == :write, do: :write_queue, else: :read_queue
    queue = fetch_queue!(state, queue_key)
    new_queue = :queue.filter(fn {c, _, _, _} -> c != caller end, queue)

    {:noreply, Map.put(state, queue_key, new_queue)}
  end

  def handle_info({:DOWN, ref, _, caller_pid, reason}, state) do
    keys = [:write_1, :read_1, :read_2]

    state
    |> Map.take(keys)
    |> Enum.find(fn {_, %{monitor_ref: r}} -> r == ref end)
    |> case do
      # We found an active connection referencing this now-dead process. We need to make sure the
      # connection is properly released
      {key, %{pid: repo_pid, busy?: true, caller_pid: ^caller_pid}} ->
        # Sanity check: by definition, the caller process is dead
        false = Process.alive?(caller_pid)

        # Release the connection. It is supposed to always succeed
        {:ok, ^key, new_state} = do_release_connection(state, repo_pid)
        notify_enqueued_requests(new_state, key)

        # If `caller_pid` had a normal death, emit a warning so they remember to properly release
        # the connection. This is likely an application bug
        if reason == :normal do
          ("Process #{inspect(caller_pid)} died with :normal reason without releasing the " <>
             "#{key} connection #{inspect(repo_pid)} -- maybe you forgot to commit/rollback?")
          |> Logger.warning()
        end

        {:noreply, new_state}

      # There is no active connection with this monitor reference. This means the process released
      # the connection before dying. Nothing to do here, then. This branch is probably unreachable
      nil ->
        {:noreply, state}
    end
  end

  defp fetch_or_create_connection(:write, state, caller, opts) do
    cond do
      is_nil(state.write_1.pid) ->
        with {:ok, _, state} <- establish_connection(state, :write_1) do
          fetch_available_connection(state, :write_1, caller, opts)
        end

      not state.write_1.busy? ->
        fetch_available_connection(state, :write_1, caller, opts)

      state.write_1.busy? ->
        {:busy, state}
    end
  end

  defp fetch_or_create_connection(:read, state, caller, opts) do
    cond do
      is_nil(state.read_1.pid) ->
        with {:ok, _, state} <- establish_connection(state, :read_1) do
          fetch_available_connection(state, :read_1, caller, opts)
        end

      not state.read_1.busy? ->
        fetch_available_connection(state, :read_1, caller, opts)

      is_nil(state.read_2.pid) ->
        with {:ok, _, state} <- establish_connection(state, :read_2) do
          fetch_available_connection(state, :read_2, caller, opts)
        end

      not state.read_2.busy? ->
        fetch_available_connection(state, :read_2, caller, opts)

      state.read_2.busy? ->
        {:busy, state}
    end
  end

  defp do_release_connection(state, pid) do
    case get_key_from_pid(state, pid) do
      {:ok, key} ->
        repo_entry = fetch_repo_entry!(state, key)

        # Stop monitoring the caller process since it released the connection
        Process.demonitor(repo_entry.monitor_ref)

        # Stop the repo_timeout timer
        stop_timer(repo_entry.timer_ref)

        # Notify the Repo that it's been released
        :ok = Repo.notify_release(pid)

        {:ok, key, Map.put(state, key, RepoEntry.on_release(repo_entry))}

      :error ->
        Logger.error("[release] Unexpected error; can't find key for #{inspect(pid)}")
        {:error, state}
    end
  end

  defp do_close_connection(state, pid) do
    case get_key_from_pid(state, pid) do
      {:ok, key} ->
        repo_entry = fetch_repo_entry!(state, key)

        :ok = Repo.close(pid)
        {:ok, Map.put(state, key, RepoEntry.on_close(repo_entry))}

      :error ->
        Logger.error("[close] Unexpected error; can't find key for #{inspect(pid)}")
        {:error, state}
    end
  end

  defp get_key_from_pid(%{write_1: %{pid: pid}}, pid), do: {:ok, :write_1}
  defp get_key_from_pid(%{read_1: %{pid: pid}}, pid), do: {:ok, :read_1}
  defp get_key_from_pid(%{read_2: %{pid: pid}}, pid), do: {:ok, :read_2}
  defp get_key_from_pid(_, _), do: :error

  defp fetch_queue!(state, queue_key), do: Map.fetch!(state, queue_key)

  defp fetch_repo_entry!(state, key), do: Map.fetch!(state, key)

  defp establish_connection(%{shard_id: shard_id, context: context} = state, key) do
    mode = if(key == :write_1, do: :readwrite, else: :readonly)
    db_path = Repo.get_path(context, shard_id)

    # REVIEW: Do I really want to link both genservers?
    case Repo.start_link({context, shard_id, db_path, mode, self()}) do
      {:ok, repo_pid} ->
        log(:info, "Established and fetched #{mode} connection", state)
        current_repo_spec = Map.fetch!(state, key)
        {:ok, repo_pid, Map.put(state, key, RepoEntry.on_establish(current_repo_spec, repo_pid))}

      error ->
        log(:error, "Error creating #{mode} connection: #{inspect(error)}", state)
        {:error, state}
    end
  end

  defp fetch_available_connection(state, key, {caller_pid, _}, opts) do
    case Map.fetch!(state, key) do
      %RepoEntry{pid: pid, busy?: false} = repo_entry ->
        monitor_ref = Process.monitor(caller_pid)
        timer_ref = start_repo_timeout_timer(key, caller_pid, opts)

        new_repo_entry = RepoEntry.on_acquire(repo_entry, {caller_pid, monitor_ref, timer_ref})
        {:ok, pid, Map.put(state, key, new_repo_entry)}

      %{busy?: true} ->
        {:busy, state}
    end
  end

  defp enqueue_request(state, mode, caller, opts) when mode in [:read, :write] do
    time = System.monotonic_time(:millisecond)
    queue_key = if mode == :write, do: :write_queue, else: :read_queue
    timer_ref = start_queue_timeout_timer(caller, mode, opts)

    queue = fetch_queue!(state, queue_key)
    Map.put(state, queue_key, :queue.in({caller, time, timer_ref, opts}, queue))
  end

  defp notify_enqueued_requests(%{write_queue: {[], []}}, :write_1), do: :noop
  defp notify_enqueued_requests(%{read_queue: {[], []}}, :read_1), do: :noop
  defp notify_enqueued_requests(%{read_queue: {[], []}}, :read_2), do: :noop
  defp notify_enqueued_requests(_, key), do: send(self(), {:released_connection, key})

  defp process_enqueued_callers(state, released_key) do
    queue_key = if released_key == :write_1, do: :write_queue, else: :read_queue
    queue = fetch_queue!(state, queue_key)

    with {{:value, {{caller_pid, _} = caller, enqueued_at, timer_ref, opts}}, new_queue} <-
           :queue.out(queue),
         true <- Process.alive?(caller_pid) || {:dead_caller, caller_pid, timer_ref, new_queue},
         {:ok, conn_pid, new_state} <- fetch_available_connection(state, released_key, caller, opts) do
      GenServer.reply(caller, {:ok, conn_pid})

      track_queue_latency(state.shard_id, queue_key, enqueued_at, opts)
      stop_timer(timer_ref)

      Map.put(new_state, queue_key, new_queue)
    else
      # The caller died while waiting for a connection. Remove it from the queue and try again. The
      # connection is still available, so we might as well lease it to the next entry in the queue
      {:dead_caller, caller_pid, timer_ref, new_queue} ->
        Logger.info("Caller #{inspect(caller_pid)} waiting for a connection has died; skipping it")
        stop_timer(timer_ref)
        new_state = Map.put(state, queue_key, new_queue)
        process_enqueued_callers(new_state, released_key)

      # The queue is empty. Nothing else needs to be done
      {:empty, _} ->
        state

      # The connection released just now was fetched by a different caller in the meantime
      {:busy, _} ->
        state
    end
  end

  defp track_queue_latency(shard_id, queue_key, enqueued_at, opts) do
    latency = System.monotonic_time(:millisecond) - enqueued_at
    latency_threshold = opts[:queue_warning_threshold] || @default_queue_warning_threshold

    if latency >= latency_threshold do
      ("#{queue_key} latency for shard #{shard_id} above threshold " <>
         "(#{latency_threshold}ms): #{latency}ms")
      |> Logger.warning()
    else
      Logger.info("#{queue_key} latency for shard #{shard_id}: #{latency}ms")
    end
  end

  defp start_repo_timeout_timer(key, caller_pid, opts) do
    repo_timeout = opts[:timeout] || @default_repo_timeout

    if is_integer(repo_timeout) do
      Process.send_after(self(), {:repo_timeout, key, caller_pid}, repo_timeout)
    else
      :no_timer
    end
  end

  defp start_queue_timeout_timer(caller, mode, opts) do
    queue_timeout = opts[:queue_timeout] || @default_queue_timeout

    if is_integer(queue_timeout) do
      Process.send_after(self(), {:queue_timeout, caller, mode}, queue_timeout)
    else
      :no_timer
    end
  end

  defp stop_timer(ref) when is_reference(ref), do: Process.cancel_timer(ref)
  defp stop_timer(:no_timer), do: :noop

  defp log(level, msg, state, extra_ctx \\ []) do
    log_fn =
      case level do
        :info -> &Logger.info/2
        # :warning -> &Logger.warning/2
        :error -> &Logger.error/2
      end

    base_ctx = [pid: self(), shard_id: state.shard_id]

    log_fn.(msg, base_ctx ++ extra_ctx)
  end
end
