defmodule Feeb.DB.Repo.ManagerTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB.{Config}
  alias Feeb.DB.Repo.Manager

  @context :test

  setup %{shard_id: shard_id} do
    {:ok, manager_pid} = start_supervised({Manager, {@context, shard_id}})
    {:ok, %{manager: manager_pid}}
  end

  describe "fetch_connection/3 - write" do
    test "creates the first connection", %{manager: manager} do
      # Nothing initially
      refute :sys.get_state(manager).write_1.pid

      # We'll request the write connection to be created
      assert {:ok, repo} = Manager.fetch_connection(manager, :write)

      # And it is created! Writer is being used by this process
      manager_state = :sys.get_state(manager)
      assert manager_state.write_1.pid == repo
      assert manager_state.write_1.busy?

      # Repo state is correct
      repo_state = :sys.get_state(repo)
      assert repo_state.shard_id == manager_state.shard_id
      assert repo_state.mode == :readwrite
      assert repo_state.path =~ Config.data_dir()
      refute repo_state.transaction_id
    end

    test "blocks when connection is used elsewhere", %{manager: manager} do
      assert {:ok, _repo} = Manager.fetch_connection(manager, :write)

      test_pid = self()

      spawn(fn ->
        Manager.fetch_connection(manager, :write)
        send(test_pid, :got_connection)
      end)

      refute_receive :got_connection, 50
    end
  end

  describe "fetch_connection/3 - read" do
    test "creates the first connection", %{manager: manager} do
      # Nothing initially
      refute :sys.get_state(manager).read_1.pid

      # We'll request the read connection to be created
      assert {:ok, repo} = Manager.fetch_connection(manager, :read)

      # And it is created! Reader is being used by this process
      manager_state = :sys.get_state(manager)
      assert manager_state.read_1.pid == repo
      assert manager_state.read_1.busy?

      # Repo state is correct
      repo_state = :sys.get_state(repo)
      assert repo_state.shard_id == manager_state.shard_id
      assert repo_state.mode == :readonly
      assert repo_state.path =~ Config.data_dir()
      refute repo_state.transaction_id
    end

    test "blocks when all connections are used elsewhere", %{manager: manager} do
      assert {:ok, _repo_1} = Manager.fetch_connection(manager, :read)
      assert {:ok, _repo_2} = Manager.fetch_connection(manager, :read)

      test_pid = self()

      spawn(fn ->
        Manager.fetch_connection(manager, :read)
        send(test_pid, :got_connection)
      end)

      refute_receive :got_connection, 50
    end

    test "first connection is preferred over second", %{manager: manager} do
      assert {:ok, repo_1} = Manager.fetch_connection(manager, :read)
      assert {:ok, repo_2} = Manager.fetch_connection(manager, :read)

      # If both are free, we should favor `repo_1`
      assert :ok == Manager.release_connection(manager, repo_1)
      assert :ok == Manager.release_connection(manager, repo_2)

      assert {:ok, repo_1} == Manager.fetch_connection(manager, :read)
      assert :ok == Manager.release_connection(manager, repo_1)
      assert {:ok, repo_1} == Manager.fetch_connection(manager, :read)
      assert :ok == Manager.release_connection(manager, repo_1)

      # If only `repo_2` is free, then of course it will be chosen next
      assert {:ok, repo_1} == Manager.fetch_connection(manager, :read)
      assert {:ok, repo_2} == Manager.fetch_connection(manager, :read)
      assert :ok == Manager.release_connection(manager, repo_2)
      assert {:ok, repo_2} == Manager.fetch_connection(manager, :read)
    end
  end

  describe "release_connection/2" do
    test "makes a connection available again", %{manager: manager} do
      assert {:ok, repo_w} = Manager.fetch_connection(manager, :write)
      assert {:ok, repo_r} = Manager.fetch_connection(manager, :read)

      manager_state = :sys.get_state(manager)
      assert manager_state.write_1.busy?
      assert manager_state.read_1.busy?

      # Release both connections
      assert :ok == Manager.release_connection(manager, repo_w)
      refute :sys.get_state(manager).write_1.busy?
      assert :ok == Manager.release_connection(manager, repo_r)
      refute :sys.get_state(manager).read_1.busy?

      # And now they can be checked out again
      assert {:ok, repo_w} == Manager.fetch_connection(manager, :write)
      assert {:ok, repo_r} == Manager.fetch_connection(manager, :read)
    end

    test "cancels the repo_timeout timer after release", %{manager: manager} do
      assert {:ok, repo} = Manager.fetch_connection(manager, :write)

      # timer_ref is set when the connection is held by this process
      state_before = :sys.get_state(manager)
      assert state_before.write_1.busy?
      assert state_before.write_1.timer_ref
      assert state_before.write_1.monitor_ref

      assert :ok == Manager.release_connection(manager, repo)

      # Once released, timer_ref was emptied
      state_after = :sys.get_state(manager)
      refute state_after.write_1.busy?
      refute state_after.write_1.timer_ref
      refute state_after.write_1.monitor_ref

      # The `false` return here means the timer could not be found -- because it was canceled
      assert false == Process.cancel_timer(state_before.write_1.timer_ref)
    end
  end

  describe "close_connection/2" do
    test "closes the connection", %{manager: manager} = ctx do
      Test.Feeb.DB.ensure_migrated(:test, ctx.shard_id)

      assert {:ok, repo_w} = Manager.fetch_connection(manager, :write)
      assert {:ok, repo_r} = Manager.fetch_connection(manager, :read)

      manager_state = :sys.get_state(manager)
      assert manager_state.write_1.busy?
      assert manager_state.read_1.busy?

      # Close both connections
      assert :ok == Manager.close_connection(manager, repo_w)
      refute :sys.get_state(manager).write_1.busy?
      assert :ok == Manager.close_connection(manager, repo_r)
      refute :sys.get_state(manager).read_1.busy?

      # We can start new connections
      assert {:ok, new_repo_w} = Manager.fetch_connection(manager, :write)
      assert {:ok, new_repo_r} = Manager.fetch_connection(manager, :read)

      # But they are not the same as the old ones
      refute new_repo_w == repo_w
      refute new_repo_r == repo_r

      # Because the old ones are dead
      refute Process.alive?(repo_w)
      refute Process.alive?(repo_r)
    end
  end

  describe "enqueueing logic" do
    test "fetch_connection/3 returns :timeout when over waiting threshold", %{manager: manager} do
      # The only write connection is blocked
      assert {:ok, _repo} = Manager.fetch_connection(manager, :write)

      # The call blocked for `queue_timeout` milliseconds until it returned `:timeout`
      assert :timeout == Manager.fetch_connection(manager, :write, queue_timeout: 50)

      spawn_pid =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write, queue_timeout: :infinity)
        end)

      # The `test_pid` caller no longer exists in the queue. However, `spawn_pid` is still waiting
      # for a connection
      manager_state = :sys.get_state(manager)
      assert :queue.len(manager_state.write_queue) == 1
      assert :queue.any(fn {{pid, _}, _, _, _} -> pid == spawn_pid end, manager_state.write_queue)
    end

    test "handles when the *only* caller waiting for a connection dies", %{manager: manager} do
      # The only write connection is taken
      assert {:ok, repo} = Manager.fetch_connection(manager, :write)

      spawn_pid =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write, queue_timeout: :infinity)
        end)

      # Initially, the `write_1` connection is busy and `spawn_pid` is waiting for it
      %{write_queue: queue_before, write_1: write_1_before} = :sys.get_state(manager)
      assert write_1_before.busy?
      assert :queue.len(queue_before) == 1
      assert :queue.any(fn {{pid, _}, _, _, _} -> pid == spawn_pid end, queue_before)

      # The caller has died but is still waiting for a connection, since we are not monitoring it
      # inside Repo.Manager
      Process.exit(spawn_pid, :kill)

      # The write connection will now be made available to the dead caller
      assert :ok == Manager.release_connection(manager, repo)

      # Afterwards, the connection is no longer busy and there is no one in the queue
      %{write_queue: queue_after, write_1: write_1_after} = :sys.get_state(manager)
      refute write_1_after.busy?
      assert :queue.len(queue_after) == 0
    end

    test "handles when *one* of the callers waiting for a connection dies", %{manager: manager} do
      # The only write connection is taken
      assert {:ok, repo} = Manager.fetch_connection(manager, :write)
      test_pid = self()

      spawn_pid_1 =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write, queue_timeout: :infinity)
        end)

      spawn_pid_2 =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write, queue_timeout: :infinity)
          send(test_pid, :got_connection)
          block_forever()
        end)

      # Initially, both `spawn_pid_1` and `spawn_pid_2` are in the queue (in that order)
      %{write_queue: queue_before} = :sys.get_state(manager)
      assert :queue.len(queue_before) == 2
      assert {{:value, {{^spawn_pid_1, _}, _, _, _}}, next_queue} = :queue.out(queue_before)
      assert {{:value, {{^spawn_pid_2, _}, _, _, _}}, _} = :queue.out(next_queue)

      # We'll kill spawn_pid_1, which as seen above is the first element in the queue
      Process.exit(spawn_pid_1, :kill)

      # The write connection will now be made available to the queue
      assert :ok == Manager.release_connection(manager, repo)

      # Now the queue is empty but the `write_1` connection is busy
      %{write_queue: queue_after, write_1: write_1_after} = :sys.get_state(manager)
      assert :queue.len(queue_after) == 0
      assert write_1_after.busy?

      # See? `spawn_pid_2` was awarded the connection despite `spawn_pid_1` dying in front of it.
      # Erlang can be brutal sometimes
      assert_receive :got_connection, 50
    end
  end

  describe "monitoring logic - leasee's death" do
    test "when process holding connection dies, connection is released", %{manager: manager} do
      request_pid =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write)
          block_forever()
        end)

      # The Repo.Manager currently has an active write_1 connection leased to `request_pid`
      state_before = :sys.get_state(manager)
      assert state_before.write_1.busy?
      assert state_before.write_1.caller_pid == request_pid

      # Now we kill `request_pid`
      Process.exit(request_pid, :kill)

      # Give ample time for the `handle_info({:DOWN, ...})` callback to run
      :timer.sleep(10)

      # With the death of `request_pid`, the write_1 connection is now available to be picked up
      state_after = :sys.get_state(manager)
      refute state_after.write_1.busy?
      assert state_after.write_1.caller_pid == nil
      assert state_after.write_1.monitor_ref == nil
    end

    test "on caller's death, requests on the queue get served", %{manager: manager} do
      request_pid =
        spawn_and_wait(fn ->
          Manager.fetch_connection(manager, :write)
          block_forever()
        end)

      queued_request_pid =
        spawn_and_wait(fn ->
          # This guy is waiting for `request_pid` to release the connection
          Manager.fetch_connection(manager, :write)
          block_forever()
        end)

      # The write_1 connection is busy (leased to `request_pid`) and there is a non-empty queue
      state_before = :sys.get_state(manager)
      assert state_before.write_1.busy?
      assert state_before.write_1.caller_pid == request_pid
      assert :queue.len(state_before.write_queue) == 1

      # Now we kill `request_pid`
      Process.exit(request_pid, :kill)

      # Give ample time for the `handle_info({:DOWN, ...})` callback to run
      :timer.sleep(10)

      # Now, the write_1 connection is still busy, but leased to `queued_request_pid` instead.
      # The write_queue is now empty
      state_after = :sys.get_state(manager)
      assert state_after.write_1.busy?
      assert state_after.write_1.caller_pid == queued_request_pid
      assert state_after.write_1.monitor_ref != state_before.write_1.monitor_ref
      assert :queue.len(state_after.write_queue) == 0
    end
  end

  describe "monitoring logic - leasee's living forever" do
    test "raises a timeout exception when leasee exceeded the timeout", %{manager: manager} do
      # We'll start a connection and specify it should be released within 25ms
      assert {:ok, repo} = Manager.fetch_connection(manager, :write, timeout: 25)

      # The leasee actually started a DB transaction
      assert :ok == GenServer.call(repo, {:begin, :exclusive})

      # And there is a `transaction_id` reference in the Repo state
      repo_state = :sys.get_state(repo)
      assert repo_state.transaction_id

      # We didn't release it, so within 25ms (plus overhead) we received an :EXIT signal
      Process.flag(:trap_exit, true)
      assert_receive {:EXIT, manager, :feebdb_repo_timeout}, 50

      # The manager state was updated to make sure the `:write_1` connection is free again
      manager_state = :sys.get_state(manager)
      refute manager_state.write_1.busy?
      refute manager_state.write_1.caller_pid
      refute manager_state.write_1.timer_ref
      refute manager_state.write_1.monitor_ref

      # The Repo state was updated -- the transaction rolled back and its reference was reset
      repo_state = :sys.get_state(repo)
      refute repo_state.transaction_id

      # And we can actually grab it again
      assert {:ok, _} = Manager.fetch_connection(manager, :write)
    end

    test "allows for infinite timeout", %{manager: manager} do
      assert {:ok, _repo} = Manager.fetch_connection(manager, :write, timeout: :infinity)

      # When `timeout: :infinity` we don't create a Repo timeout timer
      manager_state = :sys.get_state(manager)
      assert manager_state.write_1.timer_ref == :no_timer
    end
  end
end
