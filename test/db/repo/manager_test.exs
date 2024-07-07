defmodule Feeb.DB.Repo.ManagerTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB.{Config}
  alias Feeb.DB.Repo.Manager

  @context :test

  setup %{shard_id: shard_id} do
    {:ok, manager_pid} = start_supervised({Manager, {@context, shard_id}})
    {:ok, %{manager: manager_pid}}
  end

  describe "fetch_connection/2 - write" do
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

    test "busy when connection is used elsewhere", %{manager: manager} do
      assert {:ok, _repo} = Manager.fetch_connection(manager, :write)
      assert :busy == Manager.fetch_connection(manager, :write)
      assert :busy == Manager.fetch_connection(manager, :write)
    end
  end

  describe "fetch_connection/2 - read" do
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

    test "busy when connections are used elsewhere", %{manager: manager} do
      assert {:ok, _repo_1} = Manager.fetch_connection(manager, :read)
      assert {:ok, _repo_2} = Manager.fetch_connection(manager, :read)
      assert :busy == Manager.fetch_connection(manager, :read)
      assert :busy == Manager.fetch_connection(manager, :read)
    end

    test "first connection is preferred over second", %{manager: manager} do
      assert {:ok, repo_1} = Manager.fetch_connection(manager, :read)
      assert {:ok, repo_2} = Manager.fetch_connection(manager, :read)
      assert :busy == Manager.fetch_connection(manager, :read)

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
      assert :busy == Manager.fetch_connection(manager, :write)

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
  end

  describe "close_connection/2" do
    test "closes the connection", %{manager: manager} = ctx do
      Test.Feeb.DB.ensure_migrated(:test, ctx.shard_id)

      assert {:ok, repo_w} = Manager.fetch_connection(manager, :write)
      assert {:ok, repo_r} = Manager.fetch_connection(manager, :read)
      assert :busy == Manager.fetch_connection(manager, :write)

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
end
