defmodule Feeb.DB.HooksTest do
  # Not async because hooks are global and run on every commit
  use Test.Feeb.DBCase, async: false
  alias Feeb.DB.{Hooks, LocalState, Repo}
  alias Sample.Post
  alias Test.Feeb.DB, as: TestDB

  @context :test

  setup do
    on_exit(&TestDB.reset_hooks/0)
  end

  describe "commit hooks" do
    test "run in order, with the changes and assigns",
         %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :first, &send(test_pid, {:first, &1}))

      # It runs inside the transaction, so it can still query the shard and set assigns
      Hooks.register(:before_commit, :second, fn _ ->
        send(test_pid, {:second, DB.all(Post)})
        DB.assign(:seen_by, :second)
      end)

      Hooks.register(:after_commit, :third, &send(test_pid, {:third, &1}))

      DB.begin(@context, shard_id, :write)
      insert_post(1)
      DB.assign(:note, "hello")
      DB.commit()

      assert {:messages, [{:first, before}, {:second, [%Post{id: 1}]}, {:third, after_commit}]} =
               Process.info(self(), :messages)

      assert %{context: @context, shard_id: ^shard_id, capture?: true, assigns: %{note: "hello"}} =
               before

      assert [%{"t" => "posts", "op" => "insert", "pk" => %{"id" => 1}}] = before.changes

      # after_commit sees the assigns set by the before_commit hooks
      assert after_commit == %{before | assigns: %{note: "hello", seen_by: :second}}
    end

    test "run on every write, even without changes, but not on reads",
         %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :before, &send(test_pid, {:before, &1}))
      Hooks.register(:after_commit, :after, &send(test_pid, {:after, &1}))

      DB.begin(@context, shard_id, :write)
      DB.commit()

      DB.begin(@context, shard_id, :write, capture: false)
      insert_post(1)
      DB.commit()

      DB.begin(@context, shard_id, :read)
      DB.commit()

      assert {:messages,
              [
                {:before, %{capture?: true, changes: []}},
                {:after, %{capture?: true, changes: []}},
                {:before, %{capture?: false, changes: []}},
                {:after, %{capture?: false, changes: []}}
              ]} = Process.info(self(), :messages)
    end

    test "writes made inside before_commit are not captured", %{shard_id: shard_id} do
      test_pid = self()

      Hooks.register(:before_commit, :writer, fn _ ->
        insert_post(2)
        send(test_pid, {:in_hook, LocalState.get_current_context!()})
      end)

      Hooks.register(:after_commit, :after, &send(test_pid, {:after, &1}))

      TestDB.with_write({@context, shard_id}, fn -> insert_post(1) end)

      # Capture is off inside the hook, so its insert added no change. We can't see this in the
      # payload because it is built before the hooks run
      assert_received {:in_hook, %{capture?: false, changes: [[%{"pk" => %{"id" => 1}}]]}}
      assert_received {:after, %{changes: [%{"pk" => %{"id" => 1}}]}}

      # But the hook's insert was still committed
      assert [%Post{id: 1}, %Post{id: 2}] = TestDB.with_read({@context, shard_id}, &all_posts/0)
    end

    test "do not run when a read migrates the shard", %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :before, fn _ -> send(test_pid, :before) end)
      Hooks.register(:after_commit, :after, fn _ -> send(test_pid, :after) end)

      # This shard doesn't exist yet, so the read migrates it first using a write connection
      assert [] == TestDB.with_read({@context, shard_id + 1_000_000}, &all_posts/0)

      refute_received :before
      refute_received :after
    end

    test "do not run when the write disables them",
         %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :before, fn _ -> send(test_pid, :before) end)
      Hooks.register(:after_commit, :after, fn _ -> send(test_pid, :after) end)

      DB.begin(@context, shard_id, :write, hooks: false)
      insert_post(1)
      assert %{capture?: false, hooks?: false, changes: []} = LocalState.get_current_context!()
      DB.commit()

      refute_received :before
      refute_received :after
    end

    test "registering an existing id replaces the hook in place", %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:after_commit, :a, fn _ -> send(test_pid, :old_a) end)
      Hooks.register(:after_commit, :b, fn _ -> send(test_pid, :b) end)
      Hooks.register(:after_commit, :a, fn _ -> send(test_pid, :new_a) end)

      commit_post(shard_id, 1)

      assert {:messages, [:new_a, :b]} == Process.info(self(), :messages)
    end
  end

  describe "before_commit errors" do
    test "raise, throw and exit bubble up and keep the transaction open",
         %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :failing, fn _ -> fail(Process.get(:fail_with)) end)
      Hooks.register(:before_commit, :later, fn _ -> send(test_pid, :later) end)
      Hooks.register(:after_commit, :after, fn _ -> send(test_pid, :after) end)

      Enum.each([raise: RuntimeError, throw: :boom, exit: :boom], fn {kind, expected} ->
        Process.put(:fail_with, kind)
        DB.begin(@context, shard_id, :write)
        insert_post(1)

        assert catch_kind(kind, fn -> DB.commit() end) == expected
        assert %{context: @context, shard_id: ^shard_id} = LocalState.get_current_context!()
        DB.rollback()
      end)

      refute_received :later
      refute_received :after
      assert [] == TestDB.with_read({@context, shard_id}, &all_posts/0)
    end
  end

  describe "after_commit" do
    test "errors are logged and do not stop other hooks or the commit",
         %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:after_commit, :failing, fn _ -> raise "boom" end)
      Hooks.register(:after_commit, :working, fn _ -> send(test_pid, :working) end)

      log = capture_log(fn -> commit_post(shard_id, 1) end)

      assert log =~ "FeebDB after_commit hook :failing failed"
      assert log =~ "boom"
      assert_received :working
      assert [%Post{id: 1}] = TestDB.with_read({@context, shard_id}, &all_posts/0)
    end

    test "the write connection is released before they run", %{shard_id: shard_id} do
      test_pid = self()

      # Only for the test's commit, since the task's commit runs the hooks too
      Hooks.register(:after_commit, :take_connection, fn
        %{shard_id: shard_id} when self() == test_pid ->
          task =
            Task.async(fn ->
              # Fails right away if the connection is still held
              DB.begin(@context, shard_id, :write, queue_timeout: 0)
              DB.commit()
            end)

          send(test_pid, {:other_process, Task.await(task)})

        _ ->
          :ok
      end)

      commit_post(shard_id, 1)

      assert_received {:other_process, :ok}
    end

    test "do not run when the commit fails", %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:after_commit, :after, fn _ -> send(test_pid, :after) end)

      DB.begin(@context, shard_id, :write)
      insert_post(1)

      # Any failed commit will do: flag the Repo as if a change couldn't be captured
      Repo.flag_capture_failed(LocalState.get_current_context!().repo_pid)

      {result, log} = with_log(&DB.commit/0)
      assert {:error, :capture_failed} == result
      assert log =~ "Refusing to COMMIT: a change in this transaction was not captured"
      refute_received :after
      assert [] == TestDB.with_read({@context, shard_id}, &all_posts/0)
    end
  end

  describe "assigns" do
    test "are dropped on rollback", %{shard_id: shard_id} do
      test_pid = self()
      Hooks.register(:before_commit, :before, &send(test_pid, {:before, &1.assigns}))

      DB.begin(@context, shard_id, :write)
      DB.assign(:note, "dropped")
      DB.rollback()

      DB.begin(@context, shard_id, :write)
      assert DB.assigns() == %{}
      DB.commit()

      # Only the second transaction ran the hook, since rollbacks don't run hooks
      assert {:messages, [{:before, assigns}]} = Process.info(self(), :messages)
      assert assigns == %{}
    end

    test "need a write transaction", %{shard_id: shard_id} do
      assert_raise DB.NoWriteTransactionError, fn -> DB.assign(:note, 1) end
      assert_raise DB.NoWriteTransactionError, fn -> DB.assigns() end

      DB.begin(@context, shard_id, :read)
      assert_raise DB.NoWriteTransactionError, fn -> DB.assign(:note, 1) end
      DB.commit()
    end
  end

  defp insert_post(id), do: Post.new(%{id: id, title: "Post #{id}", body: "Body"}) |> DB.insert!()
  defp all_posts, do: DB.all(Post) |> Enum.sort_by(& &1.id)

  defp commit_post(shard_id, id) do
    DB.begin(@context, shard_id, :write)
    insert_post(id)
    DB.commit()
  end

  defp fail(:raise), do: raise("boom")
  defp fail(:throw), do: throw(:boom)
  defp fail(:exit), do: exit(:boom)

  defp catch_kind(:raise, fun), do: assert_raise(RuntimeError, fun).__struct__
  defp catch_kind(:throw, fun), do: catch_throw(fun.())
  defp catch_kind(:exit, fun), do: catch_exit(fun.())
end
