defmodule Feeb.DBTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB, as: DB
  alias Feeb.DB.LocalState
  alias Sample.{AllTypes, CustomTypes, Friend, Post}
  alias Sample.Types.TypedID

  @context :test

  describe "begin/4" do
    test "initiates a write transaction", %{shard_id: shard_id, db: db} do
      assert :ok == DB.begin(@context, shard_id, :write)

      # The environment was set up
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :write
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # Manager has correct data
      m_state = :sys.get_state(state.manager_pid)
      assert m_state.shard_id == shard_id
      assert m_state.write_1.pid == state.repo_pid
      assert m_state.write_1.busy?
      refute m_state.read_1.pid
      refute m_state.read_1.busy?
      refute m_state.read_2.pid
      refute m_state.read_2.busy?

      # Repo has correct data
      r_state = :sys.get_state(state.repo_pid)
      assert r_state.mode == :readwrite
      assert r_state.path == db
      assert r_state.shard_id == shard_id
      assert is_integer(r_state.transaction_id)
    end

    test "initiates a read transaction", %{shard_id: shard_id} do
      Test.Feeb.DB.ensure_migrated(@context, shard_id)

      assert :ok == DB.begin(@context, shard_id, :read)

      # The environment was set up:
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :read

      # Manager has correct data
      m_state = :sys.get_state(state.manager_pid)
      assert m_state.shard_id == shard_id
      assert m_state.read_1.pid == state.repo_pid
      assert m_state.read_1.busy?
      refute m_state.write_1.pid
      refute m_state.write_1.busy?
      refute m_state.read_2.pid
      refute m_state.read_2.busy?

      # Repo has correct data
      r_state = :sys.get_state(state.repo_pid)
      assert r_state.mode == :readonly

      DB.commit()
    end

    test "multiple writes on the same shard are enqueued", %{shard_id: shard_id} do
      task_fn = fn id ->
        DB.begin(@context, shard_id, :write)

        %{id: id, title: "Post #{id}", body: "Body #{id}"}
        |> Post.new()
        |> DB.insert!()

        DB.commit()
      end

      # We'll try to concurrently insert 3 entries in the same shard
      [
        Task.async(fn -> task_fn.(1) end),
        Task.async(fn -> task_fn.(2) end),
        Task.async(fn -> task_fn.(3) end)
      ]
      |> Task.await_many()

      # All 3 entries were inserted
      DB.begin(@context, shard_id, :read)
      assert [_, _, _] = DB.all(Post)
    end

    test "multiple reads on the same shard are enqueued", %{shard_id: shard_id} do
      task_fn = fn id ->
        DB.begin(@context, shard_id, :read)
        result = DB.one({:friends, :get_by_id}, [id])
        DB.commit()

        result
      end

      # We concurrently performed 5 read operations in the same shard. Since there are only two
      # available read Repos, some queueing was necessary
      assert [%Friend{id: 1}, %Friend{id: 2}, %Friend{id: 3}, %Friend{id: 4}, %Friend{id: 5}] =
               [
                 Task.async(fn -> task_fn.(1) end),
                 Task.async(fn -> task_fn.(2) end),
                 Task.async(fn -> task_fn.(3) end),
                 Task.async(fn -> task_fn.(4) end),
                 Task.async(fn -> task_fn.(5) end)
               ]
               |> Task.await_many()
    end

    test "supports multiple contexts in the same process" do
      {:ok, test_shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, test_shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, raw_shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:raw)

      # We can start many different BEGIN EXCLUSIVE connections in the same (Erlang) process!
      # Note they are all different SQLite databases, which is why we don't get a :busy error.
      DB.begin(:test, test_shard_1, :write)
      DB.begin(:test, test_shard_2, :write)
      DB.begin(:raw, raw_shard_1, :write)

      # LocalState contexts were created accordingly
      contexts = Process.get(:feebdb_contexts)

      assert test_entry_1 =
               Stack.find(contexts, fn state ->
                 state.context == :test and state.shard_id == test_shard_1
               end)

      assert test_entry_2 =
               Stack.find(contexts, fn state ->
                 state.context == :test and state.shard_id == test_shard_2
               end)

      assert raw_entry_1 =
               Stack.find(contexts, fn state ->
                 state.context == :raw and state.shard_id == raw_shard_1
               end)

      # Each entry has the correct context and shard ID
      assert test_entry_1.context == :test
      assert test_entry_1.shard_id == test_shard_1
      assert test_entry_2.context == :test
      assert test_entry_2.shard_id == test_shard_2
      assert raw_entry_1.context == :raw
      assert raw_entry_1.shard_id == raw_shard_1

      # Each one has a different Manager / Repo PID (they are different databases, after all)
      refute test_entry_1.manager_pid == test_entry_2.manager_pid
      refute test_entry_2.manager_pid == raw_entry_1.manager_pid
      refute test_entry_1.repo_pid == test_entry_2.repo_pid
      refute test_entry_2.repo_pid == raw_entry_1.repo_pid

      # Each context is stacked on top of each other:
      # We are currently at `raw_shard_1`:
      assert %{context: :raw, shard_id: ^raw_shard_1} = LocalState.get_current_context()
      DB.commit()

      # After the outermost connection is committed, we get back to the second one
      assert %{context: :test, shard_id: ^test_shard_2} = LocalState.get_current_context()
      DB.commit()

      # And now we are back to the first connection
      assert %{context: :test, shard_id: ^test_shard_1} = LocalState.get_current_context()
      DB.commit()

      # Well, we can't commit a fourth time because there is no fourth context
      assert %{message: error} =
               assert_raise(RuntimeError, fn ->
                 DB.commit()
               end)

      assert error =~ "Current context not set"
    end

    test "supports opening the same context and same shard under different access_types" do
      {:ok, shard_id, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_id, :write)
      DB.begin(:test, shard_id, :read)
      DB.commit()
      DB.commit()
    end

    test "supports opening the same context and same shard with same access_type" do
      {:ok, shard_id, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_id, :read)
      DB.begin(:test, shard_id, :read)
      DB.commit()
      DB.commit()
    end
  end

  describe "set_context/2" do
    test "changes the process context" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_1, :write)
      DB.begin(:test, shard_2, :write)

      # We are at `{:test, shard_2}` because that was the last call to DB.begin/4
      state = LocalState.get_current_context!()
      assert state.context == :test
      assert state.shard_id == shard_2

      # Now we will switch to `{:test, shard_1}`
      DB.set_context(:test, shard_1)
      assert LocalState.get_current_context!().shard_id == shard_1

      # And back again to `{:test, shard_2}`
      DB.set_context(:test, shard_2)
      assert LocalState.get_current_context!().shard_id == shard_2
    end

    test "enables interaction between multiple databases in the same Elixir process" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      # This would fail if they were both in the same shard due to the PK
      post_shard_1 = Post.new(%{id: 1, title: "Foo", body: "Body"})
      post_shard_2 = Post.new(%{id: 1, title: "Foo", body: "Body"})

      DB.begin(:test, shard_1, :write)
      DB.begin(:test, shard_2, :write)

      # Insert post in `{:test, shard_1}`
      DB.set_context(:test, shard_1)
      assert {:ok, _} = DB.insert(post_shard_1)

      # Insert post in `{:test, shard_2}`
      DB.set_context(:test, shard_2)
      assert {:ok, _} = DB.insert(post_shard_2)

      # Commit in `{:test, shard_2}`
      DB.commit()

      # Commit in `{:test, shard_1}`
      DB.set_context(:test, shard_1)
      DB.commit()
    end
  end

  describe "with_context/1" do
    test "allows isolated context execution" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      post_shard_1 = Post.new(%{id: 1, title: "Post On Shard 1", body: "Body"})
      post_shard_2 = Post.new(%{id: 1, title: "Post On Shard 2", body: "Body"})

      # Start a transaction on `{:test, shard_1}`
      DB.begin(:test, shard_1, :write)

      # Start a transaction on `{:test, shard_2}` in the same process
      result_from_callback =
        DB.with_context(fn ->
          # with_context/1 does not immediatelly change the context; we are still on shard_1
          assert LocalState.get_current_context!().shard_id == shard_1

          # Now that the transaction has begun, we are on shard_2
          DB.begin(:test, shard_2, :write)
          assert LocalState.get_current_context!().shard_id == shard_2

          assert {:ok, post_2} = DB.insert(post_shard_2)
          DB.commit()
          post_2
        end)

      # Now that the callback above has finished executing, we are back to `{:test, shard_1}`
      assert LocalState.get_current_context!().shard_id == shard_1

      # We can finish writing to `{:test, shard_1}`
      assert {:ok, _post_1} = DB.insert(post_shard_1)
      DB.commit()

      # We have the result from `{:test, shard_2}`
      assert result_from_callback.title == "Post On Shard 2"
    end

    test "supports executing without external/outside transaction" do
      {:ok, shard_id, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      # There is no context "outside" the `with_context` block
      refute LocalState.has_current_context?()

      DB.with_context(fn ->
        # And yet we can start a "temporary" context just fine
        DB.begin(:test, shard_id, :write)
        assert LocalState.has_current_context?()
        DB.commit()
      end)

      # Still nothing outside the `with_context` block
      refute LocalState.has_current_context?()
    end

    test "supports nested connections in the same shard" do
      {:ok, shard_id, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_id, :read)

      DB.with_context(fn ->
        DB.begin(:test, shard_id, :read)
        DB.commit()
      end)

      DB.commit()
    end
  end

  describe "commit/0" do
    test "finishes a transaction", %{shard_id: shard_id} do
      # First we start a transaction
      assert :ok == DB.begin(@context, shard_id, :write)

      # LocalState exists
      state = LocalState.get_current_context!()
      assert state.context == @context
      assert state.shard_id == shard_id
      assert state.access_type == :write

      # Naturally repo and manager are alive
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # Then we COMMIT it
      assert :ok == DB.commit()

      # Corresponding environment no longer exists
      assert_raise RuntimeError, fn ->
        LocalState.get_current_context!()
      end

      # Repo and Manager are still alive after the transaction
      assert Process.alive?(state.manager_pid)
      assert Process.alive?(state.repo_pid)

      # The write connection is available for another request
      m_state = :sys.get_state(state.manager_pid)
      refute m_state.write_1.busy?

      # The `transaction_id` entry no longer exists
      r_state = :sys.get_state(state.repo_pid)
      refute r_state.transaction_id

      # Indeed, we can BEGIN again after COMMIT has finished
      assert :ok == DB.begin(@context, shard_id, :write)
      assert :ok == DB.commit()
      assert :ok == DB.begin(@context, shard_id, :write)
      assert :ok == DB.commit()
    end

    @tag capture_log: true
    test "fails if no transactions are open" do
      assert_raise RuntimeError, fn ->
        DB.commit()
      end
    end
  end

  describe "rollback/0" do
    test "undoes the changes", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{atom: :i_am_atom, integer: 50, map_keys_atom: %{foo: "bar"}}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      DB.rollback()

      # Nothing exists in the `AllTypes` table
      DB.begin(@context, shard_id, :read)
      assert [] == DB.all(AllTypes)
    end

    test "supports multiple contexts being rolled back in the same process" do
      {:ok, shard_1, _} = Test.Feeb.DB.Setup.new_test_db(:test)
      {:ok, shard_2, _} = Test.Feeb.DB.Setup.new_test_db(:test)

      DB.begin(:test, shard_1, :write)
      DB.begin(:test, shard_2, :read)
      DB.rollback()
      DB.rollback()

      # Fails if we try to rollback again
      %{message: error} =
        assert_raise RuntimeError, fn ->
          DB.rollback()
        end

      assert error =~ "Current context not set"
    end
  end

  describe "one/1" do
    test "returns the expected result", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)
      assert %{id: 1, name: "Phoebe"} = DB.one({:friends, :get_by_id}, [1])
      assert [_] = DB.one({:pragma, :user_version})
      assert nil == DB.one({:friends, :get_by_id}, [0])
    end

    test "supports the :format flag", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{atom: :i_am_atom, integer: 50, map_keys_atom: %{foo: "bar"}}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      # Without the flag, we get the full object. Unselected fields are shown as `NotLoaded`
      assert %AllTypes{atom: :i_am_atom, string: string} =
               DB.one({:all_types, :get_atom_and_integer}, [])

      assert string == %DB.Value.NotLoaded{}

      # With the :raw flag, we return the values as they are stored in the database
      assert ["i_am_atom", 50] == DB.one({:all_types, :get_atom_and_integer}, [], format: :raw)
      assert ["{\"foo\":\"bar\"}"] == DB.one({:all_types, :get_map_keys_atom}, [], format: :raw)

      # With the :type flag, we return the values formatted by their types _without_ the full schema
      assert %{map_keys_atom: %{foo: "bar"}} ==
               DB.one({:all_types, :get_map_keys_atom}, [], format: :type)

      assert %{atom: :i_am_atom, integer: 50} ==
               DB.one({:all_types, :get_atom_and_integer}, [], format: :type)
    end

    test "works with window functions when using :raw flag", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{integer: 666}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      %{integer: 2}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      %{integer: 1}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      # NOTE: Currently, this is the only way to work with window functions: by not formatting it
      assert [666] == DB.one({:all_types, :get_max_integer}, [], format: :raw)
      assert [669] == DB.one({:all_types, :get_sum_integer}, [], format: :raw)
    end

    test "supports structs as inputs (e.g. from custom types)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # We have one `CustomType` entry
      %{typed_id: 1}
      |> CustomTypes.creation_params()
      |> CustomTypes.new()
      |> DB.insert!()

      # We can find it
      assert %_{typed_id: typed_id} = DB.one({:custom_types, :get_by_typed_id}, %TypedID{id: 1})
      assert typed_id == %TypedID{id: 1}

      # But we can't find a CustomType that does not exist:
      refute DB.one({:custom_types, :get_by_typed_id}, %TypedID{id: 50})
    end

    test "raises if multiple results are found", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :read)

      assert_raise RuntimeError, fn ->
        DB.one({:friends, :get_all})
      end
    end
  end

  describe "all/3" do
    test "supports the :format flag", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{atom: :i_am_atom, integer: 50, map: %{foo: "bar"}}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      %{atom: :other_atom, integer: -2, map: %{girl: ["so", "confusing"]}}
      |> AllTypes.creation_params()
      |> AllTypes.new()
      |> DB.insert()

      # Without the flag, we get the full object. Unselected fields are shown as `NotLoaded`
      assert [res_1, res_2] = DB.all({:all_types, :get_atom_and_integer}, [])
      assert :i_am_atom in [res_1.atom, res_2.atom]
      assert %DB.Value.NotLoaded{} == res_1.string

      # With the :raw flag, we return the values as they are stored in the database
      assert [["i_am_atom", 50], ["other_atom", -2]] |> Enum.sort() ==
               DB.all({:all_types, :get_atom_and_integer}, [], format: :raw) |> Enum.sort()

      assert [["{\"foo\":\"bar\"}"], ["{\"girl\":[\"so\",\"confusing\"]}"]] |> Enum.sort() ==
               DB.all({:all_types, :get_map}, [], format: :raw) |> Enum.sort()

      # With the :type flag, we return the values formatted by their types _without_ the full schema
      assert [%{map: %{girl: ["so", "confusing"]}}, %{map: %{foo: "bar"}}] |> Enum.sort() ==
               DB.all({:all_types, :get_map}, [], format: :type) |> Enum.sort()

      assert [%{atom: :i_am_atom, integer: 50}, %{atom: :other_atom, integer: -2}] |> Enum.sort() ==
               DB.all({:all_types, :get_atom_and_integer}, [], format: :type) |> Enum.sort()
    end
  end

  describe "insert/1" do
    test "inserts the struct (without RETURNING)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      assert {:ok, friend} =
               %{id: 7, name: "Mike"}
               |> Friend.new()
               |> DB.insert()

      assert friend.name == "Mike"
      assert friend.divorce_count == 0
    end

    test "inserts the struct (with RETURNING)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      assert {:ok, friend} =
               %{id: 7, name: "Mike"}
               |> Friend.new()
               |> DB.insert(returning: true)

      assert friend.name == "Mike"
      assert friend.divorce_count == 0
    end
  end

  describe "update/1" do
    test "updates the struct", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      post =
        %{id: 1, title: "My Post", body: "My Body"}
        |> Post.new()
        |> DB.insert!()

      assert {:ok, new_post} =
               post
               |> Post.change_title("Other title")
               |> DB.update()

      assert new_post.title == "Other title"
      refute post.updated_at == new_post.updated_at
    end

    test "update attempt that failed to find a matching entry", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      friend = DB.one({:friends, :get_by_id}, [1])

      # Before updating, we'll delete this entry
      assert DB.delete!(friend)

      # Now let's try updating it
      assert {:error, :not_found} =
               friend
               |> Friend.update(%{name: "Mr Heckles"})
               |> DB.update()
    end
  end

  describe "update_all/3" do
    test "performs the SQL-based update (without RETURNING)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{id: 1, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      %{id: 2, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      # Both posts are flagged as draft
      assert [post_1, post_2] = DB.all(Post)
      assert post_1.is_draft
      assert post_2.is_draft

      # We'll mark both posts as non-draft
      assert {:ok, nil} == DB.update_all({:posts, :publish_posts_by_title}, ["Post"])

      # Now both posts are published
      assert [post_1, post_2] = DB.all(Post)
      refute post_1.is_draft
      refute post_2.is_draft
    end

    test "performs the SQL-based update (with RETURNING)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{id: 1, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      %{id: 2, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      assert {:ok, 2} == DB.update_all({:posts, :publish_posts_by_title}, ["Post"], returning: true)
    end
  end

  describe "update_all!/3" do
    test "performs the SQL-based update", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # There are no posts initially, so this will not match any results
      assert nil == DB.update_all!({:posts, :publish_posts_by_title}, ["No matches"])

      %{id: 1, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      # Publishes all posts whose title is "Post"
      assert nil == DB.update_all!({:posts, :publish_posts_by_title}, ["Post"])
      assert [%{is_draft: false}] = DB.all(Post)
    end
  end

  describe "delete/1" do
    test "deletes the struct", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      friend = DB.one({:friends, :get_by_id}, [1])

      assert {:ok, friend} == DB.delete(friend)

      # Can't find that Friend anymore
      refute DB.one({:friends, :get_by_id}, [1])
    end

    test "deletes the struct (without RETURNING)", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      friend = DB.one({:friends, :get_by_id}, [1])

      assert {:ok, nil} == DB.delete(friend, returning: false)

      # Can't find that Friend anymore
      refute DB.one({:friends, :get_by_id}, [1])
    end

    test "delete attempt that failed to find a matching entry", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      friend = DB.one({:friends, :get_by_id}, [1])

      # The first operation succeeds
      assert {:ok, friend} == DB.delete(friend)

      # The second operation returns an error tuple
      assert {:error, :not_found} == DB.delete(friend)
    end
  end

  describe "delete_all/3" do
    test "performs the SQL-based delete", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      %{id: 1, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      %{id: 2, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      %{id: 3, title: "Not a draft", body: "My Body", is_draft: false}
      |> Post.new()
      |> DB.insert!()

      # Initially there are 3 posts
      assert [_, _, _] = DB.all(Post)

      assert {:ok, nil} == DB.delete_all({:posts, :delete_all_drafts}, [])

      # Now there's only one (the non-draft one)
      assert [_] = DB.all(Post)
    end
  end

  describe "delete_all!/3" do
    test "performs the SQL-based delete", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      # There are no posts initially, so this will match no results
      assert nil == DB.delete_all!({:posts, :delete_all_drafts}, [])

      %{id: 1, title: "Post", body: "My Body", is_draft: true}
      |> Post.new()
      |> DB.insert!()

      # Now we have 1 post (a draft)
      assert [_] = DB.all(Post)

      # Delete all drafts
      assert nil == DB.delete_all!({:posts, :delete_all_drafts}, [])

      # No more posts
      assert [] = DB.all(Post)
    end
  end
end
