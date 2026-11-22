defmodule Feeb.DB.CaptureTest do
  use Test.Feeb.DBCase, async: true
  alias Feeb.DB.LocalState
  alias Test.Feeb.DB, as: TestDB
  alias Sample.{Post, User}

  @context :test

  describe "insert" do
    test "captures the row of a templated insert",
         %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      post = new_post(1, is_draft: true) |> DB.insert!()

      assert [%{"t" => "posts", "op" => "insert", "pk" => %{"id" => 1}, "row" => row}] =
               last_changes()

      # Values are captured as they are stored in SQLite
      assert row == %{
               "id" => 1,
               "title" => "Post 1",
               "body" => "Body",
               "is_draft" => 1,
               "inserted_at" => to_string(post.inserted_at),
               "updated_at" => to_string(post.updated_at)
             }

      # Same thing when using the `returning` option
      assert {:ok, _} = new_post(2) |> DB.insert(returning: true)

      assert [%{"pk" => %{"id" => 2}, "row" => %{"title" => "Post 2", "body" => "Body"}}] =
               last_changes()
    end

    @tag db: :lobby
    test "captures the row of a hand-written insert", %{shard_id: shard_id} do
      DB.begin(:lobby, shard_id, :write)

      user =
        %{
          external_id: "ext_1",
          username: "phoebe",
          email: "phoebe@central.perk",
          password: "smelly_cat",
          inserted_at: "2024-01-01"
        }
        |> User.new()
        |> DB.insert!()

      assert [%{"t" => "users", "op" => "insert", "pk" => %{"id" => id}, "row" => row}] =
               last_changes()

      assert id == user.id

      assert row == %{
               "id" => id,
               "external_id" => "ext_1",
               "username" => "phoebe",
               "email" => "phoebe@central.perk",
               "password" => "smelly_cat",
               "inserted_at" => "2024-01-01"
             }
    end

    test "captures INSERT OR IGNORE only when the row was inserted", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      new_post(1) |> DB.insert!()

      # Post 1 already exists, so nothing is inserted and nothing is captured
      insert_or_ignore([1, "Other", "Body"])

      assert [[%{"pk" => %{"id" => 1}, "row" => %{"title" => "Post 1"}}]] =
               LocalState.get_current_context!().changes

      insert_or_ignore([2, "Post 2", "Body"])

      assert [%{"t" => "posts", "op" => "insert", "pk" => %{"id" => 2}, "row" => row}] =
               last_changes()

      # The whole row as stored, including the columns the insert left out
      assert row == %{
               "id" => 2,
               "title" => "Post 2",
               "body" => "Body",
               "is_draft" => nil,
               "inserted_at" => nil,
               "updated_at" => nil
             }
    end

    test "captures the primary key SQLite assigns, and returns it", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      {:ok, post} = %{title: "No id", body: "Body"} |> Post.new() |> DB.insert()

      assert [%{"pk" => %{"id" => id}, "row" => %{"id" => id}}] = last_changes()
      assert is_integer(id) and id == post.id
    end
  end

  test "raises and rolls back when the change can't be captured", %{shard_id: shard_id} do
    shard = {@context, shard_id}

    assert_raise Feeb.DB.CaptureError, ~r/without primary key/, fn ->
      TestDB.with_write(shard, fn ->
        send(self(), {:repo_pid, LocalState.get_current_context!().repo_pid})
        break_posts_table!()
        DB.update_all({:posts, :publish_posts_by_title}, ["Post"])
      end)
    end

    # The Repo is still alive, and everything was rolled back
    assert_received {:repo_pid, repo_pid}
    assert Process.alive?(repo_pid)
    assert [] == TestDB.with_read(shard, fn -> DB.all(Post) end)
  end

  test "refuses to commit once a change couldn't be captured", %{shard_id: shard_id} do
    shard = {@context, shard_id}

    DB.begin(@context, shard_id, :write)
    break_posts_table!()

    # The caller rescues the capture error and tries to commit anyways
    assert_raise Feeb.DB.CaptureError, ~r/without primary key/, fn ->
      DB.update_all({:posts, :publish_posts_by_title}, ["Post"])
    end

    {result, log} = with_log(&DB.commit/0)
    assert {:error, :capture_failed} == result
    assert log =~ "Refusing to COMMIT: a change in this transaction was not captured"

    # Connection was released and the transaction rolled back
    refute LocalState.has_current_context?()
    assert [] == TestDB.with_read(shard, fn -> DB.all(Post) end)

    # The flag doesn't carry over to the next transaction
    TestDB.with_write(shard, fn -> new_post(1) |> DB.insert!() end)
    assert [%Post{id: 1}] = TestDB.with_read(shard, fn -> DB.all(Post) end)
  end

  test "captures updates", %{shard_id: shard_id} do
    DB.begin(@context, shard_id, :write)

    new_post(1) |> DB.insert!() |> Post.change_title("New title") |> DB.update!()

    assert [%{"t" => "posts", "op" => "update", "pk" => %{"id" => 1}, "row" => row}] =
             last_changes()

    assert %{"id" => 1, "title" => "New title", "body" => "Body"} = row
  end

  test "captures update_all and delete_all",
       %{shard_id: shard_id} do
    DB.begin(@context, shard_id, :write)

    new_post(1, title: "Post", is_draft: true) |> DB.insert!()
    new_post(2, title: "Post", is_draft: true) |> DB.insert!()
    new_post(3, title: "Other", is_draft: true) |> DB.insert!()

    assert {:ok, nil} == DB.update_all({:posts, :publish_posts_by_title}, ["Post"])

    assert [
             %{"op" => "update", "pk" => %{"id" => 1}, "row" => %{"is_draft" => 0}},
             %{"op" => "update", "pk" => %{"id" => 2}, "row" => %{"is_draft" => 0}}
           ] = Enum.sort_by(last_changes(), & &1["pk"]["id"])

    assert {:ok, nil} == DB.delete_all({:posts, :delete_all_drafts}, [])

    assert [%{"op" => "delete", "pk" => %{"id" => 3}, "row" => %{"title" => "Other"}}] =
             last_changes()
  end

  test "captures deletes, with and without RETURNING", %{shard_id: shard_id} do
    DB.begin(@context, shard_id, :write)

    friend = DB.one({:friends, :get_by_id}, [1])
    assert {:ok, nil} == DB.delete(friend, returning: false)

    assert [%{"t" => "friends", "op" => "delete", "pk" => %{"id" => 1}, "row" => row}] =
             last_changes()

    assert row == %{"id" => 1, "name" => "Phoebe", "sibling_count" => nil}

    friend = DB.one({:friends, :get_by_id}, [2])
    assert {:ok, ^friend} = DB.delete(friend)
    assert [%{"op" => "delete", "pk" => %{"id" => 2}}] = last_changes()
  end

  @tag capture_log: true
  test "does not capture failed queries", %{shard_id: shard_id} do
    DB.begin(@context, shard_id, :write)

    new_post(1) |> DB.insert!()
    assert {:error, _} = new_post(1) |> DB.insert()

    # Only the first insert was captured
    assert [[%{"op" => "insert", "pk" => %{"id" => 1}}]] =
             LocalState.get_current_context!().changes
  end

  describe "capture is off" do
    @tag db: :uncaptured
    test "when the context does not enable it", %{shard_id: shard_id} do
      DB.begin(:uncaptured, shard_id, :write)
      new_post(1) |> DB.insert!()
      # Without capture, `returning: true` still gets `RETURNING <params>`
      assert {:ok, %{id: 2, title: "Post 2"}} = new_post(2) |> DB.insert(returning: true)
      assert %{capture?: false, changes: []} = LocalState.get_current_context!()
    end

    test "when the write disables it, and on reads", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write, capture: false)
      new_post(1) |> DB.insert!()
      assert %{capture?: false, changes: []} = LocalState.get_current_context!()
      DB.commit()

      DB.begin(@context, shard_id, :read)
      assert %{capture?: false, changes: []} = LocalState.get_current_context!()
    end
  end

  defp new_post(id, attrs \\ []) do
    %{id: id, title: "Post #{id}", body: "Body"}
    |> Map.merge(Map.new(attrs))
    |> Post.new()
  end

  # DB.insert/2 only runs the templated insert, so we call the Repo directly
  defp insert_or_ignore(bindings) do
    repo_pid = LocalState.get_current_context!().repo_pid
    {:ok, _} = Feeb.DB.Repo.insert(repo_pid, {:posts, :insert_or_ignore}, bindings, [])
  end

  # Changes from the last captured query
  defp last_changes, do: hd(LocalState.get_current_context!().changes)

  # Recreates `posts`, within the current transaction, with an extra first column (so we can bypass
  # the boot validation). As a result, `RETURNING *` returns NULL where `Post` expect its `id`.
  # This causes an error that is caught on `Capture.changes`.
  # This is async friendly because it runs in a dedicated test shard.
  defp break_posts_table! do
    DB.raw!("DROP TABLE posts")

    DB.raw!("""
    CREATE TABLE posts (extra TEXT, id INTEGER PRIMARY KEY, title TEXT, body TEXT, is_draft INTEGER,
      inserted_at TEXT, updated_at TEXT) STRICT
    """)

    DB.raw!("INSERT INTO posts (id, title, body, is_draft) VALUES (1, 'Post', 'Body', 1)")
  end
end
