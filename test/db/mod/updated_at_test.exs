defmodule Feeb.DB.Mod.UpdatedAtTest do
  use Test.Feeb.DBCase, async: true

  alias Feeb.DB, as: DB
  alias Sample.{Post}

  @context :test

  describe "UpdatedAt mod" do
    test "gets a real value on row insertion", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)

      now = DateTime.utc_now()
      post = Post.new(%{id: 1, title: "Foo", body: "Body"})

      assert {:ok, db_post} = DB.insert(post)

      # Basic data is correct
      assert db_post.id == post.id
      assert db_post.title == post.title
      assert db_post.body == post.body

      # It included `inserted_at` and `updated_at`
      assert db_post.inserted_at
      assert db_post.updated_at
      assert date_diff_ms(now, db_post.inserted_at) <= 10

      # They are the same
      assert db_post.inserted_at == db_post.updated_at
    end

    test "sets a new value on row update", %{shard_id: shard_id} do
      DB.begin(@context, shard_id, :write)
      post = Post.new(%{id: 1, title: "Foo", body: "Body"})
      assert {:ok, post} = DB.insert(post)
      :timer.sleep(2)

      # We'll update the Post
      new_post = Post.change_title(post, "my new title")

      # The `updated_at` field changed
      refute new_post.updated_at == post.updated_at

      # And it was added in the :target fields in __meta__
      assert :updated_at in new_post.__meta__.target

      # It persists if we save the struct in the database
      DB.update!(new_post)

      assert [new_post_db] = DB.all(Post)
      assert new_post_db.updated_at == new_post.updated_at
    end
  end
end
