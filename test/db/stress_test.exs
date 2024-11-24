defmodule Feeb.DB.StressTest do
  use Test.Feeb.DBCase, async: true

  alias Sample.Post

  @moduletag timeout: :infinity
  @moduletag :slow

  @context :test

  test "100k concurrent writes in the same shard at the same time", %{shard_id: shard_id} do
    # Approximate time it takes for this test to complete in my machine:
    # 10k:  0.8s
    # 100k: 7.2s
    # 1M:   72.6s
    # It's interesting to see it grows linearly.

    task_fn = fn id ->
      DB.begin(@context, shard_id, :write,
        queue_warning_threshold: :infinity,
        queue_timeout: :infinity
      )

      %{id: id, title: "Post #{id}", body: "Body #{id}"}
      |> Post.new()
      |> DB.insert!()

      DB.commit()
    end

    # Attempt to perform 100k writes concurrently
    1..100_000
    |> Task.async_stream(fn idx -> task_fn.(idx) end)
    |> Stream.run()

    # All 100k entries were inserted
    DB.begin(@context, shard_id, :read)
    assert [[100_000]] == DB.raw!("select count(*) from posts")
  end
end
