defmodule Test.Utils do
  def date_diff_ms(date_a, date_b),
    do: DateTime.diff(date_a, date_b, :millisecond)

  @doc """
  Spawns a process and blocks waiting for it to start. Might sleep for an `additional_sleep` amount
  of time (in ms). Returns the spawned pid once it's been spawned.
  """
  def spawn_and_wait(fun, additional_sleep \\ 10) do
    test_pid = self()

    spawn_pid =
      spawn(fn ->
        send(test_pid, :spawn_ok)
        fun.()
      end)

    receive do
      :spawn_ok ->
        :timer.sleep(additional_sleep)
        :ok
    end

    spawn_pid
  end

  @doc """
  Starts a receive block that will hang indefinitely. This is used for spawned processes that should
  remain alive until the end of the test suite to avoid flakes.
  """
  def block_forever do
    receive do
      :will_never_receive -> :ok
    end
  end
end
