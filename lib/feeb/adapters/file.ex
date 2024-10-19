defmodule Feeb.Adapters.File do
  @behaviour __MODULE__

  @callback mkdir_p(Path.t()) ::
              :ok | {:error, File.posix()}
  def mkdir_p(path) do
    File.mkdir_p(path)
  end

  @callback touch(Path.t()) ::
              :ok | {:error, File.posix()}

  @callback touch(Path.t(), File.erlang_time() | File.posix_time()) ::
              :ok | {:error, File.posix()}
  def touch(path, time \\ System.os_time(:second)) do
    File.touch(path, time)
  end
end
