defmodule Feeb.DB.SQLite do
  @moduledoc """
  - Useful link:
    - List of error codes and their meaning: https://www.sqlite.org/rescode.html
  """

  alias Exqlite.Sqlite3, as: Driver

  @type conn :: reference()
  @type stmt :: reference()

  @default_chunk_size 50

  def open(path) when is_binary(path), do: Driver.open(path)

  def close(conn), do: Driver.close(conn)

  def exec(conn, sql), do: Driver.execute(conn, sql)

  def raw(conn, sql) do
    with {:ok, stmt} <- prepare(conn, sql) do
      all(conn, stmt)
    end
  end

  def raw!(conn, sql) do
    {:ok, r} = raw(conn, sql)
    r
  end

  def raw2(sql, conn), do: raw(conn, sql)
  def raw2!(sql, conn), do: raw!(conn, sql)

  # TODO: What is the error type? Add typespecs and review Repo code
  def prepare(conn, sql) do
    Driver.prepare(conn, sql)
  end

  def bind(_, _, []),
    do: :ok

  def bind(conn, stmt, bindings) when is_list(bindings),
    do: Driver.bind(conn, stmt, bindings)

  @doc """
  Convenience wrapper for functions that do not expect a return value
  """
  def perform(conn, stmt) do
    case step_all(conn, stmt) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  def all(conn, stmt) do
    step_all(conn, stmt)
  end

  def one(conn, stmt) do
    case step_chunk(conn, stmt, 2) do
      {:ok, :not_done, _} -> raise "MultipleResultsError"
      {:ok, :done, [row]} -> {:ok, row}
      {:ok, :done, []} -> {:ok, nil}
      {:error, _} = error -> error
    end
  end

  defp step_all(conn, stmt, acc \\ []) do
    case step_chunk(conn, stmt, @default_chunk_size) do
      {:ok, :not_done, rows} -> step_all(conn, stmt, acc ++ rows)
      {:ok, :done, rows} -> {:ok, acc ++ rows}
      {:error, _} = error -> error
    end
  end

  defp step_chunk(conn, stmt, chunk_size) do
    case Driver.multi_step(conn, stmt, chunk_size) do
      {:rows, rows} -> {:ok, :not_done, rows}
      {:done, rows} -> {:ok, :done, rows}
      {:error, _} = error -> error
    end
  end
end
