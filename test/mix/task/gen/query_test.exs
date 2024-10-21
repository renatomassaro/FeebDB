defmodule Mix.Tasks.FeebDb.Gen.QueryTest do
  use ExUnit.Case, async: true
  import Mox
  alias Mix.Tasks.FeebDb.Gen.Query, as: GenQueryTask
  alias Feeb.Adapters.FileMock
  alias Feeb.DB.Config

  describe "run/1" do
    test "generates a query file when the input is valid" do
      queries_path = Config.queries_path()

      expect(FileMock, :mkdir_p, fn path ->
        # Parent query directory would be created in the correct path
        assert path =~ "#{queries_path}/lobby"
      end)

      expect(FileMock, :touch, fn path ->
        # Query file would be created in the correct path
        assert path == "#{queries_path}/lobby/users.sql"
      end)

      GenQueryTask.run(["users", "-d", "lobby"])
    end

    test "generates a query file when the name ends with .sql" do
      expect(FileMock, :mkdir_p, fn _ -> "" end)

      expect(FileMock, :touch, fn path ->
        # Query file would be created in the correct path
        assert path == "#{Config.queries_path()}/lobby/users.sql"
      end)

      GenQueryTask.run(["users.sql", "-d", "lobby"])
    end

    test "a domain must be specified" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          GenQueryTask.run(["foo"])
        end

      assert error =~ "You need to specify a domain"
    end

    test "domain must be part of the config file" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          GenQueryTask.run(["foo", "-d", "wut"])
        end

      assert error =~ "wut is not defined in your config file"
    end
  end
end
