defmodule Mix.Tasks.FeebDb.Gen.MigrationTest do
  use ExUnit.Case, async: true
  import Mox
  alias Mix.Tasks.FeebDb.Gen.Migration, as: GenMigrationTask
  alias Feeb.Adapters.FileMock
  alias Feeb.DB.Config

  describe "run/1" do
    test "generates a migration file when input is valid (.sql)" do
      migrations_path = Config.migrations_path()

      expect(FileMock, :mkdir_p, fn path ->
        # migrations_path (from Config) is being used as top-level path, which is then prepended
        # to the input domain
        assert path =~ "#{migrations_path}/lobby"
      end)

      expect(FileMock, :touch, fn path ->
        file_name = String.replace(path, "#{migrations_path}/lobby/", "")

        # Generated file contains "add_users.sql"
        assert String.ends_with?(file_name, "_add_users.sql")

        # As a prefix to the migration name, we've added the current time
        timestamp = String.replace(file_name, "_add_users.sql", "")

        # Given the format of YYMMDDhhmmss, we expect 12 chars in it (which can be converted to int)
        assert String.length(timestamp) == 12
        assert String.to_integer(timestamp)
      end)

      GenMigrationTask.run(["add_users", "-d", "lobby"])
    end

    test "generates a migration file when input is valid (.exs)" do
      migrations_path = Config.migrations_path()

      expect(FileMock, :mkdir_p, fn _path ->
        nil
      end)

      expect(FileMock, :mkdir_p, fn path ->
        # migrations_path (from Config) is being used as top-level path, which is then prepended
        # to the input domain
        assert path =~ "#{migrations_path}/lobby"
      end)

      expect(FileMock, :touch, fn path ->
        assert path =~ "#{migrations_path}/lobby"
        file_name = String.replace(path, "#{migrations_path}/lobby/", "")

        # Generated file contains "add_users.exs"
        assert String.ends_with?(file_name, "_add_sessions.exs")
      end)

      GenMigrationTask.run(["add_sessions.exs", "-d", "lobby"])
    end

    test "handles input if the file contains .sql extension" do
      migrations_path = Config.migrations_path()

      expect(FileMock, :mkdir_p, fn _path ->
        nil
      end)

      expect(FileMock, :touch, fn path ->
        assert path =~ "#{migrations_path}/lobby"
        file_name = String.replace(path, "#{migrations_path}/lobby/", "")

        # Generated file contains "with_extension.sql"
        assert String.ends_with?(file_name, "_with_extension.sql")
      end)

      GenMigrationTask.run(["with_extension.sql", "-d", "lobby"])
    end

    test "a domain must be specified" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          GenMigrationTask.run(["foo"])
        end

      assert error =~ "You need to specify a domain"
    end

    test "domain must be part of the config file" do
      %{message: error} =
        assert_raise RuntimeError, fn ->
          GenMigrationTask.run(["foo", "-d", "wut"])
        end

      assert error =~ "wut is not defined in your config file"
    end
  end
end
