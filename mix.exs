defmodule Feeb.DB.Mixfile do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :feebdb,
      version: @version,
      elixir: "~> 1.17",
      description: description(),
      package: package(),
      deps: deps(),
      compilers: Mix.compilers(),
      test_coverage: [tool: ExCoveralls],
      elixirc_paths: elixirc_paths(Mix.env()),
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.html": :test,
        "coveralls.detail": :test,
        "coveralls.json": :test,
        "coveralls.post": :test
      ],
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  def application do
    [
      env: [],
      mod: {Feeb.DB.Application, []}
    ]
  end

  defp description do
    """
    TODO
    """
  end

  defp package do
    [
      files: ["lib", "priv", "mix.exs", "README.md", "LICENSE.md"],
      maintainers: ["Renato Massaro"],
      licenses: ["MIT"],
      links: %{
        Changelog: "todo",
        GitHub: "todo"
      }
    ]
  end

  def deps do
    [
      {:exqlite, "~> 0.23"},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18.2", only: :test},
      {:mix_test_watch, "~> 1.2", only: [:dev, :test], runtime: false}
    ]
  end

  # Specifies which paths to compile per environment
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
