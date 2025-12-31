defmodule Mix.Tasks.FeebDb.ListSchemas do
  use Mix.Task
  require Logger

  @table :detected_schemas
  @default_target "priv/feebdb_schemas.json"

  @impl Mix.Task
  def run(_args) do
    case :timer.tc(&do_run/0) do
      {t, {:ok, total_modules}} ->
        Logger.info("Generated schema for #{total_modules} modules in #{trunc(t / 1000)}ms")

      {_, _} ->
        Logger.error("Error while generating schemas file")
    end
  end

  defp do_run do
    setup_env()
    compile_with_tracer()
    generate_schema_file()
  end

  def trace({:remote_macro, _meta, Feeb.DB.Schema, :__using__, 1} = _, env) do
    context = get_context(env)
    :ets.insert(@table, {context, env.module})
    :ok
  end

  def trace(_, _), do: :ok

  defp setup_env do
    :ets.new(@table, [:named_table, :public, :bag])
  end

  defp compile_with_tracer do
    Mix.Task.clear()
    Mix.Task.run("compile", ["--force", "--tracer", __MODULE__])
  end

  def generate_schema_file do
    modules =
      @table
      |> :ets.tab2list()
      |> Enum.sort()
      |> Enum.reduce(%{}, fn {ctx, mod}, acc ->
        ctx_mods = Map.get(acc, ctx, [])
        Map.put(acc, ctx, [to_string(mod) | ctx_mods] |> Enum.sort())
      end)

    File.write!(@default_target, :json.encode(modules))

    total_modules =
      modules
      |> Map.values()
      |> List.flatten()
      |> length()

    {:ok, total_modules}
  end

  defp get_context(%{file: path, module: module}) do
    try do
      path
      |> File.read!()
      |> String.split("@context :")
      |> Enum.at(1)
      |> String.split("\n")
      |> List.first()
      |> String.to_atom()
    rescue
      FunctionClauseError ->
        raise "Unable to find @context module attribute in #{module}"
    end
  end
end
