defmodule Mix.Tasks.FeebDb.Gen.Query do
  use Mix.Task
  require Logger
  alias Feeb.DB.Config

  @requirements ["app.config"]

  @file_adapter Application.compile_env(:feebdb, :adapters)[:file] || Feeb.Adapters.File

  @option_parser_opts [
    aliases: [d: :domain],
    strict: [domain: :string]
  ]

  @impl Mix.Task
  def run([name | other_args]) do
    {parsed_args, _} = OptionParser.parse!(other_args, @option_parser_opts)

    params = contextualize(name, parsed_args, Config.queries_path())

    create_query(params)
  end

  defp create_query(params) do
    # Ensure the top-level queries path is created (this might be the first query)
    @file_adapter.mkdir_p(params.query_path)

    # Create the query file
    @file_adapter.touch(params.full_path)

    Logger.info("Created query file: #{params.full_path}")
  end

  defp contextualize(raw_name, args, queries_base_path) do
    name = contextualize_name(raw_name)
    validate_domain!(args[:domain])

    query_path = Path.join(queries_base_path, args[:domain] || "")
    file_name = "#{name}"

    %{
      query_path: query_path,
      file_name: file_name,
      full_path: Path.join(query_path, file_name)
    }
  end

  defp contextualize_name(name) when is_binary(name) do
    if String.ends_with?(name, ".sql") do
      name
    else
      # If no extension was defined, assume user wants .sql by default
      "#{name}.sql"
    end
  end

  defp validate_domain!(nil),
    do: raise("You need to specify a domain (-d). Please let me know if you think this is an error")

  defp validate_domain!(domain) when is_binary(domain) do
    domain = String.to_atom(domain)

    # Assert that the domain is defined somewhere in the config file
    all_domains =
      Config.contexts()
      |> Enum.map(fn %{domains: domains} -> domains end)
      |> List.flatten()
      |> Enum.uniq()

    if domain not in all_domains do
      raise "#{domain} is not defined in your config file (valid domains: #{inspect(all_domains)})"
    end
  end
end
