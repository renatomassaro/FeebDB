import Config

config :feebdb,
  data_dir: System.get_env("FEEBDB_TEST_DATA_DIR", "/tmp/feebdb/test_dbs"),
  migrations_dir: "priv/test/migrations",
  queries_dir: "priv/test/queries",
  schemas_list: "priv/test/feebdb_schemas.json",
  contexts: %{
    test: %{
      shard_type: :dedicated
    },
    raw: %{
      shard_type: :dedicated
    },
    # `lobby` is an example of a global database (it has a single shard)
    lobby: %{
      shard_type: :global
    },
    # The `saas_prod_one` context has multiple domains, with one of them (`events`) being shared
    # with another context (`saas_prod_two`). The `crm` and `erp` domains are exclusive.
    saas_prod_one: %{
      shard_type: :dedicated,
      domains: [:crm, :events]
    },
    saas_prod_two: %{
      shard_type: :dedicated,
      domains: [:erp, :events]
    }
  }

config :logger, level: :warning
