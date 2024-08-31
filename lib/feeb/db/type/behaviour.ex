defmodule Feeb.DB.Type.Behaviour do
  @moduledoc """
  Behaviour used to implement a type that can be used in any FeebDB schema.
  """

  @type supported_sqlite_type :: :integer | :real | :text | :blob | :null

  @typedoc """
  Refers to the module and field being cast, dumped and/or loaded. It is meant exclusively for
  debugging purposes: if there is a type error, an exception (or warning) will be able to include
  where, exactly, that error happened, so the developer has immediate feedback on what's wrong.
  """
  @type metadata :: {schema :: module(), field :: atom()}

  @doc """
  Specifies which SQLite type the FeebDB type maps to. SQLite has very few types, as can be seen
  at `supported_sqlite_type`. We are not constrained by this, however, as long as we perform
  transformations at the application level.

  For example, we can store the FeebDB type `:boolean` in an `:integer`, where 0/1 in SQLite gets
  converted to false/true in the application. Or, we can store the `:map` FeebDB type in an `:test`
  field, which is converted to/from map format using JSON.

  The functions cast!/2, dump!/2 and load!/2 are responsible for this transformation.
  """
  @callback sqlite_type() :: supported_sqlite_type()

  # TODO: For `cast!/2` specifically, I want in the future to change its contract to return
  # {:ok, term()} | {:error, reason :: binary()}. If a field fails to cast, we shouldn't crash but
  # instead we should return an Schema with `valid?=false` and `[{field, reason}]` error somewhere.
  @doc """
  Casts the input value to a value that makes sense for the corresponding FeebDB type.

  For example, if we have an `:atom` type and the input is `"foo"`, we can safely cast it to `:foo`.
  Another interesting use-case is setting the precision of the DateTime struct in the `:datetime`
  type.

  `cast!/2` is called by `Schema.cast/2` and when the Schema is created or updated.
  """
  @callback cast!(term(), otps :: map(), metadata()) :: term()

  @doc """
  Converts the FeebDB value to a SQLite value.

  For example, for the `:atom` type we will stringify the atom and store it as a string (`TEXT`) in
  the database. A similar stringification would happen for the `:datetime_utc` and `:map` types.

  `dump!/2` is called by `Schema.dump/2`, which is triggered any time we perform a SQL query.
  """
  @callback dump!(term(), opts :: map(), metadata()) :: term()

  @doc """
  Converts the SQLite value to a FeebDB value.

  For example, a field whose FeebDB type is `:atom` in the schema will be converted from `"foo"`
  into `:foo`. Similarly, a `:datetime_utc` or `:map` would be converted from `TEXT` to the
  corresponding values.

  `load!/2` is called by `Schema.from_row/3`, which is triggered every time we need to convert a
  raw SQLite row into a FeebDB schema.
  """
  @callback load!(term(), opts :: map(), metadata()) :: term()
end
