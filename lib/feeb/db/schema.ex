defmodule Feeb.DB.Schema do
  @env Mix.env()

  defmacro __using__(_opts) do
    quote do
      alias unquote(__MODULE__)

      @before_compile unquote(__MODULE__)
      @after_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(_env) do
    meta_keys = [:__meta__, :__private__]

    quote do
      true = not is_nil(@schema) || raise "Missing @schema for #{__MODULE__}\n"

      ordered_schema =
        @schema
        |> Enum.map(fn {column_name, v} ->
          {datatype, opts, mod} =
            case v do
              {dt, opts, mod: mod} when is_list(opts) ->
                {dt, Enum.into(opts, %{}), mod}

              {dt, mod: mod} ->
                {dt, %{}, mod}

              {dt, opts} when is_list(opts) ->
                {dt, Enum.into(opts, %{}), nil}

              {dt, opt} when is_atom(opt) ->
                {dt, Map.put(%{}, opt, true), nil}

              dt when is_atom(dt) ->
                {dt, %{}, nil}
            end

          # Break compilation dependency
          type_mod = :"Elixir.Feeb.DB.Type"
          datatype_module = type_mod.get_module(datatype)

          # Break compilation dependency
          mod_module = :"Elixir.Feeb.DB.Mod"
          mod = mod_module.get_module(mod)

          {column_name, {datatype_module, opts, mod}}
        end)

      normalized_schema = Map.new(ordered_schema)

      sorted_keys = Enum.map(ordered_schema, fn {col, _} -> col end)

      if is_nil(Module.get_attribute(__MODULE__, :derived_fields)) do
        @derived_fields []
      end

      modded_fields =
        normalized_schema
        |> Enum.filter(fn {_, {_, _, mod}} -> not is_nil(mod) end)
        |> Map.new()
        |> Map.keys()

      @schema normalized_schema
      @modded_fields modded_fields
      @sorted_keys sorted_keys

      defstruct Map.keys(@schema) ++ unquote(meta_keys)

      # TODO: Inline?
      # TODO: This may be simplified if I register thsi attr as persist: true
      def __schema__, do: @schema
      def __cols__, do: @sorted_keys
      def __table__, do: @table
      def __context__, do: @context
      def __modded_fields__, do: @modded_fields
      def __derived_fields__, do: @derived_fields
    end
  end

  defmacro __after_compile__(env, _module) do
    # santiy_checks()
    # TODO:
    # - Quais checks?
    # - Que as env vars estao setadas

    assert_env = fn var ->
      if is_nil(Module.get_attribute(env.module, var)),
        do: raise("Missing @#{var} attribute in #{env.module}")
    end

    assert_env.(:context)
    assert_env.(:table)
    assert_env.(:schema)
  end

  defmacro cast(args, target_fields \\ unquote(:all)) do
    quote do
      meta = %{
        valid?: true,
        origin: :undefined,
        target: unquote(target_fields)
      }

      schema = __schema__()

      new_args =
        Enum.map(unquote(args), fn {field, value} ->
          {field, Feeb.DB.Schema.cast_value!(schema, field, value)}
        end)

      # TODO: Validate that `args` matches `target_fields` exactly

      # TODO: Validate that required fields are present and are not nil

      struct(__MODULE__, new_args)
      |> Map.put(:__meta__, meta)
      |> Map.put(:__private__, %{})

      # TODO: Warn if excess (unused/incorrect) args were passed
    end
  end

  def create(struct) do
    schema = struct.__struct__.__schema__()
    modded_fields = struct.__struct__.__modded_fields__()
    creation_time = DateTime.utc_now()

    modded_args =
      modded_fields
      |> Enum.map(fn field ->
        {type_module, opts, mod} = Map.fetch!(schema, field)
        opts = Map.put(opts, :creation_time, creation_time)

        value =
          case mod.on_create(struct, field, opts) do
            {:ok, v} ->
              type_module.cast!(v, opts)

            :noop ->
              type_module.cast!(nil, opts)
          end

        {field, value}
      end)
      |> Map.new()

    struct
    |> Map.put(:__meta__, Map.put(struct.__meta__, :origin, :application))
    |> Kernel.struct(modded_args)
  end

  def update(args_map, %_{} = struct),
    do: update(struct, args_map)

  def update(%_{} = struct, args_map) when is_map(args_map) do
    if struct.__meta__.origin != :db,
      do: raise("Can't update an application-originated struct: #{inspect(struct)}")

    schema = struct.__struct__.__schema__()
    current_target = struct.__meta__[:target]
    update_time = DateTime.utc_now()

    new_target =
      if is_nil(current_target) do
        Map.keys(args_map)
      else
        current_target ++ Map.keys(args_map)
      end

    pre_mod_struct =
      args_map
      |> Enum.reduce(struct, fn {target_field, value}, acc ->
        Map.put(acc, target_field, cast_value!(schema, target_field, value))
      end)
      |> Map.put(:__meta__, Map.put(struct.__meta__, :target, new_target))

    # Now we will apply the mods (on_update hooks) into the new struct
    modded_changes =
      struct.__struct__.__modded_fields__()
      |> Enum.reduce([], fn field, acc ->
        {type_module, opts, mod} = Map.fetch!(schema, field)
        opts = Map.put(opts, :update_time, update_time)

        case mod.on_update(struct, field, opts) do
          {:ok, v} ->
            type_module.cast!(v, opts)
            [{field, type_module.cast!(v, opts)} | acc]

          :noop ->
            acc
        end
      end)

    final_target =
      modded_changes
      |> Enum.reduce(new_target, fn {field, _}, acc ->
        [field | acc]
      end)
      |> Enum.uniq()

    modded_changes
    |> Enum.reduce(pre_mod_struct, fn {modded_field, modded_value}, acc ->
      Map.put(acc, modded_field, modded_value)
    end)
    |> Map.put(:__meta__, Map.put(struct.__meta__, :target, final_target))
  end

  def update(%_{} = struct, target_field, value) do
    schema = struct.__struct__.__schema__()
    current_target = struct.__meta__[:target]

    new_target =
      if is_nil(current_target) do
        [target_field]
      else
        [target_field | current_target]
      end

    struct
    |> Map.put(:__meta__, Map.put(struct.__meta__, :target, new_target))
    |> Map.put(target_field, cast_value!(schema, target_field, value))
  end

  def dump(struct, field) do
    schema = struct.__struct__.__schema__()

    {type_module, opts, _mod} = Map.fetch!(schema, field)

    try do
      struct
      |> Map.fetch!(field)
      |> type_module.dump!(opts)
    rescue
      FunctionClauseError ->
        if is_nil(Map.fetch!(struct, field)) and
             not Map.has_key?(opts, :nullable),
           do: raise("#{struct.__struct__}.#{field} is null but it isn't supposed to be")
    end
  end

  def from_row(model, fields, row) do
    table_fields = get_table_fields(model)
    fields_to_populate = if fields == [:*], do: table_fields, else: fields

    # TODO: Test this a lot...
    fields_to_populate =
      if length(fields_to_populate) == length(row) do
        fields_to_populate
      else
        extended_fields = Enum.uniq(model.__derived_fields__() ++ fields)

        if length(extended_fields) == length(row) do
          extended_fields
        else
          fp = fields_to_populate
          ef = extended_fields

          details = "\n\nRow: #{inspect(row)}; \nfields: #{inspect(fp)} or #{inspect(ef)}"

          raise "Row results do not match with fields to populate: #{details}"
        end
      end

    if length(fields_to_populate) != length(row) do
      details = "\n\nRow: #{inspect(row)}; \nfields: #{inspect(fields_to_populate)}"

      raise "Row results do not match with fields to populate: #{details}"
    end

    schema = model.__schema__()

    values =
      fields_to_populate
      |> Enum.zip(row)
      |> Enum.map(fn {field, v} ->
        {type_module, opts, _mod} = Map.fetch!(schema, field)
        {field, type_module.load!(v, opts)}
      end)

    model
    |> struct(values)
    |> Map.put(:__meta__, %{origin: :db})
    |> add_missing_values(table_fields, fields_to_populate)
  end

  def cast_value!(schema, field, raw_value) do
    {type_module, opts, _} = Map.fetch!(schema, field)
    type_module.cast!(raw_value, opts)
  end

  def validate(%_{} = struct, field) when is_atom(field) do
    mod = Module.concat(struct.__struct__, Validator)
    fun_name = :"validate_#{field}"

    case apply(mod, fun_name, [Map.get(struct, field)]) do
      true ->
        :ok

      false ->
        {:error, :invalid_input}
        # {:ok, _} -> :ok
        # {:error, _} = e -> e
        # :error -> {:error, :invalid_input}
    end
  end

  def validate_fields(struct, fields) when is_list(fields) do
    Enum.reduce(fields, struct, fn field, acc ->
      case validate(acc, field) do
        :ok -> acc
        error -> add_error(acc, field, error)
      end
    end)
  end

  def add_error(schema, field, {:error, error}),
    do: add_error(schema, field, error)

  def add_error(schema, field, error) do
    new_meta =
      schema.__meta__
      |> Map.put(:valid?, false)
      |> Map.put(:errors, [{field, error} | schema.__meta__[:errors] || []])

    %{schema | __meta__: new_meta}
  end

  defp get_table_fields(model) do
    if @env != :test do
      :persistent_term.get({:db_table_fields, model})
    else
      # Test schemas may not exist in the PT created at Boot time
      try do
        :persistent_term.get({:db_table_fields, model})
      rescue
        ArgumentError ->
          model.__cols__()
          raise "Bad bad test"
      end
    end
  end

  def set_private(%{__private__: private} = schema, k, v) do
    %{schema | __private__: Map.put(private, k, v)}
  end

  def get_private(%{__private__: private}, k), do: private[k]
  def get_private!(%{__private__: private}, k), do: Map.fetch!(private, k)

  defp add_missing_values(struct, f, f), do: struct

  defp add_missing_values(struct, all_fields, added_fields) do
    not_loaded = %Feeb.DB.Value.NotLoaded{}

    values =
      Enum.map(all_fields -- added_fields, fn field ->
        {field, not_loaded}
      end)

    Kernel.struct(struct, values)
  end
end
