defmodule Feeb.DB.Schema do
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

          # If the datatype module implements `overwrite_opts`, call it
          opts =
            try do
              datatype_module.overwrite_opts(opts, mod, {__MODULE__, column_name})
            rescue
              UndefinedFunctionError ->
                opts
            end

          {column_name, {datatype_module, opts, mod}}
        end)

      normalized_schema = Map.new(ordered_schema)

      {reverse_sorted_cols, virtual_cols} =
        Enum.reduce(ordered_schema, {[], []}, fn {col, {_, opts, _}}, {acc_sorted, acc_virtual} ->
          if opts[:virtual] do
            {acc_sorted, [col | acc_virtual]}
          else
            {[col | acc_sorted], acc_virtual}
          end
        end)

      sorted_cols = Enum.reverse(reverse_sorted_cols)

      modded_fields =
        normalized_schema
        |> Enum.filter(fn {_, {_, _, mod}} -> not is_nil(mod) end)
        |> Map.new()
        |> Map.keys()

      after_read_fields =
        Enum.reduce(normalized_schema, [], fn {field, {_, opts, _}}, acc ->
          if after_read = opts[:after_read] do
            [{field, after_read} | acc]
          else
            acc
          end
        end)

      @schema normalized_schema
      @modded_fields modded_fields
      @sorted_cols sorted_cols
      @virtual_cols virtual_cols
      @after_read_fields after_read_fields

      if is_nil(Module.get_attribute(__MODULE__, :derived_fields)) do
        @derived_fields []
      end

      defstruct Map.keys(@schema) ++ unquote(meta_keys)

      # TODO: Inline?
      # TODO: This may be simplified if I register thsi attr as persist: true
      def __schema__, do: @schema
      def __cols__, do: @sorted_cols
      def __virtual_cols__, do: @virtual_cols
      def __after_read_fields__, do: @after_read_fields
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
          {field, Feeb.DB.Schema.cast_value!(__MODULE__, schema, field, value)}
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
              type_module.cast!(v, opts, {struct.__struct__, field})

            :noop ->
              type_module.cast!(nil, opts, {struct.__struct__, field})
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
        Map.put(acc, target_field, cast_value!(struct.__struct__, schema, target_field, value))
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
            [{field, type_module.cast!(v, opts, {struct.__struct__, field})} | acc]

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
    |> Map.put(target_field, cast_value!(struct.__struct__, schema, target_field, value))
  end

  def dump(struct, field) do
    schema = struct.__struct__.__schema__()

    {type_module, opts, _mod} = Map.fetch!(schema, field)

    try do
      struct
      |> Map.fetch!(field)
      |> type_module.dump!(opts, {struct.__struct__, field})
    rescue
      FunctionClauseError ->
        if is_nil(Map.fetch!(struct, field)) and not Map.has_key?(opts, :nullable),
          do: raise("#{struct.__struct__}.#{field} is null but it isn't supposed to be")
    end
  end

  def from_row(model, fields, row) do
    schema = model.__schema__()
    table_fields = model.__cols__()
    virtual_fields = model.__virtual_cols__()
    after_read_fields = model.__after_read_fields__()
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

    values =
      fields_to_populate
      |> Enum.zip(row)
      |> Enum.map(fn {field, raw_value} ->
        {type_module, opts, _mod} = Map.fetch!(schema, field)
        {field, type_module.load!(raw_value, opts, {model, field})}
      end)

    model
    |> struct(values)
    |> Map.put(:__meta__, %{origin: :db})
    |> add_missing_values(table_fields, fields_to_populate)
    |> add_virtual_fields(virtual_fields, schema)
    |> trigger_after_read_callbacks(after_read_fields)
  end

  def cast_value!(schema_mod, schema, field, raw_value) do
    {type_module, opts, _} = Map.fetch!(schema, field)
    type_module.cast!(raw_value, opts, {schema_mod, field})
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

  defp add_virtual_fields(struct, [], _), do: struct

  defp add_virtual_fields(struct, virtual_fields, schema) do
    repo_config = Process.get(:repo_config)

    Enum.reduce(virtual_fields, struct, fn field_name, acc ->
      {_, %{virtual: virtual_fn}, _} = Map.fetch!(schema, field_name)

      value = apply(struct.__struct__, virtual_fn, [struct, repo_config])
      Map.put(acc, field_name, value)
    end)
  end

  defp trigger_after_read_callbacks(struct, []), do: struct

  defp trigger_after_read_callbacks(struct, after_read_fields) do
    Enum.reduce(after_read_fields, struct, fn {field, callback}, acc ->
      old_value = Map.get(struct, field)
      new_value = apply(struct.__struct__, callback, [old_value, struct])
      Map.put(acc, field, new_value)
    end)
  end
end
