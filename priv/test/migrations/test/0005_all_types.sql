-- NOTE: All columns whose name is only the type (e.g. `boolean` vs `boolean_$something`) are
-- non-nullable. However, I'm not adding the `NOT NULL` constraint because I want to make sure this
-- constraint is enforced at the application level.

CREATE TABLE all_types (
  boolean INTEGER,
  boolean_nullable INTEGER,
  string TEXT,
  string_nullable TEXT,
  integer INTEGER,
  integer_nullable INTEGER,
  atom TEXT,
  atom_nullable TEXT,
  uuid TEXT,
  uuid_nullable TEXT,
  datetime_utc TEXT,
  datetime_utc_nullable TEXT,
  datetime_utc_precision_second TEXT,
  datetime_utc_precision_millisecond TEXT,
  datetime_utc_precision_microsecond TEXT,
  datetime_utc_precision_default TEXT,
  map TEXT,
  map_nullable TEXT,
  map_keys_atom TEXT,
  map_keys_safe_atom TEXT,
  map_keys_string TEXT,
  map_keys_default TEXT
) STRICT;
