-- NOTE: All columns whose name is only the type (e.g. `boolean` vs `boolean_$something`) are
-- non-nullable. However, I'm not adding the `NOT NULL` constraint because I want to make sure this
-- constraint is enforced at the application level.

CREATE TABLE all_types (
  boolean INTEGER,
  boolean_nullable INTEGER,
  string TEXT,
  string_nullable TEXT,
  integer INTEGER,
  integer_nullable INTEGER
) STRICT;
