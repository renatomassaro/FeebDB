-- The `CREATE TABLE` below works just fine
CREATE TABLE users (
  id INTEGER PRIMARY KEY
) STRICT;

-- But this won't work, after all there's no `username` in the table
CREATE INDEX users_username_idx ON users (username);
