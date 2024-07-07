CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  external_id TEXT,
  username TEXT,
  email TEXT,
  password TEXT,
  inserted_at TEXT
) STRICT;

CREATE UNIQUE INDEX users_external_id_idx ON users (external_id);
CREATE UNIQUE INDEX users_email_idx ON users (email);
CREATE UNIQUE INDEX users_username_idx ON users (username);
