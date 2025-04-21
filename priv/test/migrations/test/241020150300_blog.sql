  CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    title TEXT,
    body TEXT,
    is_draft INTEGER,
    inserted_at TEXT,
    updated_at TEXT
  ) STRICT;
