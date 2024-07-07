CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  event_mod TEXT,
  handler_mod TEXT,
  handler_fun TEXT,
  args TEXT,
  inserted_at TEXT,
  scheduled_at TEXT,
  failed_attempts TEXT,
  attempts INTEGER,
  max_attempts INTEGER
) STRICT;

CREATE TABLE events_archive (
  id INTEGER PRIMARY KEY,
  event_mod TEXT,
  handler_mod TEXT,
  handler_fun TEXT,
  args TEXT,
  result TEXT,
  completed_at TEXT,
  failed_attempts TEXT,
  attempts INTEGER
) STRICT;

CREATE TABLE events_dlq (
  id INTEGER PRIMARY KEY,
  event_mod TEXT,
  handler_mod TEXT,
  handler_fun TEXT,
  args TEXT,
  failed_attempts TEXT,
  dlq_at TEXT,
  attempts INTEGER
) STRICT;

