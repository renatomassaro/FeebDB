CREATE TABLE crm_contact_tags (
  contact_id INTEGER,
  tag_id INTEGER,
  tag_name TEXT,
  inserted_at TEXT,
  PRIMARY KEY (contact_id, tag_id)
) STRICT, WITHOUT ROWID;

