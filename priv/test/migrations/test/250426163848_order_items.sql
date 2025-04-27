CREATE TABLE order_items (
  order_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  price INTEGER,
  inserted_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (order_id, product_id)
) STRICT;
