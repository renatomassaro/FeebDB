-- :set_quantity_returning_order_id
UPDATE order_items SET quantity = ? WHERE order_id = ? RETURNING order_id;
