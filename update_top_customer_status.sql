CREATE OR REPLACE FUNCTION update_is_top_customer()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE customers
  SET is_top_customer = (
    -- Condition 1: Total order amount > 20,000
    (SELECT SUM(total_amount) > 20000
     FROM orders
     WHERE customer_id = NEW.customer_id)
    OR
    -- Condition 2: At least 3 orders placed on different days
    (SELECT COUNT(DISTINCT DATE(order_date)) >= 3
     FROM orders
     WHERE customer_id = NEW.customer_id)
  )
  WHERE customers.customer_id = NEW.customer_id;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
