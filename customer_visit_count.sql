CREATE OR REPLACE FUNCTION update_visit_count()
RETURNS TRIGGER AS $$
BEGIN
  -- Calculate the number of distinct order dates for the customer
  UPDATE customers
  SET visit_count = (
    SELECT COUNT(DISTINCT DATE(order_date))
    FROM orders
    WHERE customer_id = NEW.customer_id
  )
  WHERE customers.customer_id = NEW.customer_id;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;