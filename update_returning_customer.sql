CREATE OR REPLACE FUNCTION update_is_returning()
RETURNS TRIGGER AS $$
BEGIN
  -- Check if the customer has at least two orders on different days
  IF EXISTS (
    SELECT 1
    FROM orders
    WHERE customer_id = NEW.customer_id
    GROUP BY DATE(order_date)
    HAVING COUNT(DISTINCT DATE(order_date)) >= 2
  ) THEN
    -- Update the is_returning field in the customers table
    UPDATE customers
    SET is_returning_customer = TRUE
    WHERE customers.customer_id = NEW.customer_id;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;