-- Top customer status
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


-- Trigger for top customers
CREATE TRIGGER update_top_customer_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION update_is_top_customer();

----------------------------------------------------------------------------------------------------------------

-- Returning customer
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


-- Trigger for returning customer
CREATE TRIGGER check_returning_customer
AFTER INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION update_is_returning();

----------------------------------------------------------------------------------------------------------------

-- Visit count
CREATE OR REPLACE FUNCTION update_visit_count()
RETURNS TRIGGER AS $$
BEGIN
  -- Calculate the number of distinct order dates for the customer
  UPDATE customers
  SET visit_counts = (
    SELECT COUNT(DISTINCT DATE(order_date))
    FROM orders
    WHERE customer_id = NEW.customer_id
  )
  WHERE customers.customer_id = NEW.customer_id;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Trigger for visit count
CREATE OR REPLACE TRIGGER update_visit_count_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION update_visit_count();

----------------------------------------------------------------------------------------------------------------
-- Feedback constraints
ALTER TABLE feedback
ADD CONSTRAINT chk_food_review_range
CHECK (food_review >= 0 AND food_review <= 4),
ADD CONSTRAINT chk_service_range
CHECK (service >= 0 AND service <= 4),
ADD CONSTRAINT chk_cleanliness_range
CHECK (cleanliness >= 0 AND cleanliness <= 4),
ADD CONSTRAINT chk_atmosphere_range
CHECK (atmosphere >= 0 AND atmosphere <= 4),
ADD CONSTRAINT chk_value_range
CHECK (value >= 0 AND value <= 4),
ADD CONSTRAINT chk_overall_experience_range
CHECK (overall_experience >= 0 AND overall_experience <= 4);

-----------------------------------------------------------------------------------------------------------------
-- Backfill for is_returning_customer
UPDATE customers
SET is_returning_customer = (
  SELECT COUNT(*) >= 2
  FROM orders
  WHERE orders.customer_id = customers.customer_id
);

-- Backfill for visit count
UPDATE customers
SET visit_counts = (
  SELECT COUNT(DISTINCT DATE(order_date))
  FROM orders
  WHERE orders.customer_id = customers.customer_id
);

-- Backfill for top customer
-- Update all customers to reflect top customer status based on existing data
UPDATE customers
SET is_top_customer = (
  -- Condition 1: Total order amount > 20000
  (SELECT SUM(total_amount) > 20000
   FROM orders
   WHERE orders.customer_id = customers.customer_id)
  OR
  -- Condition 2: At least 3 orders placed on different days
  (SELECT COUNT(DISTINCT DATE(order_date)) >= 3
   FROM orders
   WHERE orders.customer_id = customers.customer_id)
);

-----------------------------------------------------------------------------------------------------------------
-- Add unique constraint
ALTER TABLE feedback
ADD CONSTRAINT unique_customer_id UNIQUE (customer_id);