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
AFTER INSERT OR UPDATE ON orders
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
-- -- Add unique constraint feedback
-- ALTER TABLE feedback
-- ADD CONSTRAINT unique_customer_id UNIQUE (customer_id);


-- Add unique constraint receipt id
ALTER TABLE orders
ADD CONSTRAINT unique_receipt_id UNIQUE (receipt_id);

-----------------------------------------------------------------------------------------------------------------
-- Add created_at and modified_at columns to customers table
ALTER TABLE customers
ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
ADD COLUMN modified_at TIMESTAMP WITH TIME ZONE DEFAULT now();

-- Backfill created_at and modified_at columns
UPDATE customers
SET created_at = now(),
    modified_at = now()
WHERE created_at IS NULL OR modified_at IS NULL;

-- Create trigger to automatically update modified_at column on updates
CREATE OR REPLACE FUNCTION update_modified_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.modified_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_modified_at
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION update_modified_at();

-----------------------------------------------------------------------------------------------------------------
-- Make receipt_id unique
WITH batch AS (
  SELECT order_id  
  FROM orders
  WHERE receipt_id_test IS NULL
  LIMIT 1000
)
UPDATE orders o
SET receipt_id_test = 
    receipt_id || '_' || TO_CHAR(order_date, 'DD_MM_YYYY')
FROM batch
WHERE o.order_id = batch.order_id;

-- Copy receipt_id_test to original receipt_id column
UPDATE orders
SET receipt_id = receipt_id_test
WHERE receipt_id_test IS NOT NULL
  AND receipt_id_test <> '';
