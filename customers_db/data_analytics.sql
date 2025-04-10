------------------------------------------------
-- CUSTOMERS
------------------------------------------------

-- 1. Overall (unique phone number)
SELECT COUNT(DISTINCT phone_number) AS overall_unique_phone_numbers
FROM customers
WHERE phone_number IS NOT NULL
  AND phone_number <> '';

-- 2. VIP
SELECT COUNT(*) AS vip_customers
FROM customers
WHERE "is_VIP" = TRUE;

-- 3. Returning
-- First, check for mismatches:
SELECT c.customer_id, c.is_returning_customer AS stored_flag, actual.should_be_returning
FROM customers c
JOIN (
  SELECT customer_id,
         COUNT(DISTINCT DATE(order_date)) >= 2 AS should_be_returning
  FROM orders
  GROUP BY customer_id
) actual ON c.customer_id = actual.customer_id
WHERE c.is_returning_customer IS DISTINCT FROM actual.should_be_returning;


-- If all good, run:
SELECT COUNT(*) AS returning_customers
FROM customers
WHERE is_returning_customer = TRUE;


-- 4. "Top" Customers
-- First, check for mismatches:
SELECT c.customer_id, c.is_top_customer AS stored_flag, actual.should_be_top
FROM customers c
JOIN (
  SELECT customer_id,
         (SUM(total_amount) > 20000 OR COUNT(DISTINCT DATE(order_date)) >= 3) AS should_be_top
  FROM orders
  GROUP BY customer_id
) actual ON c.customer_id = actual.customer_id
WHERE c.is_top_customer IS DISTINCT FROM actual.should_be_top;


-- If all good, run:
SELECT COUNT(*) AS top_customers
FROM customers
WHERE is_top_customer = TRUE;

-- 5. Avg visits count

-- First, check for mismatches:
SELECT c.customer_id, c.visit_counts AS stored_count, actual.actual_count
FROM customers c
JOIN (
  SELECT customer_id, COUNT(DISTINCT DATE(order_date)) AS actual_count
  FROM orders
  GROUP BY customer_id
) actual ON c.customer_id = actual.customer_id
WHERE c.visit_counts IS DISTINCT FROM actual.actual_count;

-- If all good, run:
SELECT AVG(visit_counts) AS avg_visit_counts
FROM customers
WHERE visit_counts IS NOT NULL;


------------------------------------------------
-- ORDERS
------------------------------------------------

-- 7. Total
SELECT COUNT(*) AS total_orders
FROM orders;

-- 8. Avg amount spent
SELECT AVG(total_amount) AS avg_amount_spent
FROM orders;

-- 9. Avg spent Delivery
SELECT AVG(total_amount) AS avg_spent_delivery
FROM orders
WHERE order_type = 'Delivery';

-- 10. Avg spent Take-Away
SELECT AVG(total_amount) AS avg_spent_take_away
FROM orders
WHERE order_type = 'Take away';

-- 11. Avg spent Dine-In
SELECT AVG(total_amount) AS avg_spent_dine_in
FROM orders
WHERE order_type = 'Dine-In';


------------------------------------------------
-- FEEDBACK
------------------------------------------------

-- 12. Entries
SELECT COUNT(*) AS total_feedback_entries
FROM feedback;

-- 13. Avg Overall
SELECT AVG(overall_experience) AS avg_overall
FROM feedback;

-- 14. Avg Food
SELECT AVG(food_review) AS avg_food
FROM feedback;

-- 15. Avg Service
SELECT AVG(service) AS avg_service
FROM feedback;

-- 16. Avg Cleanliness
SELECT AVG(cleanliness) AS avg_cleanliness
FROM feedback;

-- 17. Avg Atmosphere
SELECT AVG(atmosphere) AS avg_atmosphere
FROM feedback;

-- 18. Avg Value
SELECT AVG(value) AS avg_value
FROM feedback;

-- 18. Where did they hear about us
SELECT where_did_they_hear_about_us, COUNT(*) AS count
FROM feedback
GROUP BY where_did_they_hear_about_us;