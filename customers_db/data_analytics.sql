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
SELECT COUNT(*) AS returning_customers
FROM customers
WHERE is_returning_customer = TRUE;

-- 4. % Returning
-- (Calculates the percentage of returning customers among all customers)
SELECT 
    ROUND(
        (COUNT(*) FILTER (WHERE is_returning_customer = TRUE) * 100.0 / COUNT(*)),
        2
    ) AS returning_percentage
FROM customers;

-- 5. "Top" Customers
SELECT COUNT(*) AS top_customers
FROM customers
WHERE is_top_customer = TRUE;

-- 6. Avg visits count
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
WHERE order_type = 'TakeAway';

-- 11. Avg spent Dine-In
SELECT AVG(total_amount) AS avg_spent_dine_in
FROM orders
WHERE order_type = 'DineIn';


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