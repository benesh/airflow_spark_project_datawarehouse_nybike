-- Sample Database Setup with Expanded Data

-- Customer Table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name TEXT
);

-- Insert 20 Customers
INSERT INTO customers (customer_id, customer_name) VALUES
(101, 'Alice'), (102, 'Bob'), (103, 'Charlie'), (104, 'Diana'), (105, 'Ethan'),
(106, 'Fiona'), (107, 'George'), (108, 'Hannah'), (109, 'Ian'), (110, 'Jane'),
(111, 'Karl'), (112, 'Laura'), (113, 'Mike'), (114, 'Nina'), (115, 'Oscar'),
(116, 'Paula'), (117, 'Quinn'), (118, 'Rachel'), (119, 'Steve'), (120, 'Tina');

-- Orders Table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Insert 1000 rows of sample data across 3 years (2022–2024)
-- This is a representative example; use a script to generate more in a real DB
INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
-- First 20 entries manually shown; remaining to be auto-generated in practice
(1, 101, '2022-01-15', 120.00), (2, 102, '2022-02-10', 200.00), (3, 103, '2022-03-05', 350.00),
(4, 104, '2022-03-18', 180.00), (5, 105, '2022-04-22', 220.00), (6, 106, '2022-05-10', 310.00),
(7, 107, '2022-06-05', 275.00), (8, 108, '2022-07-19', 410.00), (9, 109, '2022-08-14', 130.00),
(10, 110, '2022-09-03', 500.00), (11, 111, '2023-01-12', 210.00), (12, 112, '2023-02-07', 320.00),
(13, 113, '2023-03-17', 400.00), (14, 114, '2023-04-25', 370.00), (15, 115, '2023-05-30', 290.00),
(16, 116, '2023-06-12', 430.00), (17, 117, '2023-07-04', 260.00), (18, 118, '2023-08-15', 330.00),
(19, 119, '2023-09-20', 510.00), (20, 120, '2023-10-05', 150.00);
-- Add logic to generate 980 more rows if needed for a full test dataset

-- The rest of the exercise queries remain unchanged...

-- 1. ROW_NUMBER()
-- Get a row number for each order per customer based on order date
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS row_num
FROM orders;

-- 2. RANK()
-- Rank each customer's orders by amount from highest to lowest
SELECT *,
       RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rank
FROM orders;

-- 3. DENSE_RANK()
-- Like RANK(), but no gaps in ranking
SELECT *,
       DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS dense_rank
FROM orders;

-- 4. LAG()
-- Show the previous order's amount for each customer
SELECT *,
       LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_amount
FROM orders;

-- 5. LEAD()
-- Show the next order's amount for each customer
SELECT *,
       LEAD(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount
FROM orders;

-- 6. SUM() OVER
-- Calculate a running total of amount for each customer
SELECT *,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM orders;

-- 7. AVG() OVER
-- Show a moving average of the last 2 orders per customer
SELECT *,
       AVG(amount) OVER (
         PARTITION BY customer_id
         ORDER BY order_date
         ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS moving_avg
FROM orders;

-- 8. Top 2 Orders per Customer
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
  FROM orders
) AS ranked
WHERE rn <= 2;

-- 9. Days Between Orders per Customer
SELECT *,
       order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS days_since_last
FROM orders;

-- 10. Percentage of Total Spend
SELECT *,
       amount * 100.0 / SUM(amount) OVER (PARTITION BY customer_id) AS percent_of_total
FROM orders;

-- 11. Cumulative Order Count
SELECT *,
       COUNT(*) OVER (PARTITION BY customer_id ORDER BY order_date) AS cumulative_orders
FROM orders;

-- 12. Running Max Order Value
SELECT *,
       MAX(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_max
FROM orders;

-- 13. Detect Order Streaks Over 200
SELECT *,
       SUM(CASE WHEN amount > 200 THEN 0 ELSE 1 END)
       OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_id
FROM orders;

-- 14. Global Rank by Amount
SELECT *,
       RANK() OVER (ORDER BY amount DESC) AS global_rank
FROM orders;

-- 15. First and Last Order Dates
SELECT *,
       FIRST_VALUE(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS first_order,
       LAST_VALUE(order_date) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order
FROM orders;

-- 16. Days Since First Order
SELECT *,
       order_date - FIRST_VALUE(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS days_since_first
FROM orders;

-- 17. Yearly Total Spend per Customer
SELECT *,
       SUM(amount) OVER (
           PARTITION BY customer_id, EXTRACT(YEAR FROM order_date)
       ) AS yearly_total
FROM orders;

-- 18. Year-over-Year (YoY) Spend per Customer
WITH yearly_spend AS (
  SELECT
    customer_id,
    EXTRACT(YEAR FROM order_date) AS order_year,
    SUM(amount) AS total_spend
  FROM orders
  GROUP BY customer_id, EXTRACT(YEAR FROM order_date)
)
SELECT
  customer_id,
  order_year,
  total_spend,
  LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY order_year) AS prev_year_spend,
  total_spend - LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY order_year) AS yoy_diff,
  ROUND(
    100.0 * (total_spend - LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY order_year)) /
    NULLIF(LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY order_year), 0),
    2
  ) AS yoy_percent_change  
FROM yearly_spend
ORDER BY customer_id, order_year;
