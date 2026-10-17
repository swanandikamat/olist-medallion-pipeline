-- Databricks notebook source
show tables

-- COMMAND ----------

SELECT *
FROM gold_daily_sales
ORDER BY order_date;

-- COMMAND ----------

SELECT
SUM(daily_revenue) AS total_revenue
FROM gold_daily_sales;

-- COMMAND ----------

SELECT
ROUND(AVG(daily_revenue),2) AS average_daily_sales
FROM gold_daily_sales;

-- COMMAND ----------

SELECT *
FROM gold_daily_sales
ORDER BY daily_revenue DESC
LIMIT 1;

-- COMMAND ----------

SELECT *
FROM gold_daily_sales
ORDER BY daily_revenue
LIMIT 1;

-- COMMAND ----------

SELECT *
FROM gold_monthly_sales
ORDER BY month;

-- COMMAND ----------

SELECT *
FROM gold_monthly_sales
ORDER BY monthly_revenue DESC
LIMIT 1;

-- COMMAND ----------

DESCRIBE gold_top_customers

-- COMMAND ----------

SELECT
customer_id,
total_spent
FROM gold_top_customers
ORDER BY total_spent DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM gold_city_sales
ORDER BY city_revenue DESC;

-- COMMAND ----------

SELECT
customer_city,
city_revenue
FROM gold_city_sales
ORDER BY city_revenue DESC
LIMIT 5;

-- COMMAND ----------

SELECT *
FROM gold_category_sales
ORDER BY revenue DESC;

-- COMMAND ----------

SELECT *
FROM gold_category_sales
ORDER BY revenue DESC
LIMIT 1;

-- COMMAND ----------

SELECT *
FROM gold_product_sales
ORDER BY revenue DESC;

-- COMMAND ----------

SELECT
product_id,
quantity_sold
FROM gold_product_sales
ORDER BY quantity_sold DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM gold_dashboard_summary;

-- COMMAND ----------

CREATE OR REPLACE VIEW vw_daily_sales AS

SELECT *

FROM gold_daily_sales;

-- COMMAND ----------

SELECT *

FROM vw_daily_sales;

-- COMMAND ----------

SELECT

order_date,

daily_revenue,

LAG(daily_revenue)
OVER(ORDER BY order_date)
AS previous_day

FROM gold_daily_sales;

-- COMMAND ----------

SELECT

order_date,

daily_revenue,

LAG(daily_revenue)
OVER(ORDER BY order_date)
AS previous_day,

ROUND(

(
daily_revenue-
LAG(daily_revenue)
OVER(ORDER BY order_date)

)

/

LAG(daily_revenue)
OVER(ORDER BY order_date)

*100

,2)

AS growth_percent

FROM gold_daily_sales;

-- COMMAND ----------

SELECT

order_date,

daily_revenue,

SUM(daily_revenue)

OVER(

ORDER BY order_date

)

AS cumulative_sales

FROM gold_daily_sales;

-- COMMAND ----------

SELECT

customer_id,

total_spent,

RANK()

OVER(

ORDER BY total_spent DESC

)

AS customer_rank

FROM gold_top_customers;

-- COMMAND ----------

SELECT

customer_id,

total_spent,

DENSE_RANK()

OVER(

ORDER BY total_spent DESC

)

AS rank

FROM gold_top_customers;

-- COMMAND ----------

SELECT

customer_id,

ROW_NUMBER()

OVER(

ORDER BY total_spent DESC

)

AS row_num

FROM gold_top_customers;

-- COMMAND ----------

SELECT

product_id,

revenue,

ROUND(

100*revenue/

SUM(revenue)

OVER()

,2)

AS contribution

FROM gold_product_sales

ORDER BY contribution DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW executive_dashboard AS

SELECT

*

FROM gold_dashboard_summary;

-- COMMAND ----------

EXPLAIN

SELECT *

FROM gold_product_sales

WHERE revenue>50000;

-- COMMAND ----------

DESCRIBE HISTORY gold_product_sales;