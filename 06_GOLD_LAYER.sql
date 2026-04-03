-- =====================================================================
-- Gold Layer: Dimensional Model and Business-Ready Data Structures
-- =====================================================================

-- Set execution context
USE DATABASE SALES_DWH;
USE SCHEMA GOLD;

-- Validate current execution context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- =====================================================================
-- Gold Layer Overview
-- =====================================================================

-- The Gold layer contains curated, business-ready datasets designed for
-- downstream analytical consumption.
--
-- This layer typically includes:
--   • Dimension tables for descriptive attributes
--   • Fact tables for measurable business events and metrics
--   • Data marts for reporting, dashboards, APIs, and BI tools

-- =====================================================================
-- Date Dimension
-- =====================================================================

-- Create a reusable date dimension to support time-based reporting and
-- analytical aggregations across day, month, quarter, and year levels.

CREATE OR REPLACE TABLE SALES_DWH.GOLD.DIM_DATE AS
WITH DATE_SERIES AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2015-01-01') AS FULL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 7305))
)
SELECT
    TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD'))                    AS DATE_KEY,
    FULL_DATE                                                    AS FULL_DATE,
    DAYOFWEEKISO(FULL_DATE)                                      AS DAY_NUMBER_OF_WEEK,
    DAYNAME(FULL_DATE)                                           AS DAY_NAME,
    DAY(FULL_DATE)                                               AS DAY_NUMBER_OF_MONTH,
    DAYOFYEAR(FULL_DATE)                                         AS DAY_NUMBER_OF_YEAR,
    WEEKISO(FULL_DATE)                                           AS WEEK_OF_YEAR,
    MONTH(FULL_DATE)                                             AS MONTH_NUMBER,
    MONTHNAME(FULL_DATE)                                         AS MONTH_NAME,
    TO_CHAR(FULL_DATE, 'YYYY-MM')                                AS YEAR_MONTH,
    QUARTER(FULL_DATE)                                           AS QUARTER_NUMBER,
    YEAR(FULL_DATE)                                              AS YEAR_NUMBER,
    CASE
        WHEN DAYOFWEEKISO(FULL_DATE) IN (6, 7) THEN 1
        ELSE 0
    END                                                          AS IS_WEEKEND,
    DATE_TRUNC('MONTH', FULL_DATE)                               AS MONTH_START_DATE,
    LAST_DAY(FULL_DATE, 'MONTH')                                 AS MONTH_END_DATE,
    DATE_TRUNC('YEAR', FULL_DATE)                                AS YEAR_START_DATE,
    LAST_DAY(FULL_DATE, 'YEAR')                                  AS YEAR_END_DATE
FROM DATE_SERIES;

-- Validate date dimension output
SELECT * FROM DIM_DATE LIMIT 5;

-- Remove duplicate or incorrectly created dimension object from other schema if present
DROP TABLE SALES_DWH.PUBLIC.DIM_DATE;

-- Validate available tables in the current schema
SHOW TABLES;

-- =====================================================================
-- Customer Dimension
-- =====================================================================

-- Preview cleansed customer source data from the Silver layer
SELECT * FROM SALES_DWH.SILVER.CUSTOMER_CLEAN LIMIT 5;

-- Create customer dimension containing descriptive customer attributes.
-- Dimension tables are primarily used to provide business context to fact data.

CREATE OR REPLACE TABLE SALES_DWH.GOLD.DIM_CUSTOMER AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CUSTOMERID) AS CUSTOMER_KEY,
    CUSTOMERID AS CUSTOMER_ID,
    PERSONID AS PERSON_ID,
    STOREID AS STORE_ID,
    TERRITORYID AS TERRITORY_ID,
    ACCOUNTNUMBER AS ACCOUNT_NUMBER,
    CASE
        WHEN CUSTOMER_TYPE = 'STORE' THEN 'STORE'
        WHEN CUSTOMER_TYPE = 'PERSON' THEN 'PERSON'
        ELSE 'UNKNOWN'
    END AS CUSTOMER_TYPE,
    MODIFIEDDATE AS SOURCE_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS ETL_INSERT_TS
FROM SALES_DWH.SILVER.CUSTOMER_CLEAN;

-- Validate customer dimension output
SELECT * FROM SALES_DWH.GOLD.DIM_CUSTOMER LIMIT 5;

-- =====================================================================
-- Product Dimension
-- =====================================================================

-- Preview cleansed product source data from the Silver layer
SELECT * FROM SALES_DWH.SILVER.PRODUCT_CLEAN LIMIT 5;

-- Create product dimension containing descriptive product attributes
-- required for business reporting and drill-down analysis.

CREATE OR REPLACE TABLE SALES_DWH.GOLD.DIM_PRODUCT AS
SELECT
    ROW_NUMBER() OVER (ORDER BY PRODUCTID) AS PRODUCT_KEY,
    PRODUCTID AS PRODUCT_ID,
    NAME AS PRODUCT_NAME,
    PRODUCTNUMBER AS PRODUCT_NUMBER,
    COLOR AS COLOR,
    SIZE AS SIZE,
    STYLE AS STYLE,
    PRODUCTMODELID AS PRODUCT_MODEL_ID,
    PRODUCTSUBCATEGORYID AS PRODUCT_SUB_CATEGORY_ID,
    STANDARDCOST AS STANDARD_COST,
    LISTPRICE AS LIST_PRICE,
    MAKEFLAG AS MAKE_FLAG,
    FINISHEDGOODSFLAG AS FINISHED_GOODS_FLAG,
    SELLSTARTDATE AS SELL_START_DATE,
    SELLENDDATE AS SELL_END_DATE,
    CURRENT_TIMESTAMP() AS ETL_INSERT_TS
FROM SALES_DWH.SILVER.PRODUCT_CLEAN;

-- Validate product dimension output
SELECT * FROM SALES_DWH.GOLD.DIM_PRODUCT;

-- =====================================================================
-- Fact Table Design
-- =====================================================================

-- Fact tables store measurable business metrics at a defined grain.
-- In this model:
--   • FACT_SALES_ORDER_HEADER has one row per sales order
--   • FACT_SALES_ORDER_DETAIL has one row per sales order line item

-- =====================================================================
-- Sales Order Header Fact
-- Grain: One row per sales order
-- =====================================================================

CREATE OR REPLACE TABLE SALES_DWH.GOLD.FACT_SALES_ORDER_HEADER AS
SELECT
    H.SALESORDERID AS SALES_ORDER_ID,

    COALESCE(C.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    COALESCE(OD.DATE_KEY, -1) AS ORDER_DATE_KEY,
    COALESCE(DD.DATE_KEY, -1) AS DUE_DATE_KEY,
    COALESCE(SD.DATE_KEY, -1) AS SHIP_DATE_KEY,

    H.REVISIONNUMBER AS REVISION_NUMBER,
    H.STATUS AS STATUS,
    H.ONLINEORDERFLAG AS ONLINE_ORDER_FLAG,
    H.SALESPERSONID AS SALESPERSON_ID,
    H.TERRITORYID AS TERRITORY_ID,
    H.BILLTOADDRESSID AS BILL_TO_ADDRESS_ID,
    H.SHIPTOADDRESSID AS SHIP_TO_ADDRESS_ID,
    H.SHIPMETHODID AS SHIP_METHOD_ID,
    H.CREDITCARDID AS CREDIT_CARD_ID,
    H.CURRENCYRATEID AS CURRENCY_RATE_ID,

    H.SUBTOTAL AS SUBTOTAL_AMOUNT,
    H.TAXAMT AS TAX_AMOUNT,
    H.FREIGHT AS FREIGHT_AMOUNT,
    H.TOTALDUE AS TOTAL_DUE_AMOUNT,

    H.MODIFIEDDATE AS SOURCE_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS ETL_INSERT_TS
FROM SALES_DWH.SILVER.SALES_ORDER_HEADER_CLEAN H
LEFT JOIN SALES_DWH.GOLD.DIM_CUSTOMER C
    ON H.CUSTOMERID = C.CUSTOMER_ID
LEFT JOIN SALES_DWH.GOLD.DIM_DATE OD
    ON TO_DATE(H.ORDERDATE) = OD.FULL_DATE
LEFT JOIN SALES_DWH.GOLD.DIM_DATE DD
    ON TO_DATE(H.DUEDATE) = DD.FULL_DATE
LEFT JOIN SALES_DWH.GOLD.DIM_DATE SD
    ON TO_DATE(H.SHIPDATE) = SD.FULL_DATE;

-- Validate fact output
SELECT * FROM SALES_DWH.GOLD.FACT_SALES_ORDER_HEADER;

-- =====================================================================
-- Sales Order Detail Fact
-- Grain: One row per sales order line item
-- =====================================================================

CREATE OR REPLACE TABLE SALES_DWH.GOLD.FACT_SALES_ORDER_DETAIL AS
SELECT
    D.SALESORDERID AS SALES_ORDER_ID,
    D.SALESORDERDETAILID AS SALES_ORDER_DETAIL_ID,

    COALESCE(P.PRODUCT_KEY, -1) AS PRODUCT_KEY,
    COALESCE(C.CUSTOMER_KEY, -1) AS CUSTOMER_KEY,
    COALESCE(OD.DATE_KEY, -1) AS ORDER_DATE_KEY,

    D.SPECIALOFFERID AS SPECIAL_OFFER_ID,
    D.CARRIERTRACKINGNUMBER AS CARRIER_TRACKING_NUMBER,

    D.ORDERQTY AS ORDER_QTY,
    D.UNITPRICE AS UNIT_PRICE,
    D.UNITPRICEDISCOUNT AS UNIT_PRICE_DISCOUNT,

    D.ORDERQTY * D.UNITPRICE AS GROSS_LINE_AMOUNT,
    D.ORDERQTY * D.UNITPRICE * D.UNITPRICEDISCOUNT AS DISCOUNT_AMOUNT,
    D.LINETOTAL AS NET_LINE_AMOUNT,

    D.MODIFIEDDATE AS SOURCE_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS ETL_INSERT_TS
FROM SALES_DWH.SILVER.SALES_ORDER_DETAIL_CLEAN D
LEFT JOIN SALES_DWH.SILVER.SALES_ORDER_HEADER_CLEAN H
    ON D.SALESORDERID = H.SALESORDERID
LEFT JOIN SALES_DWH.GOLD.DIM_PRODUCT P
    ON D.PRODUCTID = P.PRODUCT_ID
LEFT JOIN SALES_DWH.GOLD.DIM_CUSTOMER C
    ON H.CUSTOMERID = C.CUSTOMER_ID
LEFT JOIN SALES_DWH.GOLD.DIM_DATE OD
    ON TO_DATE(H.ORDERDATE) = OD.FULL_DATE;

-- Validate fact output
SELECT * FROM SALES_DWH.GOLD.FACT_SALES_ORDER_DETAIL;

SELECT * FROM SALES_DWH.GOLD.FACT_SALES_ORDER_HEADER;

-- Check for duplicate order-level fact records
SELECT SALES_ORDER_ID, COUNT(*)
FROM SALES_DWH.GOLD.FACT_SALES_ORDER_HEADER
GROUP BY SALES_ORDER_ID
HAVING COUNT(*) > 1;

USE SCHEMA GOLD;

-- Validate aggregate measures in the order header fact
SELECT
    COUNT(*) AS RECORD_COUNT,
    SUM(SUBTOTAL_AMOUNT) AS SUBTOTAL,
    SUM(TAX_AMOUNT) AS SUM_TAX,
    SUM(FREIGHT_AMOUNT) AS SUM_FREIGHT,
    SUM(TOTAL_DUE_AMOUNT) AS SUM_TOTAL_DUE
FROM FACT_SALES_ORDER_HEADER;

-- Check for duplicate order line fact records
SELECT
    SALES_ORDER_ID,
    SALES_ORDER_DETAIL_ID,
    COUNT(*)
FROM FACT_SALES_ORDER_DETAIL
GROUP BY SALES_ORDER_ID, SALES_ORDER_DETAIL_ID
HAVING COUNT(*) > 1;

-- Validate aggregate measures in the order detail fact
SELECT
    COUNT(*) AS RECORD_COUNT,
    SUM(ORDER_QTY) AS SUM_ORDER_QTY,
    SUM(GROSS_LINE_AMOUNT) AS SUM_GROSS,
    SUM(DISCOUNT_AMOUNT) AS SUM_DISCOUNT,
    SUM(NET_LINE_AMOUNT) AS SUM_NET
FROM FACT_SALES_ORDER_DETAIL;

-- Review fact distribution by customer surrogate key
SELECT
    CUSTOMER_KEY,
    COUNT(*)
FROM FACT_SALES_ORDER_HEADER
GROUP BY CUSTOMER_KEY
ORDER BY CUSTOMER_KEY;

SELECT *
FROM FACT_SALES_ORDER_HEADER;

-- Validate date dimension population by month
SELECT YEAR_MONTH, COUNT(*)
FROM DIM_DATE
GROUP BY YEAR_MONTH;

-- =====================================================================
-- Data Mart: Monthly Sales
-- =====================================================================

-- Create a monthly sales mart to support trend reporting and KPI dashboards

CREATE OR REPLACE TABLE SALES_DWH.GOLD.MART_MONTHLY_SALES AS
SELECT
    D.YEAR_MONTH,
    D.YEAR_NUMBER,
    D.MONTH_NUMBER,
    COUNT(H.SALES_ORDER_ID) AS ORDER_QTY,
    SUM(H.TOTAL_DUE_AMOUNT) AS TOTAL_SALES
FROM FACT_SALES_ORDER_HEADER AS H
JOIN DIM_DATE AS D
    ON H.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.YEAR_MONTH, D.YEAR_NUMBER, D.MONTH_NUMBER
ORDER BY D.YEAR_MONTH;

SELECT * FROM MART_MONTHLY_SALES;

SHOW TABLES;

-- =====================================================================
-- Sales Trend Queries
-- =====================================================================

-- Total sales by day
SELECT
    D.FULL_DATE AS DATE,
    COUNT(H.SALES_ORDER_ID) AS ORDER_QTY,
    SUM(H.TOTAL_DUE_AMOUNT) AS NET_SALES
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON H.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.FULL_DATE
ORDER BY D.FULL_DATE;

-- Total sales by quarter
SELECT
    D.YEAR_NUMBER AS YEAR,
    D.QUARTER_NUMBER AS QUARTER,
    COUNT(H.SALES_ORDER_ID) AS ORDER_QTY,
    SUM(H.TOTAL_DUE_AMOUNT) AS NET_SALES
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON H.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.YEAR_NUMBER, D.QUARTER_NUMBER
ORDER BY D.YEAR_NUMBER, D.QUARTER_NUMBER;

-- Total sales by year
SELECT
    D.YEAR_NUMBER AS YEAR,
    COUNT(H.SALES_ORDER_ID) AS ORDER_QTY,
    SUM(H.TOTAL_DUE_AMOUNT) AS NET_SALES
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON H.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.YEAR_NUMBER
ORDER BY D.YEAR_NUMBER;

-- Month-over-month revenue growth analysis
WITH MONTHLY_REVENUE AS (
    SELECT
        D.YEAR_MONTH,
        SUM(H.TOTAL_DUE_AMOUNT) AS TOTAL_REVENUE
    FROM FACT_SALES_ORDER_HEADER H
    JOIN DIM_DATE D
        ON H.ORDER_DATE_KEY = D.DATE_KEY
    GROUP BY D.YEAR_MONTH
)
SELECT
    YEAR_MONTH,
    TOTAL_REVENUE,
    LAG(TOTAL_REVENUE) OVER (ORDER BY YEAR_MONTH) AS PREV_REVENUE,
    TOTAL_REVENUE - LAG(TOTAL_REVENUE) OVER (ORDER BY YEAR_MONTH) AS GROWTH,
    ((TOTAL_REVENUE - LAG(TOTAL_REVENUE) OVER (ORDER BY YEAR_MONTH))
        / NULLIF(LAG(TOTAL_REVENUE) OVER (ORDER BY YEAR_MONTH), 0)) * 100 AS GROWTH_RATE
FROM MONTHLY_REVENUE
ORDER BY YEAR_MONTH;

-- =====================================================================
-- Order Volume Analysis
-- =====================================================================

-- Total orders per day
SELECT
    D.FULL_DATE,
    COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_DAY
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON D.DATE_KEY = H.ORDER_DATE_KEY
GROUP BY D.FULL_DATE
ORDER BY D.FULL_DATE;

-- Total orders per month
SELECT
    D.YEAR_MONTH,
    COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_MONTH
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON D.DATE_KEY = H.ORDER_DATE_KEY
GROUP BY D.YEAR_MONTH
ORDER BY D.YEAR_MONTH;

-- Total orders per year
SELECT
    D.YEAR_NUMBER,
    COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_YEAR
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON D.DATE_KEY = H.ORDER_DATE_KEY
GROUP BY D.YEAR_NUMBER
ORDER BY D.YEAR_NUMBER;

-- Total orders per quarter
SELECT
    D.YEAR_NUMBER,
    D.QUARTER_NUMBER,
    COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_QUARTER
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_DATE D
    ON D.DATE_KEY = H.ORDER_DATE_KEY
GROUP BY D.YEAR_NUMBER, D.QUARTER_NUMBER
ORDER BY D.YEAR_NUMBER, D.QUARTER_NUMBER;

-- Average number of orders per day within each month
WITH AVG_MONTH AS (
    SELECT
        D.FULL_DATE,
        D.YEAR_MONTH,
        COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_DAY
    FROM FACT_SALES_ORDER_HEADER H
    JOIN DIM_DATE D
        ON D.DATE_KEY = H.ORDER_DATE_KEY
    GROUP BY D.FULL_DATE, D.YEAR_MONTH
)
SELECT
    YEAR_MONTH,
    AVG(ORDERS_PER_DAY) AS AVG_PER_MONTH
FROM AVG_MONTH
GROUP BY YEAR_MONTH;

-- Average number of orders per month within each year
WITH AVG_YEAR AS (
    SELECT
        D.YEAR_MONTH,
        D.YEAR_NUMBER,
        COUNT(H.SALES_ORDER_ID) AS ORDERS_PER_MONTH
    FROM FACT_SALES_ORDER_HEADER H
    JOIN DIM_DATE D
        ON D.DATE_KEY = H.ORDER_DATE_KEY
    GROUP BY D.YEAR_MONTH, D.YEAR_NUMBER
)
SELECT
    YEAR_NUMBER,
    AVG(ORDERS_PER_MONTH) AS AVG_PER_YEAR
FROM AVG_YEAR
GROUP BY YEAR_NUMBER;

-- =====================================================================
-- Product Performance Analysis
-- =====================================================================

-- Top-selling products by quantity and net sales
SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    SUM(D.ORDER_QTY) AS PRODUCT_NET_QTY,
    SUM(D.NET_LINE_AMOUNT) AS PRODUCT_NET_SALE
FROM FACT_SALES_ORDER_DETAIL D
JOIN DIM_PRODUCT P
    ON D.PRODUCT_KEY = P.PRODUCT_KEY
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME
ORDER BY PRODUCT_NET_QTY DESC;

-- Create product sales mart
CREATE OR REPLACE TABLE SALES_DWH.GOLD.MART_PRODUCT_SALES AS
SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    SUM(D.ORDER_QTY) AS PRODUCT_NET_QTY,
    SUM(D.NET_LINE_AMOUNT) AS PRODUCT_NET_SALE
FROM FACT_SALES_ORDER_DETAIL D
JOIN DIM_PRODUCT P
    ON D.PRODUCT_KEY = P.PRODUCT_KEY
GROUP BY P.PRODUCT_ID, P.PRODUCT_NAME
ORDER BY PRODUCT_NET_SALE DESC;

SELECT * FROM MART_PRODUCT_SALES;

-- =====================================================================
-- Customer Performance Analysis
-- =====================================================================

-- Create customer sales mart
CREATE OR REPLACE TABLE MART_CUSTOMER_SALES AS
SELECT
    C.CUSTOMER_ID AS CUSTOMER_ID,
    C.CUSTOMER_TYPE AS CUSTOMER_TYPE,
    SUM(TOTAL_DUE_AMOUNT) AS CUSTOMER_NET_SALES
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_CUSTOMER C
    ON H.CUSTOMER_KEY = C.CUSTOMER_KEY
GROUP BY C.CUSTOMER_ID, C.CUSTOMER_TYPE
ORDER BY C.CUSTOMER_ID;

-- Top customers by sales
SELECT *
FROM MART_CUSTOMER_SALES
ORDER BY CUSTOMER_NET_SALES DESC
LIMIT 10;

-- Average order value per customer
SELECT
    C.CUSTOMER_ID,
    AVG(TOTAL_DUE_AMOUNT) AS AVG_ORDER_VALUE
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_CUSTOMER C
    ON H.CUSTOMER_KEY = C.CUSTOMER_KEY
GROUP BY C.CUSTOMER_ID
ORDER BY AVG_ORDER_VALUE DESC;

-- =====================================================================
-- Discount and Product Utilization Analysis
-- =====================================================================

-- Product with the highest total discount amount
SELECT
    P.PRODUCT_KEY,
    SUM(DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT
FROM FACT_SALES_ORDER_DETAIL D
JOIN DIM_PRODUCT P
    ON D.PRODUCT_KEY = P.PRODUCT_KEY
GROUP BY P.PRODUCT_KEY
ORDER BY TOTAL_DISCOUNT DESC;

-- Product with the highest discount usage frequency
SELECT 
    P.PRODUCT_KEY,
    P.PRODUCT_NAME,
    COUNT(*) AS DISCOUNT_USAGE
FROM FACT_SALES_ORDER_DETAIL D
JOIN DIM_PRODUCT P
    ON D.PRODUCT_KEY = P.PRODUCT_KEY
WHERE D.DISCOUNT_AMOUNT > 0
GROUP BY P.PRODUCT_KEY, P.PRODUCT_NAME
ORDER BY DISCOUNT_USAGE DESC;

-- Total quantity sold per product
SELECT
    P.PRODUCT_KEY,
    P.PRODUCT_NAME,
    SUM(D.ORDER_QTY) AS QTY_PER_PRODUCT
FROM FACT_SALES_ORDER_DETAIL D
JOIN DIM_PRODUCT P
    ON D.PRODUCT_KEY = P.PRODUCT_KEY
GROUP BY P.PRODUCT_KEY, P.PRODUCT_NAME;

-- =====================================================================
-- Customer Order Behavior Analysis
-- =====================================================================

-- Number of orders per customer
SELECT
    C.CUSTOMER_ID,
    COUNT(DISTINCT H.SALES_ORDER_ID) AS ORDER_QTY_CUSTOMER
FROM FACT_SALES_ORDER_HEADER H
JOIN DIM_CUSTOMER C
    ON H.CUSTOMER_KEY = C.CUSTOMER_KEY
GROUP BY C.CUSTOMER_ID
ORDER BY ORDER_QTY_CUSTOMER DESC;

-- Classify customers as repeat or new based on order count
WITH CUSTOMER_COUNT AS (
    SELECT
        C.CUSTOMER_ID,
        COUNT(DISTINCT H.SALES_ORDER_ID) AS ORDER_QTY_CUSTOMER
    FROM FACT_SALES_ORDER_HEADER H
    JOIN DIM_CUSTOMER C
        ON H.CUSTOMER_KEY = C.CUSTOMER_KEY
    GROUP BY C.CUSTOMER_ID
)
SELECT
    CUSTOMER_ID,
    CASE
        WHEN ORDER_QTY_CUSTOMER > 1 THEN 'REPEAT'
        ELSE 'NEW'
    END AS CUSTOMER_SEGMENT,
    ORDER_QTY_CUSTOMER
FROM CUSTOMER_COUNT
WHERE ORDER_QTY_CUSTOMER > 0
ORDER BY ORDER_QTY_CUSTOMER DESC;