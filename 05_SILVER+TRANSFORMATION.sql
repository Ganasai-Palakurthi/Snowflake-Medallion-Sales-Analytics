-- =====================================================================
-- Silver Layer Data Standardization and Cleansing
-- =====================================================================

-- Set execution context
USE DATABASE SALES_DWH;
USE SCHEMA SILVER;

-- Validate current execution context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- =====================================================================
-- Silver Layer Transformation Scope
-- =====================================================================

-- Apply foundational data quality transformations before loading data
-- into the Silver layer.
--
-- Typical Silver-layer transformations include:
--   • Null handling and default value standardization
--   • String normalization (TRIM, NULLIF, case standardization)
--   • Numeric precision alignment
--   • Basic temporal validation
--   • Business-rule-based attribute derivation
--   • Deduplication where required based on business keys
--
-- Note: This script focuses primarily on cleansing and standardization.
-- If duplicate source records exist, deduplication should be applied
-- explicitly using business keys and row ranking logic.

-- =====================================================================
-- CUSTOMER DATA CLEANSING
-- =====================================================================

CREATE OR REPLACE TABLE SILVER.CUSTOMER_CLEAN AS
SELECT
    CUSTOMERID,
    PERSONID,
    STOREID,
    COALESCE(TERRITORYID, -1) AS TERRITORYID,
    UPPER(NULLIF(TRIM(ACCOUNTNUMBER), '')) AS ACCOUNTNUMBER,
    LOWER(NULLIF(TRIM(ROWGUID), '')) AS ROWGUID,
    CASE
        WHEN MODIFIEDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE MODIFIEDDATE
    END AS MODIFIEDDATE,
    CASE
        WHEN PERSONID IS NOT NULL AND STOREID IS NULL THEN 'PERSON'
        WHEN PERSONID IS NULL AND STOREID IS NOT NULL THEN 'STORE'
        WHEN PERSONID IS NULL AND STOREID IS NULL THEN 'NOT_APPLICABLE'
        WHEN PERSONID IS NOT NULL AND STOREID IS NOT NULL THEN 'INVALID'
    END AS CUSTOMER_TYPE
FROM SALES_DWH.BRONZE.CUSTOMER_RAW;

-- Validate transformed customer data
SELECT * 
FROM CUSTOMER_CLEAN
LIMIT 10;

-- =====================================================================
-- PRODUCT DATA CLEANSING
-- =====================================================================

CREATE OR REPLACE TABLE SILVER.PRODUCT_CLEAN AS
SELECT
    PRODUCTID,
    NULLIF(TRIM(NAME), '') AS NAME,
    UPPER(NULLIF(TRIM(PRODUCTNUMBER), '')) AS PRODUCTNUMBER,
    MAKEFLAG,
    FINISHEDGOODSFLAG,
    UPPER(NULLIF(TRIM(COLOR), '')) AS COLOR,
    SAFETYSTOCKLEVEL,
    REORDERPOINT,
    ROUND(STANDARDCOST, 2) AS STANDARDCOST,
    ROUND(LISTPRICE, 2) AS LISTPRICE,
    UPPER(NULLIF(TRIM(SIZE), '')) AS SIZE,
    UPPER(NULLIF(TRIM(SIZEUNITMEASURECODE), '')) AS SIZEUNITMEASURECODE,
    UPPER(NULLIF(TRIM(WEIGHTUNITMEASURECODE), '')) AS WEIGHTUNITMEASURECODE,
    ROUND(WEIGHT, 2) AS WEIGHT,
    DAYSTOMANUFACTURE,
    UPPER(NULLIF(TRIM(PRODUCTLINE), '')) AS PRODUCTLINE,
    UPPER(NULLIF(TRIM(CLASS), '')) AS CLASS,
    UPPER(NULLIF(TRIM(STYLE), '')) AS STYLE,
    PRODUCTSUBCATEGORYID,
    PRODUCTMODELID,
    CASE
        WHEN SELLSTARTDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE SELLSTARTDATE
    END AS SELLSTARTDATE,
    CASE
        WHEN SELLENDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE SELLENDDATE
    END AS SELLENDDATE,
    CASE
        WHEN DISCONTINUEDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE DISCONTINUEDDATE
    END AS DISCONTINUEDDATE,
    LOWER(NULLIF(TRIM(ROWGUID), '')) AS ROWGUID,
    CASE
        WHEN MODIFIEDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE MODIFIEDDATE
    END AS MODIFIEDDATE
FROM SALES_DWH.BRONZE.PRODUCT_RAW;

-- Validate transformed product data
SELECT * 
FROM PRODUCT_CLEAN
LIMIT 10;

-- =====================================================================
-- SALES ORDER DETAIL DATA CLEANSING
-- =====================================================================

CREATE OR REPLACE TABLE SALES_DWH.SILVER.SALES_ORDER_DETAIL_CLEAN AS
SELECT
    SALESORDERID,
    SALESORDERDETAILID,
    UPPER(NULLIF(TRIM(CARRIERTRACKINGNUMBER), '')) AS CARRIERTRACKINGNUMBER,
    ORDERQTY,
    PRODUCTID,
    SPECIALOFFERID,
    ROUND(UNITPRICE, 5) AS UNITPRICE,
    ROUND(UNITPRICEDISCOUNT, 2) AS UNITPRICEDISCOUNT,
    ROUND(LINETOTAL, 6) AS LINETOTAL,
    LOWER(NULLIF(TRIM(ROWGUID), '')) AS ROWGUID,
    CASE
        WHEN MODIFIEDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE MODIFIEDDATE
    END AS MODIFIEDDATE
FROM SALES_DWH.BRONZE.SALES_ORDER_DETAIL_RAW;

-- Validate transformed sales order detail data
SELECT * 
FROM SALES_DWH.SILVER.SALES_ORDER_DETAIL_CLEAN
LIMIT 5;

-- =====================================================================
-- SALES ORDER HEADER DATA CLEANSING
-- =====================================================================

CREATE OR REPLACE TABLE SALES_DWH.SILVER.SALES_ORDER_HEADER_CLEAN AS
SELECT
    SALESORDERID,
    REVISIONNUMBER,
    CASE
        WHEN ORDERDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE ORDERDATE
    END AS ORDERDATE,
    CASE
        WHEN DUEDATE > CURRENT_TIMESTAMP() THEN NULL
        WHEN ORDERDATE > DUEDATE THEN NULL
        ELSE DUEDATE
    END AS DUEDATE,
    CASE
        WHEN SHIPDATE > CURRENT_TIMESTAMP() THEN NULL
        WHEN ORDERDATE > SHIPDATE THEN NULL
        ELSE SHIPDATE
    END AS SHIPDATE,
    STATUS,
    ONLINEORDERFLAG,
    UPPER(NULLIF(TRIM(SALESORDERNUMBER), '')) AS SALESORDERNUMBER,
    UPPER(NULLIF(TRIM(PURCHASEORDERNUMBER), '')) AS PURCHASEORDERNUMBER,
    UPPER(NULLIF(TRIM(ACCOUNTNUMBER), '')) AS ACCOUNTNUMBER,
    CUSTOMERID,
    SALESPERSONID,
    TERRITORYID,
    BILLTOADDRESSID,
    SHIPTOADDRESSID,
    SHIPMETHODID,
    CREDITCARDID,
    UPPER(NULLIF(TRIM(CREDITCARDAPPROVALCODE), '')) AS CREDITCARDAPPROVALCODE,
    CURRENCYRATEID,
    ROUND(SUBTOTAL, 4) AS SUBTOTAL,
    ROUND(TAXAMT, 4) AS TAXAMT,
    ROUND(FREIGHT, 4) AS FREIGHT,
    ROUND(TOTALDUE, 6) AS TOTALDUE,
    LOWER(NULLIF(TRIM(ROWGUID), '')) AS ROWGUID,
    CASE
        WHEN MODIFIEDDATE > CURRENT_TIMESTAMP() THEN NULL
        ELSE MODIFIEDDATE
    END AS MODIFIEDDATE
FROM SALES_DWH.BRONZE.SALES_ORDER_HEADER_RAW;

-- Validate transformed sales order header data
SELECT * 
FROM SALES_DWH.SILVER.SALES_ORDER_HEADER_CLEAN
LIMIT 5;

-- Validate object creation in the Silver schema
SHOW TABLES;