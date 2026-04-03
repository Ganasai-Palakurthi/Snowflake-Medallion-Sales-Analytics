-- =====================================================================
-- Bronze Layer Table Creation and Raw Data Ingestion
-- =====================================================================

-- Set the active database context
USE DATABASE SALES_DWH;

-- Set the active schema context for raw data ingestion
-- Bronze is used to store source-aligned data with minimal transformation
USE SCHEMA BRONZE;

-- Validate the current execution context before running DDL or DML
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- =====================================================================
-- Bronze Raw Table Definitions
-- =====================================================================

-- Create raw customer table to store source data as received
CREATE OR REPLACE TABLE CUSTOMER_RAW (
    CustomerID NUMBER,
    PersonID NUMBER,
    StoreID NUMBER,
    TerritoryID NUMBER,
    AccountNumber STRING,
    rowguid STRING,
    ModifiedDate TIMESTAMP_NTZ
);

-- Create raw product table to store source product master data
CREATE OR REPLACE TABLE PRODUCT_RAW (
    ProductID NUMBER,
    Name STRING,
    ProductNumber STRING,
    MakeFlag NUMBER(1),
    FinishedGoodsFlag NUMBER(1),
    Color STRING,
    SafetyStockLevel NUMBER,
    ReorderPoint NUMBER,
    StandardCost NUMBER(10,2),
    ListPrice NUMBER(10,2),
    Size STRING,
    SizeUnitMeasureCode STRING,
    WeightUnitMeasureCode STRING,
    Weight NUMBER(10,2),
    DaysToManufacture NUMBER,
    ProductLine STRING,
    Class STRING,
    Style STRING,
    ProductSubcategoryID NUMBER,
    ProductModelID NUMBER,
    SellStartDate TIMESTAMP_NTZ,
    SellEndDate TIMESTAMP_NTZ,
    DiscontinuedDate TIMESTAMP_NTZ,
    rowguid STRING,
    ModifiedDate TIMESTAMP_NTZ
);

-- Create raw sales order detail table for transactional line-level data
CREATE OR REPLACE TABLE SALES_ORDER_DETAIL_RAW (
    SalesOrderID NUMBER,
    SalesOrderDetailID NUMBER,
    CarrierTrackingNumber STRING,
    OrderQty NUMBER,
    ProductID NUMBER,
    SpecialOfferID NUMBER,
    UnitPrice NUMBER(10,5),
    UnitPriceDiscount NUMBER(10,2),
    LineTotal NUMBER(12,6),
    rowguid STRING,
    ModifiedDate TIMESTAMP_NTZ
);

-- Create raw sales order header table for order-level transactional data
CREATE OR REPLACE TABLE SALES_ORDER_HEADER_RAW (
    SalesOrderID NUMBER,
    RevisionNumber NUMBER,
    OrderDate TIMESTAMP_NTZ,
    DueDate TIMESTAMP_NTZ,
    ShipDate TIMESTAMP_NTZ,
    Status NUMBER,
    OnlineOrderFlag NUMBER(1),
    SalesOrderNumber STRING,
    PurchaseOrderNumber STRING,
    AccountNumber STRING,
    CustomerID NUMBER,
    SalesPersonID NUMBER,
    TerritoryID NUMBER,
    BillToAddressID NUMBER,
    ShipToAddressID NUMBER,
    ShipMethodID NUMBER,
    CreditCardID NUMBER,
    CreditCardApprovalCode STRING,
    CurrencyRateID NUMBER,
    SubTotal NUMBER(12,4),
    TaxAmt NUMBER(10,4),
    Freight NUMBER(10,4),
    TotalDue NUMBER(12,6),
    Comment STRING,
    rowguid STRING,
    ModifiedDate TIMESTAMP_NTZ
);

-- =====================================================================
-- Raw Data Ingestion into Bronze Layer
-- =====================================================================

-- Load source data from S3 into Snowflake internal Bronze tables.
-- COPY INTO is used here for batch ingestion from the external stage.
--
-- Other ingestion options commonly used in production include:
--   • Snowpipe: for event-driven or near-real-time ingestion
--   • External Tables: for querying data in place without loading it
--
-- In this implementation, COPY INTO is appropriate because the data
-- is being batch loaded into internal Snowflake tables for downstream
-- transformation into Silver and Gold layers.

COPY INTO CUSTOMER_RAW
FROM @SALES_EXT_STAGE/customer/
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT);

COPY INTO PRODUCT_RAW
FROM @SALES_EXT_STAGE/product/
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT);

COPY INTO SALES_ORDER_DETAIL_RAW
FROM @SALES_EXT_STAGE/sales_order_detail/
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT);

COPY INTO SALES_ORDER_HEADER_RAW
FROM @SALES_EXT_STAGE/sales_order_header/
FILE_FORMAT = (FORMAT_NAME = CSV_FILE_FORMAT);

-- Validate table creation in the current schema
SHOW TABLES;

-- =====================================================================
-- Post-Load Validation
-- =====================================================================

-- Verify that data has been successfully loaded into Bronze tables
SELECT * FROM CUSTOMER_RAW LIMIT 5;

SELECT * FROM PRODUCT_RAW LIMIT 5;

SELECT * FROM SALES_ORDER_DETAIL_RAW LIMIT 5;

SELECT * FROM SALES_ORDER_HEADER_RAW LIMIT 5;