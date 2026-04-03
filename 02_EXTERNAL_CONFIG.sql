-- =====================================================================
-- External Storage Connectivity Setup (Amazon S3 Integration)
-- =====================================================================

-- Create a storage integration to securely connect Snowflake to
-- external cloud storage without embedding credentials.
-- Snowflake uses this integration to assume an authorized IAM role
-- for secure access to S3 locations.

CREATE OR REPLACE STORAGE INTEGRATION S3
TYPE = EXTERNAL_STAGE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::873873310101:role/S3_Access_Snowflake'
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://sales-data-lake-dev/bronze/');

-- After integration creation, update the AWS IAM trust policy with the
-- Snowflake-generated external ID to permit secure role assumption.

DESC STORAGE INTEGRATION S3;

-- Validate available storage integrations
SHOW STORAGE INTEGRATIONS;

-- =====================================================================
-- File Format Definition
-- =====================================================================

-- Define a reusable file format for CSV-based ingestion.
-- File formats are schema-level objects and can be reused across
-- multiple ingestion processes via fully qualified references.

CREATE OR REPLACE FILE FORMAT SALES_DWH.BRONZE.CSV_FILE_FORMAT
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '');

-- Validate file format creation
SHOW FILE FORMATS;

-- =====================================================================
-- External Stage Definition
-- =====================================================================

-- Create an external stage to reference data stored in S3.
-- The stage defines the location and access configuration but does not
-- physically load data into Snowflake tables.

CREATE OR REPLACE STAGE SALES_DWH.BRONZE.SALES_EXT_STAGE
URL = 's3://sales-data-lake-dev/bronze/'
STORAGE_INTEGRATION = S3
FILE_FORMAT = SALES_DWH.BRONZE.CSV_FILE_FORMAT;

-- Set execution context
USE DATABASE SALES_DWH;
USE SCHEMA BRONZE;

-- List files available in the external stage
LIST @SALES_EXT_STAGE;

-- =====================================================================
-- Interview Reference
-- =====================================================================

-- Storage Integration vs External Stage:
--   • Storage Integration:
--       Security object enabling Snowflake to authenticate to external
--       cloud storage by assuming an IAM role.
--
--   • External Stage:
--       Metadata object that defines the external data location and
--       access configuration using the storage integration and file format.