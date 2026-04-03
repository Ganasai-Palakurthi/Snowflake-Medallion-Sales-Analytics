-- =====================================================================
-- Database and Schema Initialization (Medallion Architecture Setup)
-- =====================================================================

-- Create or replace the primary data warehouse database
CREATE OR REPLACE DATABASE SALES_DWH;

-- Set the active database context
USE DATABASE SALES_DWH;

-- ---------------------------------------------------------------------
-- Create core schemas representing medallion architecture layers
-- ---------------------------------------------------------------------

-- Bronze layer: raw ingested data (as-is from source systems)
CREATE OR REPLACE SCHEMA BRONZE;

-- Silver layer: cleaned, standardized, and deduplicated data
CREATE OR REPLACE SCHEMA SILVER;

-- Gold layer: curated, business-level aggregates and data marts
CREATE OR REPLACE SCHEMA GOLD;

-- ---------------------------------------------------------------------
-- Additional schema considerations (commonly used in production)
-- ---------------------------------------------------------------------

-- ELT schema (optional but recommended in enterprise environments):
-- Used to handle intermediate processing, including:
--   • Data quality validation outputs
--   • Segregation of GOOD / BAD / REJECTED records
--   • Transformation checkpoints between Bronze and Silver layers
--   • Logging and audit tables for pipeline observability
--
-- Note: Some organizations separate this into multiple schemas 
-- (e.g., STAGING, ERROR, AUDIT) instead of a single ELT schema.

-- ---------------------------------------------------------------------
-- Metadata validation
-- ---------------------------------------------------------------------

-- Verify schema creation within the current database
SHOW SCHEMAS;

-- ---------------------------------------------------------------------
-- Interview Reference
-- ---------------------------------------------------------------------

-- Database vs Schema:
--   • Database: Logical container for schemas; used for high-level 
--     organization, access control, and environment separation 
--     (e.g., DEV, QA, PROD).
--
--   • Schema: Subdivision within a database; used to organize 
--     objects such as tables, views, stages, and procedures 
--     based on data layers or business domains.