-- IntelliStock Database Setup
-- Creates database, schema, and tables using idempotent DDL
-- Create database
CREATE DATABASE IF NOT EXISTS INTELLISTOCK_DB;
-- Use database
USE DATABASE INTELLISTOCK_DB;
-- Create schema
CREATE SCHEMA IF NOT EXISTS PUBLIC;
-- Use schema
USE SCHEMA PUBLIC;
-- Create inventory table with exact schema from PRD
CREATE TABLE IF NOT EXISTS INVENTORY (
    date DATE,
    organization STRING,
    location STRING,
    item STRING,
    opening_stock INTEGER,
    received INTEGER,
    issued INTEGER,
    closing_stock INTEGER,
    lead_time_days INTEGER
);
-- Optional: If you want to load from CSV using Snowflake's COPY command
-- First, create a stage (you'll need to configure this based on your file location)
/*
 CREATE OR REPLACE STAGE inventory_stage;
 
 -- Put your CSV file to the stage (run this from SnowSQL or web interface)
 -- PUT file://path/to/inventory_sample.csv @inventory_stage;
 
 -- Load data from staged CSV
 COPY INTO INVENTORY
 FROM @inventory_stage/inventory_sample.csv
 FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
 ON_ERROR = 'CONTINUE';
 */
-- Verify table creation
SELECT 'Table created successfully' as status;
DESCRIBE TABLE INVENTORY;