-- Redshift Compression Encodings
-- This script demonstrates different compression encodings and their performance benefits

-- Create table with default encoding (RAW)
-- Use case: Small tables or data that doesn't compress well
CREATE TABLE small_reference_data (
    id INTEGER,
    name VARCHAR(100),
    code VARCHAR(50),
    is_active BOOLEAN
) DISTSTYLE EVEN;

-- Create table with RUNLENGTH encoding
-- Best for: Columns with many repeated values (booleans, status flags, etc.)
CREATE TABLE customer_status (
    customer_id VARCHAR(50) DISTKEY,
    customer_name VARCHAR(100),
    account_status VARCHAR(20) ENCODE RUNLENGTH,  -- Many repeated status values
    is_premium BOOLEAN ENCODE RUNLENGTH,           -- Boolean values compress well
    email VARCHAR(100),
    registration_date TIMESTAMP
) DISTKEY (customer_id);

-- Create table with BYTE DICT encoding
-- Best for: Low-cardinality string columns with many repeated values
CREATE TABLE product_categories (
    category_id VARCHAR(50) DISTKEY,
    category_name VARCHAR(100) ENCODE BYTEDICT,  -- Limited number of category names
    department VARCHAR(50) ENCODE BYTEDICT,       -- Limited department names
    parent_category VARCHAR(50) ENCODE BYTEDICT,
    description TEXT
) DISTKEY (category_id);

-- Create table with DELTA encoding
-- Best for: Numeric columns with sequential or incremental values
CREATE TABLE order_history (
    order_id BIG IDENTITY(1,1) ENCODE DELTA,      -- Sequential order IDs
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2) ENCODE DELTA,     -- Amounts often have patterns
    tax_amount DECIMAL(10,2) ENCODE DELTA,
    shipping_amount DECIMAL(10,2) ENCODE DELTA
) DISTKEY (customer_id);

-- Create table with LZO encoding
-- Best for: Large text columns or binary data
CREATE TABLE product_descriptions (
    product_id VARCHAR(50) DISTKEY,
    product_name VARCHAR(200),
    description TEXT ENCODE LZO,                  -- Large text fields
    specifications TEXT ENCODE LZO,
    technical_manual TEXT ENCODE LZO
) DISTKEY (product_id);

-- Create table with ZSTD encoding (modern, high compression)
-- Best for: General purpose compression with good ratio and speed
CREATE TABLE sales_analytics (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50) DISTKEY,
    sale_amount DECIMAL(10,2) ENCODE ZSTD,
    sale_date TIMESTAMP ENCODE ZSTD,
    region VARCHAR(50) ENCODE ZSTD,
    store_id INTEGER ENCODE ZSTD,
    quantity INTEGER ENCODE ZSTD
) DISTKEY (customer_id);

-- Compression encoding selection guide:
-- RUNLENGTH: Best for boolean flags, status columns, repeated values
-- BYTEDICT: Best for low-cardinality strings (categories, status, codes)
-- DELTA: Best for sequential numbers, timestamps, incremental values
-- LZO: Best for large text fields, documents, binary data
-- ZSTD: Best for general purpose compression, good balance of ratio and speed
-- TEXT255: Best for short strings that don't compress well with other methods

-- Performance benefits of compression:
-- 1. Reduces storage requirements (up to 90% compression ratio)
-- 2. Improves query performance (less data to read from disk)
-- 3. Reduces I/O operations
-- 4. Can improve network bandwidth usage
-- 5. Allows more data to fit in memory

-- Compression encoding recommendations by data type:
-- - BOOLEAN: RUNLENGTH
-- - INTEGER: DELTA (sequential), TEXT255 (random)
-- - DECIMAL: DELTA (sequential), TEXT255 (random)
-- - TIMESTAMP: DELTA (sequential), TEXT255 (random)
-- - VARCHAR(1-255): BYTEDICT (low cardinality), TEXT255 (high cardinality)
-- - VARCHAR(256+): LZO, ZSTD (large text)
-- - TEXT: LZO, ZSTD

-- Example of optimal compression encoding selection
CREATE TABLE optimized_sales_data (
    sale_id BIGINT IDENTITY(1,1) ENCODE DELTA,
    product_id VARCHAR(50) ENCODE BYTEDICT,       -- Limited product IDs
    customer_id VARCHAR(50) DISTKEY,
    sale_amount DECIMAL(10,2) ENCODE DELTA,       -- Sequential amounts
    sale_date TIMESTAMP ENCODE DELTA,              -- Sequential dates
    region VARCHAR(50) ENCODE BYTEDICT,           -- Limited regions
    store_id INTEGER ENCODE DELTA,                -- Sequential store IDs
    quantity INTEGER ENCODE DELTA,                -- Sequential quantities
    discount_percentage DECIMAL(5,2) ENCODE TEXT255 -- Limited discount values
) DISTKEY (customer_id);

-- Monitoring compression effectiveness:
-- Check compression ratios using system tables
SELECT 
    schemaname,
    tablename,
    attname,
    compressiontype,
    compresstype
FROM stv_tblcomp
JOIN pg_class ON stv_tblcomp.tbl = pg_class.oid
JOIN pg_attribute ON stv_tblcomp.att = pg_attribute.attnum
WHERE schemaname = 'public'
ORDER BY schemaname, tablename, attname;

-- Apply compression to existing tables:
-- ALTER TABLE sales_analytics ALTER COLUMN sale_amount ENCODE ZSTD;
-- ALTER TABLE sales_analytics ALTER COLUMN region ENCODE BYTEDICT;