-- Redshift Spectrum and External Tables
-- This script demonstrates Redshift Spectrum for querying data in S3

-- Create external schema for S3 data access
-- Note: Requires proper IAM role permissions for S3 access
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'spectrum_database'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Create external table for CSV data in S3
-- This allows querying CSV files directly from S3 without loading into Redshift
CREATE EXTERNAL TABLE spectrum_sales_csv (
    sale_id BIGINT,
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date VARCHAR(20),  -- CSV format, will be cast in queries
    region VARCHAR(50),
    store_id INTEGER
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://your-bucket-name/sales-data/csv/'
FILEFORMAT TEXTFILE;

-- Create external table for Parquet data in S3
-- Parquet is more efficient for analytical workloads
CREATE EXTERNAL TABLE spectrum_sales_parquet (
    sale_id BIGINT,
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER,
    category VARCHAR(50)
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://your-bucket-name/sales-data/parquet/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Create external table for partitioned data
-- Partitioning improves query performance by reducing data scanned
CREATE EXTERNAL TABLE spectrum_sales_partitioned (
    sale_id BIGINT,
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    store_id INTEGER,
    category VARCHAR(50)
)
PARTITIONED BY (sale_date DATE, region VARCHAR(50))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://your-bucket-name/sales-data/partitioned/';

-- Add partitions to the external table
-- This needs to be done when new data is added to S3
ALTER TABLE spectrum_sales_partitioned ADD PARTITION (sale_date='2024-01-01', region='West')
LOCATION 's3://your-bucket-name/sales-data/partitioned/sale_date=2024-01-01/region=West/';

-- Spectrum Query Examples
-- Query external CSV data with data type conversion
SELECT 
    sale_id,
    product_id,
    customer_id,
    sale_amount,
    TO_TIMESTAMP(sale_date, 'YYYY-MM-DD HH24:MI:SS') as sale_date_timestamp,
    region,
    store_id
FROM spectrum_sales_csv
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West'
LIMIT 1000;

-- Query external Parquet data (more efficient)
SELECT 
    sale_id,
    product_id,
    customer_id,
    sale_amount,
    sale_date,
    region,
    store_id,
    category
FROM spectrum_sales_parquet
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West'
  AND category = 'Electronics'
ORDER BY sale_date DESC
LIMIT 1000;

-- Query partitioned external data (most efficient)
-- Partition pruning reduces data scanned significantly
SELECT 
    sale_id,
    product_id,
    customer_id,
    sale_amount,
    store_id,
    category
FROM spectrum_sales_partitioned
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West'
  AND category = 'Electronics'
ORDER BY sale_date DESC
LIMIT 1000;

-- Spectrum Performance Optimization:

-- 1. Use Parquet format for better compression and performance
-- 2. Partition data by frequently filtered columns
-- 3. Use appropriate file sizes (100MB-1GB per file)
-- 4. Avoid small files (combine when possible)
-- 5. Use columnar format for analytical queries
-- 6. Consider file compression (Snappy, GZIP)

-- Spectrum Best Practices:

-- 1. Data Format Selection
-- - CSV: Simple, good for small datasets, poor compression
-- - Parquet: Excellent for analytical workloads, good compression
-- - ORC: Good for Hive compatibility, excellent compression
-- - Avro: Good for schema evolution, good compression

-- 2. Partitioning Strategy
-- - Partition by columns frequently used in WHERE clauses
-- - Use date partitions for time-series data
-- - Consider region/country partitions for multi-tenant data
-- - Avoid over-partitioning (too many small files)

-- 3. File Organization
-- - Use consistent file naming conventions
-- - Avoid small files (<10MB) when possible
-- - Use appropriate file sizes for your workload
-- - Consider file compaction for frequently updated data

-- 4. Security and Permissions
-- - Use IAM roles with least privilege access
-- - Enable encryption for sensitive data
-- - Use VPC endpoints for secure S3 access
-- - Implement proper access controls

-- Spectrum Monitoring and Management:

-- Query to monitor external table access
SELECT 
    query,
    service_class,
    start_time,
    end_time,
    total_time,
    rows,
    bytes,
    is_rrscan
FROM stv_wlm_query_state
WHERE query LIKE '%spectrum_sales%' 
  AND start_time > NOW() - INTERVAL '1 hour'
ORDER BY start_time DESC;

-- Query to analyze Spectrum query performance
SELECT 
    schemaname,
    tablename,
    attname,
    compressiontype,
    compresstype
FROM stv_tblcomp
JOIN pg_class ON stv_tblcomp.tbl = pg_class.oid
JOIN pg_attribute ON stv_tblcomp.att = pg_attribute.attnum
WHERE schemaname = 'spectrum_schema'
ORDER BY schemaname, tablename, attname;

-- Spectrum vs Redshift Native Performance Comparison
-- Create a native Redshift table for comparison
CREATE TABLE native_sales (
    sale_id BIGINT,
    product_id VARCHAR(50),
    customer_id VARCHAR(50) DISTKEY,
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER,
    category VARCHAR(50)
) DISTKEY (customer_id)
COMPOUND SORTKEY (sale_date, region, store_id);

-- Compare query performance between Spectrum and native tables
-- EXPLAIN ANALYZE
SELECT * FROM native_sales 
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West'
LIMIT 1000;

-- EXPLAIN ANALYZE
SELECT * FROM spectrum_sales_parquet 
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West'
LIMIT 1000;

-- Spectrum Use Cases:
-- - Querying large datasets that don't need to be loaded into Redshift
-- - Integrating with data lakes and S3-based data pipelines
-- - Cost-effective querying of historical data
-- - Federated queries across multiple data sources
-- - ETL pipeline optimization with staging in S3

-- Clean up (for development purposes)
-- DROP EXTERNAL TABLE IF EXISTS spectrum_sales_csv;
-- DROP EXTERNAL TABLE IF EXISTS spectrum_sales_parquet;
-- DROP EXTERNAL TABLE IF EXISTS spectrum_sales_partitioned;
-- DROP SCHEMA IF EXISTS spectrum_schema CASCADE;