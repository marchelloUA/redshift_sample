-- Redshift Sort Keys and Performance Optimization
-- This script demonstrates sort key strategies and their impact on query performance

-- Create table with COMPOUND sort key
-- Best for: Queries that filter on multiple columns in a specific order
-- Performance: Significantly improves query performance when filtering on leading sort columns
CREATE TABLE sales_compound (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER,
    -- Compound sort key: sale_date first (most selective), then region, then store_id
    CONSTRAINT sales_compound_pkey PRIMARY KEY (sale_id)
) DISTKEY (customer_id)
COMPOUND SORTKEY (sale_date, region, store_id);

-- Create table with INTERLEAVED sort key
-- Best for: Queries that filter on multiple columns in different orders
-- Performance: Provides balanced performance across multiple query patterns
CREATE TABLE sales_interleaved (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER,
    -- Interleaved sort key: balances performance for different query patterns
    CONSTRAINT sales_interleaved_pkey PRIMARY KEY (sale_id)
) DISTKEY (customer_id)
INTERLEAVED SORTKEY (sale_date, region, store_id, product_id);

-- Create table with single-column sort key for time-series data
-- Best for: Time-based queries and range scans
CREATE TABLE time_series_data (
    timestamp TIMESTAMP SORTKEY,
    metric_value DECIMAL(12,4),
    metric_name VARCHAR(50),
    device_id VARCHAR(50),
    location VARCHAR(50)
) DISTSTYLE EVEN;

-- Sort key selection strategies:
-- 1. Choose columns that are frequently used in WHERE clauses
-- 2. Place most selective columns first in the sort key
-- 3. For compound sort keys, order by selectivity (most selective first)
-- 4. Use interleaved sort keys when queries filter on different column combinations
-- 5. Consider data access patterns - which queries are most important to optimize

-- Performance comparison queries
-- Query optimized for compound sort key (fast when filtering on sale_date)
SELECT * FROM sales_compound 
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'West';

-- Query optimized for interleaved sort key (flexible performance)
SELECT * FROM sales_interleaved 
WHERE region = 'West' AND store_id = 100;

-- Query optimized for time-series sort key
SELECT * FROM time_series_data 
WHERE timestamp BETWEEN '2024-01-01 00:00:00' AND '2024-01-31 23:59:59';

-- Sort key best practices:
-- - Sort keys should be chosen based on actual query patterns
-- - Leading sort columns have the biggest performance impact
-- - Avoid over-sorting - too many sort columns can slow down data loading
-- - Consider compression benefits - sorted data compresses better
-- - Monitor query performance and adjust sort keys as needed

-- Vacuum and analyze considerations with sort keys
-- Regular maintenance is important to maintain sort key effectiveness
-- Vacuum removes deleted rows and reclaims space
-- Analyze updates statistics for query optimization
-- Example maintenance commands (run during maintenance windows):
-- VACUUM sales_compound;
-- ANALYZE sales_compound;