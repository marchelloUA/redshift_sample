-- Redshift Distribution Styles and Keys
-- This script demonstrates different distribution styles and their performance implications

-- Create a table with EVEN distribution (default)
-- Best for: Tables with relatively uniform data access patterns
-- Performance: Good for general purpose queries, but can cause data skew for certain join operations
CREATE TABLE sales_even (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50),
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER
) DISTSTYLE EVEN;

-- Create a table with ALL distribution
-- Best for: Small reference tables that are frequently joined with large fact tables
-- Performance: Eliminates data movement for joins, but duplicates data across all nodes
CREATE TABLE product_categories_all (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100),
    department VARCHAR(50),
    parent_category VARCHAR(50)
) DISTSTYLE ALL;

-- Create a table with KEY distribution
-- Best for: Tables with a natural distribution key that optimizes join performance
-- Performance: Reduces data movement for joins on the distribution key
CREATE TABLE customers_key (
    customer_id VARCHAR(50) DISTKEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    registration_date TIMESTAMP,
    last_login_date TIMESTAMP,
    total_purchases DECIMAL(12,2),
    preferred_store_id INTEGER
) DISTKEY (customer_id);

-- Create a table with AUTO distribution (Redshift Spectrum/Serverless)
-- Best for: Modern Redshift environments with automatic distribution optimization
CREATE TABLE orders_auto (
    order_id BIGINT IDENTITY(1,1),
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    shipping_address TEXT
) DISTSTYLE AUTO;

-- Performance optimization: Distribution key selection strategy
-- Key considerations for choosing distribution keys:
-- 1. Choose columns that are frequently used in JOIN clauses
-- 2. Choose columns with high cardinality (many distinct values)
-- 3. Avoid columns with low cardinality (few distinct values) as distribution keys
-- 4. Consider query patterns - which joins are most performance-critical

-- Example of optimal distribution key selection for sales data
CREATE TABLE sales_optimized (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50) DISTKEY,  -- High cardinality, frequently joined
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER
) DISTKEY (customer_id);

-- Multi-table join optimization with consistent distribution keys
-- When multiple tables share the same distribution key, joins are more efficient
CREATE TABLE customer_orders (
    order_id BIGINT IDENTITY(1,1),
    customer_id VARCHAR(50) DISTKEY,
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20)
) DISTKEY (customer_id);

CREATE TABLE customer_payments (
    payment_id BIGINT IDENTITY(1,1),
    order_id BIGINT,
    customer_id VARCHAR(50) DISTKEY,  -- Same distribution key as customer_orders
    payment_amount DECIMAL(10,2),
    payment_date TIMESTAMP,
    payment_method VARCHAR(50)
) DISTKEY (customer_id);