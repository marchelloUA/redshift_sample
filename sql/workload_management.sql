-- Redshift Workload Management (WLM)
-- This script demonstrates WLM configuration and query prioritization

-- Create sample tables for workload demonstration
CREATE TABLE high_priority_data (
    transaction_id BIGINT IDENTITY(1,1),
    customer_id VARCHAR(50) DISTKEY,
    amount DECIMAL(12,2),
    transaction_date TIMESTAMP,
    status VARCHAR(20),
    priority_level VARCHAR(10)
) DISTKEY (customer_id)
COMPOUND SORTKEY (transaction_date, priority_level);

CREATE TABLE batch_processing_data (
    batch_id BIGINT IDENTITY(1,1),
    batch_name VARCHAR(100),
    processing_date TIMESTAMP,
    status VARCHAR(20),
    records_processed INTEGER,
    processing_time_seconds INTEGER
) DISTSTYLE EVEN;

-- WLM Query Classification Examples
-- These queries would typically be classified into different WLM queues

-- High priority query (real-time analytics)
SELECT 
    customer_id,
    SUM(amount) as total_transactions,
    COUNT(*) as transaction_count,
    MAX(transaction_date) as last_transaction
FROM high_priority_data
WHERE transaction_date > NOW() - INTERVAL '1 hour'
  AND status = 'completed'
GROUP BY customer_id
ORDER BY total_transactions DESC
LIMIT 100;

-- Medium priority query (daily reporting)
SELECT 
    DATE_TRUNC('day', transaction_date) as transaction_day,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_amount,
    AVG(amount) as avg_transaction_amount
FROM high_priority_data
WHERE transaction_date > NOW() - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', transaction_date)
ORDER BY transaction_day DESC;

-- Low priority query (batch processing)
SELECT 
    batch_name,
    processing_date,
    status,
    records_processed,
    processing_time_seconds,
    records_processed / NULLIF(processing_time_seconds, 0) as records_per_second
FROM batch_processing_data
WHERE processing_date > NOW() - INTERVAL '7 days'
ORDER BY processing_date DESC;

-- WLM Configuration Examples
-- Note: These are examples of WLM queries, not actual WLM configuration

-- Query to monitor current WLM queue usage
SELECT 
    service_class,
    query,
    start_time,
    end_time,
    total_time,
    rows,
    bytes,
    is_rrscan
FROM stv_wlm_query_state
WHERE start_time > NOW() - INTERVAL '1 hour'
ORDER BY start_time DESC;

-- Query to analyze query performance by service class
SELECT 
    service_class,
    COUNT(*) as query_count,
    AVG(total_time) as avg_execution_time,
    MAX(total_time) as max_execution_time,
    SUM(rows) as total_rows_processed,
    SUM(bytes) as total_bytes_processed
FROM stv_wlm_query_state
WHERE start_time > NOW() - INTERVAL '24 hours'
GROUP BY service_class
ORDER BY avg_execution_time DESC;

-- WLM Best Practices:

-- 1. Query Classification
-- Classify queries based on:
-- - Business priority (real-time vs batch)
-- - Resource requirements (memory, CPU)
-- - Expected execution time
-- - Data access patterns

-- 2. Queue Configuration
-- - Set appropriate memory limits for each queue
-- - Configure concurrency limits
-- - Set timeout thresholds
-- - Configure user and group assignments

-- 3. Resource Management
-- - Monitor memory usage per query
-- - Set query timeouts
-- - Configure user-level resource limits
-- - Implement query queuing

-- 4. Performance Monitoring
-- - Track query execution times
-- - Monitor queue wait times
-- - Analyze resource utilization
-- - Identify performance bottlenecks

-- Example WLM Query Analysis
-- Identify queries that are consuming excessive resources
SELECT 
    query,
    service_class,
    start_time,
    total_time,
    rows,
    bytes,
    workmem,
    temp_blocks,
    is_rrscan
FROM stv_wlm_query_state
WHERE total_time > 5000  -- Queries taking more than 5 seconds
  AND start_time > NOW() - INTERVAL '1 hour'
ORDER BY total_time DESC
LIMIT 20;

-- Query to identify memory-intensive queries
SELECT 
    query,
    service_class,
    workmem,
    temp_blocks,
    rows,
    bytes
FROM stv_wlm_query_state
WHERE workmem > 1000000  -- Queries using more than 1MB of memory
  AND start_time > NOW() - INTERVAL '1 hour'
ORDER BY workmem DESC
LIMIT 20;

-- WLM Optimization Strategies:

-- 1. Short Query Acceleration
-- - Configure separate queue for short-running queries
-- - Set lower memory limits for short queries
-- - Enable query result caching

-- 2. Long Query Management
-- - Configure separate queue for long-running queries
-- - Set appropriate memory limits
-- - Implement query timeouts
-- - Monitor progress regularly

-- 3. User-Based Resource Management
-- - Configure different queues for different user groups
-- - Set resource limits per user
-- - Implement priority-based scheduling

-- 4. Concurrency Control
-- - Set appropriate concurrency limits per queue
-- - Monitor queue wait times
-- - Adjust concurrency based on workload

-- 5. Memory Management
-- - Configure memory limits based on query complexity
-- - Monitor memory usage patterns
-- - Adjust WLM settings based on workload analysis

-- Example of WLM Performance Tuning
-- Query to analyze queue performance over time
SELECT 
    DATE_TRUNC('hour', start_time) as hour,
    service_class,
    COUNT(*) as query_count,
    AVG(total_time) as avg_execution_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_time) as p95_execution_time
FROM stv_wlm_query_state
WHERE start_time > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', start_time), service_class
ORDER BY hour, service_class;

-- WLM Maintenance Tasks:
-- - Regular review of query performance
-- - Update WLM configuration based on changing workloads
-- - Monitor queue wait times and adjust concurrency
-- - Analyze memory usage patterns and adjust limits
-- - Review and update query classification rules

-- Clean up (for development purposes)
-- DROP TABLE IF EXISTS high_priority_data;
-- DROP TABLE IF EXISTS batch_processing_data;