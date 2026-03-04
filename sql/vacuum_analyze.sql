-- Redshift Vacuum and Analyze Operations
-- This script demonstrates proper maintenance procedures for Redshift

-- Create sample tables with delete/update operations to demonstrate vacuum needs
CREATE TABLE sales_with_deletes (
    sale_id BIGINT IDENTITY(1,1),
    product_id VARCHAR(50),
    customer_id VARCHAR(50) DISTKEY,
    sale_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    region VARCHAR(50),
    store_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE
) DISTKEY (customer_id)
COMPOUND SORTKEY (sale_date, region);

CREATE TABLE large_fact_table (
    fact_id BIGINT IDENTITY(1,1),
    dimension_id VARCHAR(50) DISTKEY,
    metric_value DECIMAL(12,4),
    timestamp TIMESTAMP,
    category VARCHAR(50),
    source_system VARCHAR(50)
) DISTKEY (dimension_id)
COMPOUND SORTKEY (timestamp, category);

-- Insert sample data
INSERT INTO sales_with_deletes (product_id, customer_id, sale_amount, sale_date, region, store_id, is_active)
SELECT 
    'PROD_' || (random() * 1000)::INT,
    'CUST_' || (random() * 1000)::INT,
    (random() * 1000)::DECIMAL(10,2),
    NOW() - (random() * 365 * 24 * 60 * 60)::INTERVAL,
    CASE WHEN random() < 0.3 THEN 'West' WHEN random() < 0.6 THEN 'East' ELSE 'Central' END,
    (random() * 100)::INT,
    CASE WHEN random() < 0.1 THEN FALSE ELSE TRUE END
FROM generate_series(1, 100000);

INSERT INTO large_fact_table (dimension_id, metric_value, timestamp, category, source_system)
SELECT 
    'DIM_' || (random() * 500)::INT,
    (random() * 1000)::DECIMAL(12,4),
        NOW() - (random() * 365 * 24 * 60 * 60)::INTERVAL,
    CASE WHEN random() < 0.25 THEN 'A' WHEN random() < 0.5 THEN 'B' WHEN random() < 0.75 THEN 'C' ELSE 'D' END,
    CASE WHEN random() < 0.5 THEN 'SYS1' ELSE 'SYS2' END
FROM generate_series(1, 500000);

-- Demonstrate delete operations (creates deleted rows that need vacuum)
DELETE FROM sales_with_deletes WHERE is_active = FALSE;
DELETE FROM large_fact_table WHERE metric_value < 100;

-- Vacuum and Analyze Procedures

-- Procedure for basic vacuum and analyze
CREATE OR REPLACE PROCEDURE basic_maintenance()
AS $$
BEGIN
    -- Vacuum tables to remove deleted rows and reclaim space
    VACUUM sales_with_deletes;
    VACUUM large_fact_table;
    
    -- Analyze tables to update statistics for query optimization
    ANALYZE sales_with_deletes;
    ANALYZE large_fact_table;
    
    RAISE NOTICE 'Basic maintenance completed successfully';
END;
$$ LANGUAGE plpgsql;

-- Procedure for selective vacuum (more efficient)
CREATE OR REPLACE PROCEDURE selective_vacuum()
AS $$
BEGIN
    -- Check table statistics before vacuum
    RAISE NOTICE 'Before vacuum - deleted rows: %', 
        (SELECT COUNT(*) FROM stv_deleted_rows WHERE tbl = 'sales_with_deletes'::regclass);
    
    -- Vacuum only tables that need it
    -- In practice, you would check stv_deleted_rows or monitor table statistics
    VACUUM sales_with_deletes;
    
    -- Check table statistics after vacuum
    RAISE NOTICE 'After vacuum - deleted rows: %', 
        (SELECT COUNT(*) FROM stv_deleted_rows WHERE tbl = 'sales_with_deletes'::regclass);
    
    -- Analyze to update statistics
    ANALYZE sales_with_deletes;
    
    RAISE NOTICE 'Selective vacuum completed successfully';
END;
$$ LANGUAGE plpgsql;

-- Procedure for aggressive vacuum (for heavily updated tables)
CREATE OR REPLACE PROCEDURE aggressive_vacuum()
AS $$
BEGIN
    -- Vacuum with FULL option to reclaim maximum space
    -- Note: This is more resource-intensive but more thorough
    VACUUM FULL sales_with_deletes;
    VACUUM FULL large_fact_table;
    
    -- Analyze to update statistics
    ANALYZE sales_with_deletes;
    ANALYZE large_fact_table;
    
    RAISE NOTICE 'Aggressive vacuum completed successfully';
END;
$$ LANGUAGE plpgsql;

-- Procedure for scheduled maintenance
CREATE OR REPLACE PROCEDURE scheduled_maintenance()
AS $$
DECLARE
    table_name TEXT;
    vacuum_needed BOOLEAN;
BEGIN
    -- Iterate through all tables and check if they need vacuum
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
          AND tablename NOT LIKE 'pg_%'
          AND tablename NOT LIKE 'stv_%'
    LOOP
        -- Check if table has significant deleted rows
        -- This is a simplified check - in practice you'd use more sophisticated logic
        BEGIN
            EXECUTE format('SELECT COUNT(*) > 1000 FROM stv_deleted_rows WHERE tbl = %L::regclass', table_name)
            INTO vacuum_needed;
            
            IF vacuum_needed THEN
                EXECUTE format('VACUUM %I', table_name);
                EXECUTE format('ANALYZE %I', table_name);
                RAISE NOTICE 'Vacuumed table: %', table_name;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Error processing table %: %', table_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE 'Scheduled maintenance completed successfully';
END;
$$ LANGUAGE plpgsql;

-- Vacuum and Analyze Best Practices:

-- 1. When to vacuum:
-- - Tables with frequent DELETE/UPDATE operations
-- - Tables with high churn rates
-- - Tables showing performance degradation
-- - Tables with significant deleted rows (check stv_deleted_rows)
-- - During maintenance windows (low activity periods)

-- 2. When to analyze:
-- - After large data loads
-- - After significant data changes
-- - Before complex queries
-- - When query performance degrades
-- - During maintenance windows

-- 3. Vacuum strategies:
-- - Basic VACUUM: Removes deleted rows, reclaims some space
-- - VACUUM FULL: Reclaims maximum space, more resource-intensive
-- - Selective VACUUM: Only vacuum tables that need it
-- - Scheduled VACUUM: Regular maintenance during off-peak hours

-- 4. Analyze strategies:
-- - Basic ANALYZE: Updates table statistics
-- - ANALYZE VERBOSE: Updates detailed statistics
-- - Column-specific ANALYZE: Update statistics for specific columns
-- - Scheduled ANALYZE: Regular statistics updates

-- Monitoring vacuum and analyze operations:

-- Query to check vacuum progress
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
WHERE query LIKE '%VACUUM%' 
  OR query LIKE '%ANALYZE%'
  AND start_time > NOW() - INTERVAL '24 hours'
ORDER BY start_time DESC;

-- Query to check deleted rows
SELECT 
    schemaname,
    tablename,
    deleted_rows,
    dead_rows
FROM stv_deleted_rows
WHERE schemaname = 'public'
  AND deleted_rows > 0
ORDER BY deleted_rows DESC;

-- Query to check table statistics
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation,
    most_common_vals,
    most_common_freqs
FROM pg_stats
WHERE schemaname = 'public'
  AND tablename IN ('sales_with_deletes', 'large_fact_table')
ORDER BY schemaname, tablename, attname;

-- Performance impact monitoring:
-- Query to analyze query performance before and after maintenance
SELECT 
    query,
    service_class,
    start_time,
    total_time,
    rows,
    bytes
FROM stv_wlm_query_state
WHERE query LIKE '%sales_with_deletes%' 
  AND start_time > NOW() - INTERVAL '48 hours'
ORDER BY start_time DESC;

-- Advanced vacuum techniques:

-- 1. Vacuum with specific parameters
VACUUM (VERBOSE, ANALYZE) sales_with_deletes;

-- 2. Vacuum specific columns
ALTER TABLE sales_with_deletes ALTER COLUMN sale_amount SET STATISTICS 1000;
ANALYZE sales_with_deletes (sale_amount);

-- 3. Vacuum with table locks (for critical operations)
LOCK TABLE sales_with_deletes IN EXCLUSIVE MODE;
VACUUM FULL sales_with_deletes;
ANALYZE sales_with_deletes;

-- 4. Automated vacuum based on table statistics
CREATE OR REPLACE PROCEDURE auto_vacuum_analyze()
AS $$
DECLARE
    table_name TEXT;
    deleted_count INTEGER;
    total_count INTEGER;
    vacuum_threshold FLOAT := 0.1; -- 10% deleted rows threshold
BEGIN
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
          AND tablename NOT LIKE 'pg_%'
          AND tablename NOT LIKE 'stv_%'
    LOOP
        BEGIN
            -- Get deleted and total row counts
            EXECUTE format('
                SELECT 
                    (SELECT COUNT(*) FROM stv_deleted_rows WHERE tbl = %L::regclass),
                    (SELECT COUNT(*) FROM %I)
                ', table_name, table_name)
            INTO deleted_count, total_count;
            
            -- Vacuum if deleted rows exceed threshold
            IF deleted_count > 0 AND total_count > 0 AND 
               (deleted_count::FLOAT / total_count::FLOAT) > vacuum_threshold THEN
                EXECUTE format('VACUUM %I', table_name);
                EXECUTE format('ANALYZE %I', table_name);
                RAISE NOTICE 'Vacuumed table %: % deleted rows out of % total', 
                    table_name, deleted_count, total_count;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Error processing table %: %', table_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE 'Auto vacuum completed successfully';
END;
$$ LANGUAGE plpgsql;

-- Clean up (for development purposes)
-- DROP TABLE IF EXISTS sales_with_deletes;
-- DROP TABLE IF EXISTS large_fact_table;
-- DROP PROCEDURE IF EXISTS basic_maintenance();
-- DROP PROCEDURE IF EXISTS selective_vacuum();
-- DROP PROCEDURE IF EXISTS aggressive_vacuum();
-- DROP PROCEDURE IF EXISTS scheduled_maintenance();
-- DROP PROCEDURE IF EXISTS auto_vacuum_analyze();