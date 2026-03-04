-- Redshift Incremental Loading Strategies
-- This script demonstrates various incremental loading techniques for Redshift

-- Create source tables for incremental loading demonstration
CREATE TABLE source_orders (
    order_id BIGINT,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE target_orders (
    order_id BIGINT,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTKEY (customer_id)
COMPOUND SORTKEY (order_date);

-- Create staging table for incremental processing
CREATE TABLE staging_orders (
    order_id BIGINT,
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    operation_type VARCHAR(10),  -- 'INSERT', 'UPDATE', 'DELETE'
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Strategy 1: Timestamp-based incremental loading
-- Load records modified since last load timestamp
CREATE OR REPLACE PROCEDURE incremental_load_by_timestamp()
AS $$
DECLARE
    last_load_time TIMESTAMP;
BEGIN
    -- Get the last load time from target table
    SELECT MAX(loaded_at) INTO last_load_time FROM target_orders;
    
    IF last_load_time IS NULL THEN
        -- Initial load - all records
        INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM source_orders
        WHERE order_date <= CURRENT_TIMESTAMP;
    ELSE
        -- Incremental load - records modified since last load
        INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM source_orders
        WHERE order_date > last_load_time
          AND order_date <= CURRENT_TIMESTAMP;
    END IF;
    
    -- Update loaded timestamp
    UPDATE target_orders SET loaded_at = CURRENT_TIMESTAMP
    WHERE loaded_at IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Strategy 2: ID-based incremental loading
-- Load records with IDs greater than last processed ID
CREATE OR REPLACE PROCEDURE incremental_load_by_id()
AS $$
DECLARE
    last_order_id BIGINT;
BEGIN
    -- Get the last order ID from target table
    SELECT MAX(order_id) INTO last_order_id FROM target_orders;
    
    IF last_order_id IS NULL THEN
        -- Initial load - all records
        INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM source_orders
        ORDER BY order_id;
    ELSE
        -- Incremental load - records with IDs greater than last processed
        INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM source_orders
        WHERE order_id > last_order_id
        ORDER BY order_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Strategy 3: Change Data Capture (CDC) with staging
-- Process changes through staging table for upsert operations
CREATE OR REPLACE PROCEDURE cdc_incremental_load()
AS $$
BEGIN
    -- Step 1: Identify new and updated records
    INSERT INTO staging_orders (order_id, customer_id, order_date, total_amount, status, operation_type)
    SELECT 
        order_id, 
        customer_id, 
        order_date, 
        total_amount, 
        status,
        CASE 
            WHEN NOT EXISTS (SELECT 1 FROM target_orders t WHERE t.order_id = s.order_id) THEN 'INSERT'
            WHEN EXISTS (SELECT 1 FROM target_orders t WHERE t.order_id = s.order_id) 
              AND (t.status != s.status OR t.total_amount != s.total_amount) THEN 'UPDATE'
            ELSE 'INSERT'
        END as operation_type
    FROM source_orders s
    LEFT JOIN target_orders t ON s.order_id = t.order_id
    WHERE s.created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'  -- Process recent changes
      OR (t.order_id IS NOT NULL AND (t.status != s.status OR t.total_amount != s.total_amount));
    
    -- Step 2: Apply changes to target table
    -- Insert new records
    INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
    SELECT order_id, customer_id, order_date, total_amount, status
    FROM staging_orders
    WHERE operation_type = 'INSERT';
    
    -- Update existing records
    UPDATE target_orders
    SET 
        customer_id = s.customer_id,
        order_date = s.order_date,
        total_amount = s.total_amount,
        status = s.status,
        loaded_at = CURRENT_TIMESTAMP
    FROM staging_orders s
    WHERE target_orders.order_id = s.order_id
      AND s.operation_type = 'UPDATE';
    
    -- Clean up staging table
    DELETE FROM staging_orders WHERE processed_at < CURRENT_TIMESTAMP - INTERVAL '1 hour';
END;
$$ LANGUAGE plpgsql;

-- Strategy 4: Delta loading with window functions
-- Load only changed records using window functions
CREATE OR REPLACE PROCEDURE delta_loading()
AS $$
BEGIN
    -- Use window functions to identify changed records
    INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
    SELECT 
        order_id, 
        customer_id, 
        order_date, 
        total_amount, 
        status
    FROM (
        SELECT 
            order_id, 
            customer_id, 
            order_date, 
            total_amount, 
            status,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) as rn
        FROM source_orders
        WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
    ) ranked
    WHERE rn = 1  -- Only the latest version of each record
      AND NOT EXISTS (
          SELECT 1 FROM target_orders t 
          WHERE t.order_id = ranked.order_id 
          AND t.created_at >= ranked.created_at - INTERVAL '5 minutes'
      );
END;
$$ LANGUAGE plpgsql;

-- Strategy 5: Batch incremental loading
-- Process data in batches for better performance
CREATE OR REPLACE PROCEDURE batch_incremental_load(batch_size INTEGER DEFAULT 1000)
AS $$
DECLARE
    last_order_id BIGINT := 0;
    records_processed INTEGER := 0;
    total_records INTEGER := 0;
BEGIN
    -- Get total records to process
    SELECT COUNT(*) INTO total_records FROM source_orders WHERE order_id > last_order_id;
    
    -- Process in batches
    WHILE records_processed < total_records LOOP
        INSERT INTO target_orders (order_id, customer_id, order_date, total_amount, status)
        SELECT order_id, customer_id, order_date, total_amount, status
        FROM source_orders
        WHERE order_id > last_order_id
        ORDER BY order_id
        LIMIT batch_size;
        
        -- Update last processed ID
        SELECT MAX(order_id) INTO last_order_id FROM target_orders;
        
        -- Update counter
        records_processed := records_processed + batch_size;
        
        -- Commit after each batch
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Incremental Loading Best Practices:

-- 1. Choose the right strategy based on your data characteristics:
-- - Timestamp-based: Good for time-series data with clear modification times
-- - ID-based: Good for sequentially generated IDs
-- - CDC: Best for complex change tracking
-- - Delta loading: Good for high-frequency updates
-- - Batch loading: Good for large datasets

-- 2. Performance optimization:
-- - Use appropriate distribution and sort keys
-- - Process data in batches for large datasets
-- - Use staging tables for complex transformations
-- - Implement proper error handling and retry logic
-- - Monitor and optimize query performance

-- 3. Data quality and consistency:
-- - Implement proper validation checks
-- - Handle duplicate records appropriately
-- - Maintain data integrity constraints
-- - Implement proper error logging
-- - Consider data freshness requirements

-- 4. Monitoring and maintenance:
-- - Track load performance and success rates
-- - Monitor data growth and storage usage
-- - Implement proper backup and recovery procedures
-- - Regular maintenance of indexes and statistics
-- - Performance tuning based on workload patterns

-- Example of monitoring incremental loads
-- Query to track load performance
SELECT 
    procedure_name,
    call_time,
    total_time,
    rows_processed,
    success_flag
FROM stl_load_history
WHERE call_time > NOW() - INTERVAL '24 hours'
ORDER BY call_time DESC;

-- Clean up (for development purposes)
-- DROP TABLE IF EXISTS source_orders;
-- DROP TABLE IF EXISTS target_orders;
-- DROP TABLE IF EXISTS staging_orders;
-- DROP PROCEDURE IF EXISTS incremental_load_by_timestamp();
-- DROP PROCEDURE IF EXISTS incremental_load_by_id();
-- DROP PROCEDURE IF EXISTS cdc_incremental_load();
-- DROP PROCEDURE IF EXISTS delta_loading();
-- DROP PROCEDURE IF EXISTS batch_incremental_load();