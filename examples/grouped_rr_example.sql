-- PGMQ FIFO Queue Example
-- This example demonstrates how to use FIFO queues with message group keys

-- Create a queue for order processing
SELECT pgmq.create('order_processing');

-- Create FIFO index for better performance
SELECT pgmq.create_fifo_index('order_processing');

-- Example 1: Basic FIFO ordering within a customer group
-- Send multiple orders for the same customer - these must be processed in order
SELECT pgmq.send('order_processing', 
    '{"customer_id": "cust_123", "order_id": "ord_001", "action": "create", "amount": 100}'::jsonb,
    '{"x-pgmq-group": "cust_123"}'::jsonb
);

SELECT pgmq.send('order_processing', 
    '{"customer_id": "cust_123", "order_id": "ord_001", "action": "update", "amount": 150}'::jsonb,
    '{"x-pgmq-group": "cust_123"}'::jsonb
);

SELECT pgmq.send('order_processing', 
    '{"customer_id": "cust_123", "order_id": "ord_001", "action": "complete"}'::jsonb,
    '{"x-pgmq-group": "cust_123"}'::jsonb
);

-- Example 2: Parallel processing for different customers
-- Send orders for different customers - these can be processed in parallel
SELECT pgmq.send('order_processing', 
    '{"customer_id": "cust_456", "order_id": "ord_002", "action": "create", "amount": 200}'::jsonb,
    '{"x-pgmq-group": "cust_456"}'::jsonb
);

SELECT pgmq.send('order_processing', 
    '{"customer_id": "cust_789", "order_id": "ord_003", "action": "create", "amount": 300}'::jsonb,
    '{"x-pgmq-group": "cust_789"}'::jsonb
);

-- Example 3: Read messages with FIFO ordering
-- This will return the first message from each customer group
SELECT 
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action,
    headers->>'x-pgmq-group' as fifo_group
FROM pgmq.read_grouped_rr('order_processing', 30, 10);

-- Expected result: 3 messages (first from each customer group)
-- - cust_123: create order
-- - cust_456: create order  
-- - cust_789: create order

-- Example 4: Process and delete the first message for cust_123
-- After processing, delete the message to allow the next one in the group
SELECT pgmq.delete('order_processing', 1);

-- Now read again - should get the update message for cust_123
SELECT 
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action,
    headers->>'x-pgmq-group' as fifo_group
FROM pgmq.read_grouped_rr('order_processing', 30, 10);

-- Example 5: Mixed FIFO and non-FIFO messages
-- Send a message without FIFO header (goes to default group)
SELECT pgmq.send('order_processing',
    '{"system_message": "daily_report", "timestamp": "2024-01-01T00:00:00Z"}'::jsonb
);

-- Send another message for cust_123
SELECT pgmq.send('order_processing',
    '{"customer_id": "cust_123", "order_id": "ord_004", "action": "create", "amount": 400}'::jsonb,
    '{"x-pgmq-group": "cust_123"}'::jsonb
);

-- Read with FIFO - will get system message and messages from customer groups
SELECT
    msg_id,
    COALESCE(message->>'customer_id', message->>'system_message') as identifier,
    message->>'action' as action,
    COALESCE(headers->>'x-pgmq-group', 'default') as fifo_group
FROM pgmq.read_grouped_rr('order_processing', 30, 10);

-- Example 6: Using polling for real-time processing
-- This will wait up to 5 seconds for new messages
SELECT
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action,
    headers->>'x-pgmq-group' as fifo_group
FROM pgmq.read_grouped_rr_with_poll('order_processing', 30, 5, 5, 100);

-- Example 7: Visibility timeout behavior
-- Read a message with short timeout to demonstrate blocking
SELECT
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action
FROM pgmq.read_grouped_rr('order_processing', 5, 1);

-- Immediately try to read again - should not get the same customer's next message
-- because the previous message is still being processed (visibility timeout)
SELECT
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action
FROM pgmq.read_grouped_rr('order_processing', 30, 10);

-- Wait for visibility timeout to expire (5 seconds)
SELECT pg_sleep(6);

-- Now read again - should get the message that was previously blocked
SELECT
    msg_id,
    message->>'customer_id' as customer_id,
    message->>'action' as action
FROM pgmq.read_grouped_rr('order_processing', 30, 10);

-- Example 8: Error handling and message retry
-- Set visibility timeout to 0 to make a message immediately available again
SELECT pgmq.set_vt('order_processing', 2, 0);

-- Now the message can be read again
SELECT
    msg_id,
    read_ct,
    message->>'customer_id' as customer_id,
    message->>'action' as action
FROM pgmq.read_grouped('order_processing', 30, 10);

-- Example 9: Queue metrics and monitoring
SELECT * FROM pgmq.metrics('order_processing');

-- Clean up
SELECT pgmq.drop_queue('order_processing');

-- Summary of FIFO behavior:
-- 1. Messages with the same x-pgmq-group header value are processed in strict order
-- 2. Only the oldest unprocessed message from each FIFO group can be read
-- 3. Messages from different FIFO groups can be processed in parallel
-- 4. Messages without FIFO headers are treated as a single default group
-- 5. Visibility timeout still applies - messages being processed block subsequent messages in the same group
-- 6. Use pgmq.create_fifo_index() for better performance when using FIFO frequently
