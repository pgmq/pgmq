-- Active: 1762355844622@@127.0.0.1@5432@postgres
-- Manual test script for advanced JSON filtering
-- Run this with: psql -d your_database -f test_advanced_filtering.sql

-- Ensure extension is installed and up to date
CREATE EXTENSION IF NOT EXISTS pgmq;
-- If upgrading from 1.8.0, run: ALTER EXTENSION pgmq UPDATE TO '1.9.0';

-- Create test queue
SELECT pgmq.create('test_filter');

-- Send test messages
SELECT pgmq.send('test_filter', '{"name": "Alice", "age": 25}');
SELECT pgmq.send('test_filter', '{"name": "Bob", "age": 30}');
SELECT pgmq.send('test_filter', '{"name": "Charlie", "age": 20}');

-- Test 1: Simple containment (backward compatible)
SELECT 'Test 1: Simple containment' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"name": "Bob"}');
-- Expected: Should return message with name="Alice"

-- Reset VT for all messages
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);

-- Test 2: Greater than operator
SELECT 'Test 2: age > 20' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "age", "operator": "<=", "value": 20}');
-- Expected: Should return Alice (25) and Bob (30)

-- Reset VT
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);

-- Test 3: Less than operator
SELECT 'Test 3: age < 30' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "age", "operator": "<", "value": 30}');
-- Expected: Should return Alice (25) and Charlie (20)

-- Reset VT
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);

-- Test 4: Equality operator
SELECT 'Test 4: age = 30' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "age", "operator": "=", "value": 30}');
-- Expected: Should return Bob (30)

-- Reset VT
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);

-- Test 5: Exists operator
SELECT pgmq.send('test_filter', '{"name": "David", "city": "NYC"}') AS msg4 \gset
SELECT pgmq.send('test_filter', '{"name": "Eve"}') AS msg5 \gset

-- Reset VT for all
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);
SELECT pgmq.set_vt('test_filter', :msg4, 0);
SELECT pgmq.set_vt('test_filter', :msg5, 0);

SELECT 'Test 5: age exists' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "age", "operator": "exists"}');
-- Expected: Should return Alice, Bob, Charlie (all have age field)

-- Reset VT
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);
SELECT pgmq.set_vt('test_filter', :msg4, 0);
SELECT pgmq.set_vt('test_filter', :msg5, 0);

-- Test 6: Nested fields
SELECT pgmq.send('test_filter', '{"user": {"name": "Frank", "age": 35}}') AS msg6 \gset
SELECT pgmq.set_vt('test_filter', :msg6, 0);

SELECT 'Test 6: nested field user.age > 30' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "user.age", "operator": ">", "value": 30}');
-- Expected: Should return Frank (user.age = 35)

-- Test 7: String comparison
SELECT pgmq.send('test_filter', '{"status": "active", "priority": "high"}') AS msg7 \gset
SELECT pgmq.set_vt('test_filter', :msg7, 0);

SELECT 'Test 7: status = "active"' AS test;
SELECT msg_id, message FROM pgmq.read('test_filter', 0, 10, '{"field": "status", "operator": "=", "value": "active"}');
-- Expected: Should return message with status="active"

-- Test 8: Empty filter (should return all)
SELECT pgmq.set_vt('test_filter', :msg1, 0);
SELECT pgmq.set_vt('test_filter', :msg2, 0);
SELECT pgmq.set_vt('test_filter', :msg3, 0);

SELECT 'Test 8: Empty filter {}' AS test;
SELECT COUNT(*) FROM pgmq.read('test_filter', 0, 10, '{}');
-- Expected: Should return all visible messages

-- Test 9: read_with_poll with filter
SELECT pgmq.create('test_poll');
SELECT pgmq.send('test_poll', '{"age": 25}');
SELECT pgmq.send('test_poll', '{"age": 30}');

SELECT 'Test 9: read_with_poll with filter' AS test;
SELECT msg_id, message FROM pgmq.read_with_poll('test_poll', 0, 10, 1, 100, '{"field": "age", "operator": ">", "value": 20}');
-- Expected: Should return messages with age > 20

-- Test 10: Error cases
SELECT 'Test 10: Invalid operator (should error)' AS test;
DO $$
BEGIN
    PERFORM pgmq.read('test_filter', 0, 1, '{"field": "age", "operator": "invalid", "value": 25}');
    RAISE EXCEPTION 'Should have raised an error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Correctly raised error: %', SQLERRM;
END $$;

-- Cleanup
SELECT pgmq.drop_queue('test_filter');
SELECT pgmq.drop_queue('test_poll');

SELECT 'All tests completed!' AS result;

