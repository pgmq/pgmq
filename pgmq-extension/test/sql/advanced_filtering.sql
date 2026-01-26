-- Test advanced JSON filtering functionality
-- This tests the new filtering capabilities added in version 1.9.0

-- Create test queue
SELECT pgmq.create('test_advanced_filtering');

-- Test 1: Simple containment (backward compatibility)
-- Send messages with different structures
SELECT pgmq.send('test_advanced_filtering', '{"name": "Alice", "age": 25}');
SELECT pgmq.send('test_advanced_filtering', '{"name": "Bob", "age": 30}');
SELECT pgmq.send('test_advanced_filtering', '{"name": "Charlie", "age": 20}');

-- Test simple containment filter (should work as before)
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"name": "Alice"}');

-- Reset VT for next tests
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 2: Greater than operator (>)
SELECT msg_id, message FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": ">", "value": 20}');
-- Should return messages with age > 20 (Alice: 25, Bob: 30)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 3: Greater than or equal operator (>=)
SELECT COUNT(*) = 2 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": ">=", "value": 25}');
-- Should return messages with age >= 25 (Alice: 25, Bob: 30)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 4: Less than operator (<)
SELECT COUNT(*) = 2 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "<", "value": 30}');
-- Should return messages with age < 30 (Alice: 25, Charlie: 20)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 5: Less than or equal operator (<=)
SELECT COUNT(*) = 2 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "<=", "value": 25}');
-- Should return messages with age <= 25 (Alice: 25, Charlie: 20)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 6: Equality operator (=)
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "=", "value": 30}');
-- Should return message with age = 30 (Bob)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 7: Not equal operator (!=)
SELECT COUNT(*) = 2 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "!=", "value": 30}');
-- Should return messages with age != 30 (Alice: 25, Charlie: 20)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);

-- Test 8: Exists operator
SELECT pgmq.send('test_advanced_filtering', '{"name": "David", "city": "NYC"}');
SELECT pgmq.send('test_advanced_filtering', '{"name": "Eve"}');

-- Reset VT for all messages
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 4, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 5, 0);

-- Check for age field existence
SELECT COUNT(*) = 3 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "exists"}');
-- Should return messages with age field (Alice, Bob, Charlie)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 4, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 5, 0);

-- Test 9: Nested fields
SELECT pgmq.send('test_advanced_filtering', '{"user": {"name": "Frank", "age": 35, "address": {"city": "LA"}}}');
SELECT pgmq.send('test_advanced_filtering', '{"user": {"name": "Grace", "age": 28}}');

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 1, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 2, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 3, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 4, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 5, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 6, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 7, 0);

-- Test nested field comparison
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "user.age", "operator": ">", "value": 30}');
-- Should return message with user.age > 30 (Frank: 35)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 6, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 7, 0);

-- Test nested field exists
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "user.address.city", "operator": "exists"}');
-- Should return message with user.address.city (Frank)

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 6, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 7, 0);

-- Test 10: String comparison
SELECT pgmq.send('test_advanced_filtering', '{"status": "active", "priority": "high"}');
SELECT pgmq.send('test_advanced_filtering', '{"status": "inactive", "priority": "low"}');

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 8, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 9, 0);

-- Test string equality
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "status", "operator": "=", "value": "active"}');
-- Should return message with status = "active"

-- Reset VT
SELECT pgmq.set_vt('test_advanced_filtering', 8, 0);
SELECT pgmq.set_vt('test_advanced_filtering', 9, 0);

-- Test 11: Empty filter (should return all visible messages)
SELECT COUNT(*) >= 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{}');

-- Test 12: Invalid operator (should raise error)
SELECT pgmq.read('test_advanced_filtering', 0, 1, '{"field": "age", "operator": "invalid", "value": 25}');

-- Test 13: Missing value for comparison operator (should raise error)
SELECT pgmq.read('test_advanced_filtering', 0, 1, '{"field": "age", "operator": ">"}');

-- Test 14: read_with_poll with advanced filtering
SELECT pgmq.create('test_poll_filtering');
SELECT pgmq.send('test_poll_filtering', '{"age": 25}');
SELECT pgmq.send('test_poll_filtering', '{"age": 30}');

-- Test read_with_poll with filter
SELECT COUNT(*) = 1 FROM pgmq.read_with_poll('test_poll_filtering', 0, 10, 1, 100, '{"field": "age", "operator": ">", "value": 20}');
-- Should return messages with age > 20

-- Test 15: Multiple messages matching filter
SELECT pgmq.create('test_multiple_matches');
SELECT pgmq.send('test_multiple_matches', '{"score": 85}');
SELECT pgmq.send('test_multiple_matches', '{"score": 90}');
SELECT pgmq.send('test_multiple_matches', '{"score": 75}');
SELECT pgmq.send('test_multiple_matches', '{"score": 95}');

-- Read messages with score >= 85
SELECT COUNT(*) = 3 FROM pgmq.read('test_multiple_matches', 0, 10, '{"field": "score", "operator": ">=", "value": 85}');
-- Should return 3 messages (85, 90, 95)

-- Test 16: No matches
SELECT pgmq.create('test_no_matches');
SELECT pgmq.send('test_no_matches', '{"value": 10}');
SELECT pgmq.send('test_no_matches', '{"value": 20}');

-- Try to read with filter that matches nothing
SELECT COUNT(*) = 0 FROM pgmq.read('test_no_matches', 0, 10, '{"field": "value", "operator": ">", "value": 100}');

-- Test 17: NULL value handling
SELECT pgmq.send('test_advanced_filtering', '{"name": "NullTest", "age": null}');
SELECT pgmq.set_vt('test_advanced_filtering', (SELECT MAX(msg_id) FROM pgmq.q_test_advanced_filtering), 0);

-- Test NULL comparison
SELECT COUNT(*) = 1 FROM pgmq.read('test_advanced_filtering', 0, 10, '{"field": "age", "operator": "=", "value": null}');

-- Cleanup
SELECT pgmq.drop_queue('test_advanced_filtering');
SELECT pgmq.drop_queue('test_poll_filtering');
SELECT pgmq.drop_queue('test_multiple_matches');
SELECT pgmq.drop_queue('test_no_matches');

