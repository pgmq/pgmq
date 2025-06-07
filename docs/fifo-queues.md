# FIFO Queues

PGMQ supports FIFO (First-In-First-Out) queues with message group keys, similar to AWS SQS FIFO queues. This feature allows you to ensure strict ordering of messages within logical groups while still allowing parallel processing across different groups.

## Overview

FIFO queues in PGMQ work by using message headers to specify group identifiers. Messages with the same group ID are processed in strict order, while messages from different groups can be processed in parallel.

### Key Features

- **Strict ordering within groups**: Messages with the same FIFO group ID are processed in the exact order they were sent
- **Parallel processing across groups**: Different FIFO groups can be processed simultaneously
- **Backward compatibility**: Existing queues work unchanged; FIFO is opt-in via headers
- **Visibility timeout support**: FIFO respects visibility timeouts to prevent duplicate processing
- **Performance optimized**: Uses efficient indexing for FIFO group lookups

## How It Works

### Message Group IDs

FIFO ordering is controlled by the `x-pgmq-fifo` header value:

```sql
-- Send messages to the same FIFO group
SELECT pgmq.send('my_queue', '{"order": 1}', '{"x-pgmq-fifo": "user123"}');
SELECT pgmq.send('my_queue', '{"order": 2}', '{"x-pgmq-fifo": "user123"}');

-- Send message to different FIFO group
SELECT pgmq.send('my_queue', '{"order": 1}', '{"x-pgmq-fifo": "user456"}');
```

### Reading FIFO Messages

Use `pgmq.read_fifo()` instead of `pgmq.read()` to respect FIFO ordering:

```sql
-- Read with FIFO ordering
SELECT * FROM pgmq.read_fifo('my_queue', 30, 5);
```

This will return:
- The oldest unprocessed message from each FIFO group
- Up to the requested quantity of messages
- Messages from different groups in parallel

### Default Group Behavior

Messages without the `x-pgmq-fifo` header are treated as belonging to a single default group:

```sql
-- These messages will be processed in FIFO order relative to each other
SELECT pgmq.send('my_queue', '{"message": "first"}');
SELECT pgmq.send('my_queue', '{"message": "second"}');
```

## API Reference

### Reading Functions

#### `pgmq.read_fifo(queue_name, vt, qty, conditional)`

Read messages while respecting FIFO ordering within groups.

**Parameters:**
- `queue_name` (text): Name of the queue
- `vt` (integer): Visibility timeout in seconds
- `qty` (integer): Maximum number of messages to read
- `conditional` (jsonb): Optional message filtering

#### `pgmq.read_fifo_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms, conditional)`

Same as `read_fifo()` but with polling support for real-time processing.

#### `pgmq.read_fifo_sqs_style(queue_name, vt, qty, conditional)`

Read messages with AWS SQS FIFO-style batch retrieval behavior. Unlike `read_fifo()` which returns at most one message per group, this function attempts to return as many messages as possible from the same message group to maximize throughput for related messages.

**Behavior:**
- Prioritizes filling the batch from the earliest message group first
- Returns multiple messages from the same group when available
- Only moves to other groups if the batch cannot be filled from the first group
- Maintains strict FIFO ordering within each group

#### `pgmq.read_fifo_sqs_style_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms, conditional)`

Same as `read_fifo_sqs_style()` but with polling support for real-time processing.

### Utility Functions

#### `pgmq.create_fifo_index(queue_name)`

Creates a GIN index on the headers column to improve FIFO read performance. Recommended when using FIFO functionality frequently.

#### `pgmq.create_fifo_indexes_all()`

Creates FIFO indexes on all existing queues.

## Usage Patterns

### 1. User-Specific Processing

Ensure messages for each user are processed in order:

```sql
-- User 1 messages
SELECT pgmq.send('user_events', '{"action": "login"}', '{"x-pgmq-fifo": "user_123"}');
SELECT pgmq.send('user_events', '{"action": "purchase"}', '{"x-pgmq-fifo": "user_123"}');

-- User 2 messages (can be processed in parallel)
SELECT pgmq.send('user_events', '{"action": "login"}', '{"x-pgmq-fifo": "user_456"}');
```

### 2. Order Processing

Maintain order integrity for financial transactions:

```sql
-- Order lifecycle events
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "create"}', '{"x-pgmq-fifo": "ord_1"}');
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "payment"}', '{"x-pgmq-fifo": "ord_1"}');
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "fulfill"}', '{"x-pgmq-fifo": "ord_1"}');
```

### 3. Document Processing

Process document versions in sequence:

```sql
-- Document updates
SELECT pgmq.send('docs', '{"doc_id": "doc_1", "version": 1}', '{"x-pgmq-fifo": "doc_1"}');
SELECT pgmq.send('docs', '{"doc_id": "doc_1", "version": 2}', '{"x-pgmq-fifo": "doc_1"}');
```

## Performance Considerations

### Indexing

Create FIFO indexes for better performance:

```sql
-- For a specific queue
SELECT pgmq.create_fifo_index('my_queue');

-- For all queues
SELECT pgmq.create_fifo_indexes_all();
```

### Group Distribution

- **Good**: Many small groups with few messages each
- **Avoid**: Few large groups with many messages (reduces parallelism)

### Message Processing

- Process and delete/archive messages promptly to avoid blocking subsequent messages
- Use appropriate visibility timeouts to handle processing failures
- Monitor queue metrics to identify bottlenecks

## Error Handling

### Visibility Timeout Expiry

If message processing fails, the visibility timeout will expire and the message becomes available again:

```sql
-- Message fails processing, timeout expires
-- Next read_fifo() call will return the same message for retry
```

### Manual Retry

Force a message to be immediately available:

```sql
-- Set visibility timeout to 0 for immediate retry
SELECT pgmq.set_vt('my_queue', 123, 0);
```

### Dead Letter Handling

Archive messages that fail repeatedly:

```sql
-- After max retries, archive the problematic message
SELECT pgmq.archive('my_queue', 123);
```

## Migration from Regular Queues

FIFO functionality is backward compatible:

1. **Existing code continues to work**: `pgmq.read()` functions unchanged
2. **Gradual adoption**: Start using `pgmq.read_fifo()` for new consumers
3. **Mixed usage**: Some consumers can use FIFO, others regular reads
4. **Performance**: Add FIFO indexes when ready to optimize

## FIFO Reading Strategies

PGMQ provides two different FIFO reading strategies to suit different use cases:

### Fair Distribution (`pgmq.read_fifo()`)

Returns at most one message per FIFO group per call:

```sql
-- With groups A (5 messages), B (3 messages), C (2 messages)
SELECT * FROM pgmq.read_fifo('queue', 30, 10);
-- Returns: 3 messages (1 from each group)
```

**Best for:**
- Ensuring fair processing across all groups
- Preventing starvation of groups with fewer messages
- Load balancing across different workflows

### Throughput Optimization (`pgmq.read_fifo_sqs_style()`)

Attempts to fill the batch from the earliest group first:

```sql
-- With groups A (5 messages), B (3 messages), C (2 messages)
SELECT * FROM pgmq.read_fifo_sqs_style('queue', 30, 10);
-- Returns: 10 messages (5 from A + 3 from B + 2 from C)

SELECT * FROM pgmq.read_fifo_sqs_style('queue', 30, 3);
-- Returns: 3 messages (all from group A)
```

**Best for:**
- Maximizing throughput for related messages
- Processing workflows where batching related messages is beneficial
- Mimicking AWS SQS FIFO behavior exactly

### Choosing the Right Strategy

| Scenario | Recommended Function | Reason |
|----------|---------------------|---------|
| Multi-tenant processing | `read_fifo()` | Ensures fair resource allocation |
| Order processing pipeline | `read_fifo_sqs_style()` | Related orders processed together |
| User activity streams | `read_fifo()` | Prevents one active user from blocking others |
| Document workflows | `read_fifo_sqs_style()` | Process all versions of a document together |
| Financial transactions | `read_fifo_sqs_style()` | Batch related transactions for efficiency |

## Comparison with AWS SQS FIFO

| Feature | PGMQ FIFO | PGMQ SQS-Style | AWS SQS FIFO |
|---------|-----------|----------------|--------------|
| Group-based ordering | ✅ | ✅ | ✅ |
| Parallel group processing | ✅ | ✅ | ✅ |
| Batch retrieval strategy | Fair (1 per group) | Throughput-optimized | Throughput-optimized |
| Message deduplication | ❌ | ❌ | ✅ |
| Throughput limits | No limits | No limits | 300 TPS per group |
| Exactly-once delivery | ❌ | ❌ | ✅ |
| Cost | Free | Free | Pay per request |

## Best Practices

1. **Choose appropriate group keys**: Balance between ordering requirements and parallelism
2. **Create FIFO indexes**: Improve performance for frequently used queues
3. **Monitor group distribution**: Ensure even distribution across groups
4. **Handle failures gracefully**: Implement retry logic and dead letter handling
5. **Test thoroughly**: Verify ordering behavior under load
6. **Use meaningful group IDs**: Make debugging and monitoring easier

## Examples

See [examples/fifo_example.sql](../examples/fifo_example.sql) for comprehensive usage examples.
