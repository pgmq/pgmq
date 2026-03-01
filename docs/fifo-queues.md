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

FIFO ordering is controlled by the `x-pgmq-group` header value:

```sql
-- Send messages to the same FIFO group
SELECT pgmq.send('my_queue', '{"order": 1}', '{"x-pgmq-group": "user123"}');
SELECT pgmq.send('my_queue', '{"order": 2}', '{"x-pgmq-group": "user123"}');

-- Send message to different FIFO group
SELECT pgmq.send('my_queue', '{"order": 1}', '{"x-pgmq-group": "user456"}');
```

### Reading FIFO Messages

PGMQ provides three FIFO reading strategies. Choose the one that best fits your workload:

- `pgmq.read_grouped_rr(...)` (Round-Robin, layered interleaving): Fairly interleaves messages across groups. Great for multi-tenant and user-centric workloads.
- `pgmq.read_grouped(...)` (SQS-style throughput): Fills batches from the oldest eligible group first, returning multiple messages from the same group for throughput.
- `pgmq.read_grouped_head(...)` (One-per-group): Returns exactly one message per group (the head of each group), up to N groups. Ideal for parallel, per-group processing.

```sql
-- Fair distribution across groups (round-robin, layered)
SELECT * FROM pgmq.read_grouped_rr('my_queue', 30, 5);

-- Throughput-optimized, SQS-style batch filling
SELECT * FROM pgmq.read_grouped('my_queue', 30, 5);

-- One message per group, up to 5 groups
SELECT * FROM pgmq.read_grouped_head('my_queue', 30, 5);
```

Round-robin (RR) will:
- Interleave across groups in layers, preserving order within each group
- Return up to the requested quantity across groups
- Prevent starvation of smaller/less-active groups

SQS-style will:
- Fill the batch from the earliest eligible group first
- Return multiple messages from the same group when available
- Move to other groups only if needed to fill the batch

Head-per-group will:

- Return at most one message per FIFO group (the oldest available message in that group)
- Dispatch up to N groups in a single read call
- Ensure strict per-group ordering while enabling parallel group processing

### Default Group Behavior

Messages without the `x-pgmq-group` header are treated as belonging to a single default group:

```sql
-- These messages will be processed in FIFO order relative to each other
SELECT pgmq.send('my_queue', '{"message": "first"}');
SELECT pgmq.send('my_queue', '{"message": "second"}');
```

## API Reference

### Reading Functions

#### `pgmq.read_grouped_rr(queue_name, vt, qty)`

Read messages while respecting FIFO ordering within groups.

**Parameters:**
- `queue_name` (text): Name of the queue
- `vt` (integer): Visibility timeout in seconds
- `qty` (integer): Maximum number of messages to read

#### `pgmq.read_grouped_rr_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms)`

Same as `read_grouped_rr()` but with polling support for real-time processing.

#### `pgmq.read_grouped(queue_name, vt, qty)`

Read messages with AWS SQS FIFO-style batch retrieval behavior. Unlike `read_grouped_rr()` which interleaves fairly across groups, this function attempts to return as many messages as possible from the same message group to maximize throughput for related messages.

**Behavior:**
- Prioritizes filling the batch from the earliest message group first
- Returns multiple messages from the same group when available
- Only moves to other groups if the batch cannot be filled from the first group
- Maintains strict FIFO ordering within each group

#### `pgmq.read_grouped_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms)`

Same as `read_grouped()` but with polling support for real-time processing.

#### `pgmq.read_grouped_head(queue_name, vt, qty)`

Read exactly one message per FIFO group — the head (oldest, lowest `msg_id`) message in each group — across up to `qty` groups in a single operation. Only groups with a visible, unlocked head message are included.

**Parameters:**

- `queue_name` (text): Name of the queue
- `vt` (integer): Visibility timeout in seconds applied to each returned message
- `qty` (integer): Maximum number of groups (and therefore messages) to return

**Behavior:**

- Determines the absolute head `msg_id` per FIFO group, regardless of current visibility
- Skips groups whose head message is not visible (being processed by another worker)
- Returns at most one message per group, ordered by `msg_id`
- Uses `FOR UPDATE SKIP LOCKED` for safe concurrent access

**Use when:**

- Horizontal scaling: multiple processes each call `read_grouped_head()` and handle distinct groups
- Process groups in parallel

**Example:**

```sql
-- Claim the head message from up to 10 groups, locking them for 30 seconds
SELECT * FROM pgmq.read_grouped_head('my_queue', 30, 10);
```

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
SELECT pgmq.send('user_events', '{"action": "login"}', '{"x-pgmq-group": "user_123"}');
SELECT pgmq.send('user_events', '{"action": "purchase"}', '{"x-pgmq-group": "user_123"}');

-- User 2 messages (can be processed in parallel)
SELECT pgmq.send('user_events', '{"action": "login"}', '{"x-pgmq-group": "user_456"}');
```

### 2. Order Processing

Maintain order integrity for financial transactions:

```sql
-- Order lifecycle events
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "create"}', '{"x-pgmq-group": "ord_1"}');
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "payment"}', '{"x-pgmq-group": "ord_1"}');
SELECT pgmq.send('orders', '{"order_id": "ord_1", "action": "fulfill"}', '{"x-pgmq-group": "ord_1"}');
```

### 3. Document Processing

Process document versions in sequence:

```sql
-- Document updates
SELECT pgmq.send('docs', '{"doc_id": "doc_1", "version": 1}', '{"x-pgmq-group": "doc_1"}');
SELECT pgmq.send('docs', '{"doc_id": "doc_1", "version": 2}', '{"x-pgmq-group": "doc_1"}');
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
-- Next read_grouped() call will return the same message for retry
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
2. **Gradual adoption**: Start using `pgmq.read_grouped_rr()` or `pgmq.read_grouped()` for new consumers
3. **Mixed usage**: Some consumers can use FIFO, others regular reads
4. **Performance**: Add FIFO indexes when ready to optimize

## FIFO Reading Strategies

PGMQ provides two different FIFO reading strategies to suit different use cases:

### Fair Distribution (`pgmq.read_grouped_rr()`)

Interleaves messages across FIFO groups in layers:

```sql
-- With groups A (5 messages), B (3 messages), C (2 messages)
SELECT * FROM pgmq.read_grouped_rr('queue', 30, 10);
-- Returns (layered interleaving): A1, B1, C1, A2, B2, C2, A3, B3, A4, ...
```

**Best for:**

- Ensuring fair processing across all groups
- Preventing starvation of groups with fewer messages
- Load balancing across different workflows

### Throughput Optimization (`pgmq.read_grouped()`)

Attempts to fill the batch from the earliest group first:

```sql
-- With groups A (5 messages), B (3 messages), C (2 messages)
SELECT * FROM pgmq.read_grouped('queue', 30, 10);
-- Returns: 10 messages (5 from A + 3 from B + 2 from C)

SELECT * FROM pgmq.read_grouped('queue', 30, 3);
-- Returns: 3 messages (all from group A)
```

**Best for:**
- Maximizing throughput for related messages
- Processing workflows where batching related messages is beneficial
- Mimicking AWS SQS FIFO behavior exactly

### One-per-Group (`pgmq.read_grouped_head()`)

Returns exactly one message per FIFO group, up to N groups:

```sql
-- With groups A (5 messages), B (3 messages), C (2 messages)
SELECT * FROM pgmq.read_grouped_head('queue', 30, 10);
-- Returns: 3 messages — 1 from A, 1 from B, 1 from C

SELECT * FROM pgmq.read_grouped_head('queue', 30, 2);
-- Returns: 2 messages — 1 from A, 1 from B (oldest two groups)
```

**Best for:**

- Horizontal scaling scenarios where groups map to independent units of work (e.g. ordering key)
- Parallel per-group processing: spawn one worker per active group
- Strict per-group ordering with no intra-group batching

### Choosing the Right Strategy

| Scenario | Recommended Function | Reason |
|----------|---------------------|---------|
| Multi-tenant processing | `read_grouped_rr()` | Ensures fair resource allocation |
| Order processing pipeline | `read_grouped()` | Related orders processed together |
| User activity streams | `read_grouped_rr()` | Prevents one active user from blocking others |
| Document workflows | `read_grouped()` | Process all versions of a document together |
| Financial transactions | `read_grouped()` | Batch related transactions for efficiency |
| Parallel per-group workers    | `read_grouped_head()` | One outstanding message per group at a time           |
| Horizontally scaled consumers | `read_grouped_head()` | Safely dispatches distinct groups to separate workers |

## Comparison with AWS SQS FIFO

| Feature | PGMQ FIFO (RR) | PGMQ read_grouped | AWS SQS FIFO |
|---------|-----------------|----------------|--------------|
| Group-based ordering | ✅ | ✅ | ✅ |
| Parallel group processing | ✅ | ✅ | ✅ |
| Batch retrieval strategy | Fair (layered interleaving) | Throughput-optimized | Throughput-optimized |
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
