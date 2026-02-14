# Topic-Based Routing

PGMQ supports topic-based message routing with wildcard patterns, similar to AMQP topic exchanges. This feature allows
you to route messages to multiple queues based on routing keys and pattern matching.

## Overview

Topic routing in PGMQ works by binding patterns to queues. When a message is sent with a routing key, it is delivered to
all queues whose patterns match that key. This enables flexible publish-subscribe patterns and content-based routing.

### Key Features

- **Wildcard patterns**: Use `*` (one segment) and `#` (zero or more segments) for flexible matching
- **Multiple queue delivery**: A single message can be routed to multiple queues
- **Pattern-based subscriptions**: Queues subscribe to message types via patterns
- **Dry-run testing**: Test routing without sending messages using `test_routing()`
- **Precompiled regex**: Patterns are compiled to regex at bind time for fast matching
- **Automatic cleanup**: Bindings are removed when queues are dropped (CASCADE)

## How It Works

### Routing Keys and Patterns

Routing keys and patterns use dot-separated segments:

```sql
-- Routing keys (used when sending messages)
'logs.api.error'
'orders.created'
'user.signup.completed'

-- Patterns (used when binding to queues)
'logs.*'           -- Matches exactly one segment after 'logs.'
'logs.#'           -- Matches zero or more segments after 'logs.'
'*.error'          -- Matches any single segment before '.error'
'#.error'          -- Matches zero or more segments before '.error'
'logs.*.error'     -- Matches 'logs.{any}.error'
```

### Wildcards

| Wildcard | Meaning               | Example Pattern | Matches                        | Does Not Match           |
|----------|-----------------------|-----------------|--------------------------------|--------------------------|
| `*`      | Exactly one segment   | `logs.*`        | `logs.error`, `logs.info`      | `logs`, `logs.api.error` |
| `#`      | Zero or more segments | `logs.#`        | `logs.error`, `logs.api.error` | `logs` (no dot after)    |

### Binding Patterns to Queues

```sql
-- Create queues
SELECT pgmq.create('all_logs');
SELECT pgmq.create('error_logs');
SELECT pgmq.create('api_errors');

-- Bind patterns
SELECT pgmq.bind_topic('logs.#', 'all_logs'); -- All logs
SELECT pgmq.bind_topic('logs.*.error', 'error_logs'); -- Error logs from any service
SELECT pgmq.bind_topic('logs.api.error', 'api_errors'); -- Only API errors
```

### Sending Messages with Routing Keys

```sql
-- This message routes to: all_logs, error_logs, api_errors (3 queues)
SELECT pgmq.send_topic('logs.api.error', '{"message": "API failed"}');

-- This message routes to: all_logs, error_logs (2 queues)
SELECT pgmq.send_topic('logs.db.error', '{"message": "DB connection failed"}');

-- This message routes to: all_logs only (1 queue)
SELECT pgmq.send_topic('logs.api.info', '{"message": "Request received"}');
```

## API Reference

### Binding Functions

#### `pgmq.bind_topic(pattern, queue_name)`

Bind a pattern to a queue. Messages matching the pattern will be routed to this queue.

**Parameters:**

- `pattern` (text): The wildcard pattern to match routing keys
- `queue_name` (text): Name of the queue to receive matching messages

**Returns:** void

**Example:**

```sql
SELECT pgmq.bind_topic('orders.#', 'order_events');
SELECT pgmq.bind_topic('orders.*.failed', 'failed_orders');
```

**Notes:**

- Patterns are validated before binding
- Binding the same pattern to the same queue is idempotent (no error, no duplicate)
- One pattern can be bound to multiple queues
- One queue can have multiple pattern bindings

#### `pgmq.unbind_topic(pattern, queue_name)`

Remove a pattern binding from a queue.

**Parameters:**

- `pattern` (text): The pattern to unbind
- `queue_name` (text): Name of the queue

**Returns:** boolean - `true` if a binding was removed, `false` if no binding existed

**Example:**

```sql
SELECT pgmq.unbind_topic('orders.#', 'order_events');
```

### Sending Functions

#### `pgmq.send_topic(routing_key, msg, headers, delay)`

Send a message to all queues whose patterns match the routing key.

**Parameters:**

- `routing_key` (text): The routing key for pattern matching
- `msg` (jsonb): The message payload
- `headers` (jsonb): Optional message headers
- `delay` (integer): Delay in seconds before message becomes visible

**Returns:** integer - Number of queues the message was sent to

**Example:**

```sql
-- Full signature
SELECT pgmq.send_topic('orders.created', '{"order_id": 123}', '{"priority": "high"}', 0);

-- Simplified versions
SELECT pgmq.send_topic('orders.created', '{"order_id": 123}');
SELECT pgmq.send_topic('orders.created', '{"order_id": 123}', 5); -- 5 second delay
```

**Notes:**

- If no patterns match the routing key, the message is not sent (returns 0)
- If multiple patterns match, the message is sent to all matching queues
- All sends succeed or all fail (transactional)

#### `pgmq.send_batch_topic(routing_key, msgs, headers, delay)`

Send multiple messages to all queues whose patterns match the routing key.

**Parameters:**

- `routing_key` (text): The routing key for pattern matching
- `msgs` (jsonb[]): Array of message payloads to send
- `headers` (jsonb[]): Optional array of headers for each message
- `delay` (integer): Delay in seconds before messages become visible
- `delay` (timestamp with time zone): Timestamp when messages become visible

**Returns:** Table with columns:
- `queue_name` (text): Name of the queue that received messages
- `msg_id` (bigint): ID of each message that was sent

**Example:**

```sql
-- Send 3 messages to all matching queues
SELECT * FROM pgmq.send_batch_topic(
    'orders.created',
    ARRAY[
        '{"order_id": 1, "amount": 100}',
        '{"order_id": 2, "amount": 200}',
        '{"order_id": 3, "amount": 300}'
    ]::jsonb[]
);

-- Returns (if 2 queues match the pattern):
--  queue_name       | msg_id
-- ------------------+--------
--  order_processor  | 1
--  order_processor  | 2
--  order_processor  | 3
--  order_analytics  | 1
--  order_analytics  | 2
--  order_analytics  | 3

-- With headers
SELECT * FROM pgmq.send_batch_topic(
    'notifications.email',
    ARRAY['{"to": "user1@example.com"}', '{"to": "user2@example.com"}']::jsonb[],
    ARRAY['{"priority": "high"}', '{"priority": "normal"}']::jsonb[]
);

-- Simplified versions
SELECT * FROM pgmq.send_batch_topic('logs.info', ARRAY['{"msg": "test"}']::jsonb[]);
SELECT * FROM pgmq.send_batch_topic('alerts.critical', ARRAY['{"alert": "down"}']::jsonb[], 60); -- 60 second delay

-- With timestamp delay (visible in 1 hour)
SELECT * FROM pgmq.send_batch_topic(
    'scheduled.tasks',
    ARRAY['{"task": "backup"}']::jsonb[],
    CURRENT_TIMESTAMP + INTERVAL '1 hour'
);
```

**Notes:**

- Each message in the batch is sent to all matching queues
- If the routing key matches 2 queues and you send 3 messages, you get 6 total messages (3 per queue)
- Headers array length must exactly match messages array length when provided (not NULL). Empty headers arrays will fail validation if msgs is not empty. To send without headers, omit the parameter or pass NULL
- If no patterns match the routing key, returns an empty result set
- All sends succeed or all fail (transactional)

### Testing Functions

#### `pgmq.test_routing(routing_key)`

Test which queues would receive a message with the given routing key, without actually sending a message.

**Parameters:**

- `routing_key` (text): The routing key to test

**Returns:** Table with columns:

- `pattern` (text): The matching pattern
- `queue_name` (text): The queue that would receive the message
- `compiled_regex` (text): The internal regex used for matching

**Example:**

```sql
SELECT *
FROM pgmq.test_routing('logs.api.error');
-- Returns:
--   pattern      | queue_name  | compiled_regex
--   -------------+-------------+-------------------
--   logs.#       | all_logs    | ^logs\..*$
--   logs.*.error | error_logs  | ^logs\.[^.]+\.error$
```

### Validation Functions

#### `pgmq.validate_routing_key(routing_key)`

Validate that a routing key is well-formed.

**Valid routing keys:**

- Alphanumeric characters, dots, hyphens, and underscores
- Cannot start or end with a dot
- Cannot contain consecutive dots
- Cannot contain wildcards (`*` or `#`)
- Maximum 255 characters

**Example:**

```sql
SELECT pgmq.validate_routing_key('logs.api.error'); -- Returns true
SELECT pgmq.validate_routing_key('logs..error'); -- Raises exception
```

#### `pgmq.validate_topic_pattern(pattern)`

Validate that a topic pattern is well-formed.

**Valid patterns:**

- Same rules as routing keys, plus `*` and `#` wildcards
- Cannot have consecutive wildcards (`**`, `##`, `*#`, `#*`)

**Example:**

```sql
SELECT pgmq.validate_topic_pattern('logs.*.error'); -- Returns true
SELECT pgmq.validate_topic_pattern('logs.**'); -- Raises exception
```

## Usage Patterns

### 1. Log Aggregation

Route logs to different queues based on severity and service:

```sql
-- Create queues
SELECT pgmq.create('logs_all');
SELECT pgmq.create('logs_errors');
SELECT pgmq.create('logs_critical');
SELECT pgmq.create('api_logs');

-- Bind patterns
SELECT pgmq.bind_topic('#', 'logs_all'); -- All logs
SELECT pgmq.bind_topic('*.error', 'logs_errors'); -- All errors
SELECT pgmq.bind_topic('*.critical', 'logs_critical'); -- All critical
SELECT pgmq.bind_topic('api.#', 'api_logs');
-- API service logs

-- Send logs
SELECT pgmq.send_topic('api.error', '{"msg": "API error"}');
-- Routes to: logs_all, logs_errors, api_logs

SELECT pgmq.send_topic('db.critical', '{"msg": "DB down"}');
-- Routes to: logs_all, logs_critical
```

### 2. Event Broadcasting (Fanout)

Send events to all interested consumers:

```sql
-- Create consumer queues
SELECT pgmq.create('notifications');
SELECT pgmq.create('analytics');
SELECT pgmq.create('audit_log');

-- All queues subscribe to everything
SELECT pgmq.bind_topic('#', 'notifications');
SELECT pgmq.bind_topic('#', 'analytics');
SELECT pgmq.bind_topic('#', 'audit_log');

-- Any event goes to all queues
SELECT pgmq.send_topic('user.signup', '{"user_id": 123}');
-- Routes to all 3 queues
```

### 3. Direct Routing (Exact Match)

Route specific events to specific queues:

```sql
-- Create queues
SELECT pgmq.create('user_events');
SELECT pgmq.create('order_events');

-- Bind exact patterns (no wildcards)
SELECT pgmq.bind_topic('user.created', 'user_events');
SELECT pgmq.bind_topic('user.updated', 'user_events');
SELECT pgmq.bind_topic('user.deleted', 'user_events');
SELECT pgmq.bind_topic('order.placed', 'order_events');
SELECT pgmq.bind_topic('order.shipped', 'order_events');

-- Messages route to exactly one queue
SELECT pgmq.send_topic('user.created', '{"user_id": 1}');
-- Routes to: user_events only
```

### 4. Geographic Routing

Route messages based on region:

```sql
-- Create regional queues
SELECT pgmq.create('events_us');
SELECT pgmq.create('events_eu');
SELECT pgmq.create('events_global');

-- Bind regional patterns
SELECT pgmq.bind_topic('us.#', 'events_us');
SELECT pgmq.bind_topic('eu.#', 'events_eu');
SELECT pgmq.bind_topic('#', 'events_global');

-- Route by region
SELECT pgmq.send_topic('us.orders.created', '{"order_id": 100}');
-- Routes to: events_us, events_global

SELECT pgmq.send_topic('eu.users.signup', '{"user_id": 50}');
-- Routes to: events_eu, events_global
```

### 5. Multi-Tenant Processing

Route messages to tenant-specific and shared queues:

```sql
-- Create queues
SELECT pgmq.create('tenant_acme');
SELECT pgmq.create('tenant_globex');
SELECT pgmq.create('all_tenants');

-- Bind patterns
SELECT pgmq.bind_topic('acme.#', 'tenant_acme');
SELECT pgmq.bind_topic('globex.#', 'tenant_globex');
SELECT pgmq.bind_topic('#', 'all_tenants');

-- Route by tenant
SELECT pgmq.send_topic('acme.orders.new', '{"order": "data"}');
-- Routes to: tenant_acme, all_tenants
```

### 6. Batch Message Publishing

Send multiple messages at once using `send_batch_topic`:

```sql
-- Create notification queues
SELECT pgmq.create('email_queue');
SELECT pgmq.create('sms_queue');
SELECT pgmq.create('all_notifications');

-- Bind patterns
SELECT pgmq.bind_topic('notify.email.#', 'email_queue');
SELECT pgmq.bind_topic('notify.sms.#', 'sms_queue');
SELECT pgmq.bind_topic('notify.#', 'all_notifications');

-- Send batch of email notifications
SELECT * FROM pgmq.send_batch_topic(
    'notify.email.welcome',
    ARRAY[
        '{"to": "user1@example.com", "name": "Alice"}',
        '{"to": "user2@example.com", "name": "Bob"}',
        '{"to": "user3@example.com", "name": "Carol"}'
    ]::jsonb[]
);
-- Routes 3 messages each to: email_queue and all_notifications (6 total messages)

-- Send batch with headers for priority handling
SELECT * FROM pgmq.send_batch_topic(
    'notify.sms.alert',
    ARRAY['{"phone": "+1234567890", "msg": "Alert!"}']::jsonb[],
    ARRAY['{"priority": "urgent"}']::jsonb[]
);
-- Routes to: sms_queue and all_notifications with priority header

-- Send batch with delay (e.g., scheduled notifications)
SELECT * FROM pgmq.send_batch_topic(
    'notify.email.reminder',
    ARRAY[
        '{"to": "user@example.com", "subject": "Reminder"}',
        '{"to": "admin@example.com", "subject": "Daily report"}'
    ]::jsonb[],
    3600  -- Delay 1 hour
);
-- Messages become visible in 1 hour
```

## Performance Considerations

### Pattern Compilation

Patterns are compiled to regular expressions when bound, not at send time:

```sql
-- Pattern 'logs.*.error' is stored with precompiled regex: ^logs\.[^.]+\.error$
SELECT pattern, compiled_regex
FROM pgmq.topic_bindings;
```

This means:

- Binding is slightly slower (regex compilation)
- Sending is fast (uses precompiled regex)
- Many bindings = more patterns to check per send

### Indexing

The `topic_bindings` table has a covering index for efficient pattern scanning:

```sql
-- Index includes queue_name for index-only scans
CREATE INDEX idx_topic_bindings_covering
    ON pgmq.topic_bindings (pattern) INCLUDE (queue_name);
```

## Examples

See [examples/topics.sql](../examples/topics.sql) for comprehensive usage examples including:

- Wildcard differences (`*` vs `#`)
- Topic-based routing
- Fanout pattern (broadcast)
- Direct routing (exact match)
- Dry-run testing
