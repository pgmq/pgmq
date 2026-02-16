# Functions

## Sending Messages

### send

Send a single message to a queue with optional headers and delay.

**Signatures:**

```text
pgmq.send(queue_name text, msg jsonb)
pgmq.send(queue_name text, msg jsonb, headers jsonb)
pgmq.send(queue_name text, msg jsonb, delay integer)
pgmq.send(queue_name text, msg jsonb, delay timestamp with time zone)
pgmq.send(queue_name text, msg jsonb, headers jsonb, delay integer)
pgmq.send(queue_name text, msg jsonb, headers jsonb, delay timestamp with time zone)

RETURNS SETOF bigint
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg   | jsonb        | The message to send to the queue      |
| headers   | jsonb        | Optional message headers/metadata      |
| delay   | integer        | Time in seconds before the message becomes visible      |
| delay   | timestamp with time zone        | Timestamp when the message becomes visible      |

**Returns:** The ID of the message that was added to the queue.

Examples:

```sql
-- Send a message
select * from pgmq.send('my_queue', '{"hello": "world"}');
 send
------
    1

-- Send a message with headers
select * from pgmq.send('my_queue', '{"hello": "world"}', '{"trace_id": "abc123"}');
 send
------
    2

-- Message with a delay of 5 seconds
select * from pgmq.send('my_queue', '{"hello": "world"}', 5);
 send
------
    3

-- Message readable from tomorrow
select * from pgmq.send('my_queue', '{"hello": "world"}', CURRENT_TIMESTAMP + INTERVAL '1 day');
 send
------
    4

-- Message with headers and delay
select * from pgmq.send('my_queue', '{"hello": "world"}', '{"priority": "high"}', 10);
 send
------
    5
```

---

### send_batch

Send 1 or more messages to a queue with optional headers and delay.

**Signatures:**

```text
pgmq.send_batch(queue_name text, msgs jsonb[])
pgmq.send_batch(queue_name text, msgs jsonb[], headers jsonb[])
pgmq.send_batch(queue_name text, msgs jsonb[], delay integer)
pgmq.send_batch(queue_name text, msgs jsonb[], delay timestamp with time zone)
pgmq.send_batch(queue_name text, msgs jsonb[], headers jsonb[], delay integer)
pgmq.send_batch(queue_name text, msgs jsonb[], headers jsonb[], delay timestamp with time zone)

RETURNS SETOF bigint
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msgs   | jsonb[]       | Array of messages to send to the queue      |
| headers   | jsonb[]       | Array of headers for each message (must match msgs length, or can be omitted)      |
| delay   | integer        | Time in seconds before the messages become visible      |
| delay   | timestamp with time zone        | Timestamp when the messages become visible      |

**Returns:** The IDs of the messages that were added to the queue.

**Validation:** When `headers` is provided (not NULL), its array length must exactly match the length of `msgs`. This includes empty arrays - an empty headers array (e.g., `ARRAY[]::jsonb[]`) will fail validation if `msgs` is not empty. To send messages without headers, either omit the `headers` parameter or pass NULL.

Examples:

```sql
-- Send multiple messages
select * from pgmq.send_batch('my_queue',
    ARRAY[
        '{"hello": "world_0"}',
        '{"hello": "world_1"}'
    ]::jsonb[]
);
 send_batch
------------
          1
          2

-- Send with headers for each message
select * from pgmq.send_batch('my_queue',
    ARRAY['{"hello": "world_0"}', '{"hello": "world_1"}']::jsonb[],
    ARRAY['{"trace_id": "abc"}', '{"trace_id": "def"}']::jsonb[]
);
 send_batch
------------
          3
          4

-- Messages with a delay of 5 seconds
select * from pgmq.send_batch('my_queue',
    ARRAY[
        '{"hello": "world_0"}',
        '{"hello": "world_1"}'
    ]::jsonb[],
    5
);
 send_batch
------------
          5
          6

-- Messages readable from tomorrow
select * from pgmq.send_batch('my_queue',
    ARRAY[
        '{"hello": "world_0"}',
        '{"hello": "world_1"}'
    ]::jsonb[],
    CURRENT_TIMESTAMP + INTERVAL '1 day'
);
 send_batch
------------
          7
          8
```

---

## Topic-Based Routing

PGMQ supports topic-based message routing with wildcard patterns. Messages can be routed to multiple queues based on routing keys and pattern matching. See the [Topics guide](../../topics.md) for detailed information.

### send_topic

Send a message to all queues whose patterns match the routing key.

**Signatures:**

```text
pgmq.send_topic(routing_key text, msg jsonb)
pgmq.send_topic(routing_key text, msg jsonb, delay integer)
pgmq.send_topic(routing_key text, msg jsonb, headers jsonb, delay integer)

RETURNS integer
```

**Parameters:**

| Parameter   | Type    | Description                                             |
| :---------- | :------ | :------------------------------------------------------ |
| routing_key | text    | The routing key for pattern matching                    |
| msg         | jsonb   | The message payload                                     |
| headers     | jsonb   | Optional message headers/metadata                       |
| delay       | integer | Time in seconds before the message becomes visible      |

**Returns:** The number of queues the message was sent to.

Examples:

```sql
-- Create queues and bind patterns
select pgmq.create('logs_all');
select pgmq.create('logs_errors');
select pgmq.bind_topic('logs.#', 'logs_all');
select pgmq.bind_topic('logs.*.error', 'logs_errors');

-- Send to matching queues
select pgmq.send_topic('logs.api.error', '{"message": "API failed"}');
 send_topic
------------
          2
-- Message sent to 2 queues: logs_all and logs_errors

-- With headers
select pgmq.send_topic('logs.db.error', '{"message": "DB error"}', '{"severity": "high"}', 0);

-- With delay
select pgmq.send_topic('logs.api.info', '{"message": "Request received"}', 5);
```

---

### send_batch_topic

Send multiple messages to all queues whose patterns match the routing key.

**Signatures:**

```text
pgmq.send_batch_topic(routing_key text, msgs jsonb[])
pgmq.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[])
pgmq.send_batch_topic(routing_key text, msgs jsonb[], delay integer)
pgmq.send_batch_topic(routing_key text, msgs jsonb[], delay timestamp with time zone)
pgmq.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[], delay integer)
pgmq.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[], delay timestamp with time zone)

RETURNS TABLE(queue_name text, msg_id bigint)
```

**Parameters:**

| Parameter   | Type     | Description                                                       |
| :---------- | :------- | :---------------------------------------------------------------- |
| routing_key | text     | The routing key for pattern matching                              |
| msgs        | jsonb[]  | Array of message payloads to send                                 |
| headers     | jsonb[]  | Optional array of headers for each message                        |
| delay       | integer  | Time in seconds before the messages become visible                |
| delay       | timestamp with time zone | Timestamp when the messages become visible           |

**Returns:** A table with the queue name and message ID for each message sent.

**Validation:** When `headers` is provided (not NULL), its array length must exactly match the length of `msgs`. Empty headers arrays will fail validation if `msgs` is not empty. To send messages without headers, either omit the `headers` parameter or pass NULL.

Examples:

```sql
-- Send batch of messages to matching queues
select * from pgmq.send_batch_topic(
    'orders.created',
    array[
        '{"order_id": 1, "amount": 100}',
        '{"order_id": 2, "amount": 200}'
    ]::jsonb[]
);
      queue_name       | msg_id
-----------------------+--------
 order_processor       |      1
 order_processor       |      2
 order_analytics       |      1
 order_analytics       |      2
-- Each message sent to all matching queues

-- With headers
select * from pgmq.send_batch_topic(
    'notifications.email',
    array['{"to": "user1@example.com"}', '{"to": "user2@example.com"}']::jsonb[],
    array['{"priority": "high"}', '{"priority": "normal"}']::jsonb[]
);

-- With delay (messages visible in 60 seconds)
select * from pgmq.send_batch_topic(
    'alerts.critical',
    array['{"alert": "system down"}']::jsonb[],
    60
);

-- With timestamp delay (visible in 1 hour)
select * from pgmq.send_batch_topic(
    'scheduled.tasks',
    array['{"task": "backup"}']::jsonb[],
    CURRENT_TIMESTAMP + INTERVAL '1 hour'
);
```

---

### bind_topic

Bind a pattern to a queue. Messages with routing keys matching the pattern will be routed to this queue.

**Signature:**

```text
pgmq.bind_topic(pattern text, queue_name text)

RETURNS void
```

**Parameters:**

| Parameter  | Type | Description                                |
| :--------- | :--- | :----------------------------------------- |
| pattern    | text | The wildcard pattern to match routing keys |
| queue_name | text | Name of the queue to receive matching messages |

**Wildcard patterns:**
- `*` matches exactly one segment (e.g., `logs.*` matches `logs.error` but not `logs.api.error`)
- `#` matches zero or more segments (e.g., `logs.#` matches `logs.error` and `logs.api.error`)

Examples:

```sql
-- Create queue
select pgmq.create('error_logs');

-- Bind patterns
select pgmq.bind_topic('logs.*.error', 'error_logs');  -- Errors from any service
select pgmq.bind_topic('alerts.#', 'error_logs');      -- All alerts

-- Binding is idempotent
select pgmq.bind_topic('logs.*.error', 'error_logs');  -- No error, no duplicate
```

---

### unbind_topic

Remove a pattern binding from a queue.

**Signature:**

```text
pgmq.unbind_topic(pattern text, queue_name text)

RETURNS boolean
```

**Parameters:**

| Parameter  | Type | Description                   |
| :--------- | :--- | :---------------------------- |
| pattern    | text | The pattern to unbind         |
| queue_name | text | Name of the queue             |

**Returns:** `true` if a binding was removed, `false` if no binding existed.

Examples:

```sql
-- Remove binding
select pgmq.unbind_topic('logs.*.error', 'error_logs');
 unbind_topic
--------------
 t

-- No binding to remove
select pgmq.unbind_topic('nonexistent.pattern', 'error_logs');
 unbind_topic
--------------
 f
```

---

### test_routing

Test which queues would receive a message with the given routing key, without actually sending a message.

**Signature:**

```text
pgmq.test_routing(routing_key text)

RETURNS TABLE(pattern text, queue_name text, compiled_regex text)
```

**Parameters:**

| Parameter   | Type | Description                  |
| :---------- | :--- | :--------------------------- |
| routing_key | text | The routing key to test      |

**Returns:** Table showing all patterns that match the routing key and which queues they route to.

Examples:

```sql
-- Test routing
select * from pgmq.test_routing('logs.api.error');
    pattern     |  queue_name  | compiled_regex
----------------+--------------+---------------------------
 logs.#         | logs_all     | ^logs\..*$
 logs.*.error   | error_logs   | ^logs\.[^.]+\.error$
```

---

## Reading Messages

### read

Read 1 or more messages from a queue. The VT specifies the amount of time in seconds that the message will be invisible to other consumers after reading.

<pre>
 <code>
pgmq.read(
    queue_name text,
    vt integer,
    qty integer,
    conditional jsonb DEFAULT '{}')

RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter   | Type     | Description |
| :---        |  :----   |  :--- |
| queue_name  | text     | The name of the queue   |
| vt          | integer  | Time in seconds that the message become invisible after reading |
| qty         | integer  | The number of messages to read from the queue |
| conditional | jsonb    | Filters the messages by their json content. Defaults to '{}' - no filtering. **This feature is experimental, and the API is subject to change in future releases**  |

Examples:

Read messages from a queue

```sql
select * from pgmq.read('my_queue', 10, 2);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |       message        | headers 
--------+---------+-------------------------------+-------------------------------+-------------------------------+----------------------+---------
      1 |       1 | 2026-01-23 20:07:48.083773-06 | 2026-01-23 20:08:04.085665-06 | 2026-01-23 20:08:14.085665-06 | {"hello": "world_0"} | 
      2 |       1 | 2026-01-23 20:07:48.083773-06 | 2026-01-23 20:08:04.08568-06  | 2026-01-23 20:08:14.08568-06  | {"hello": "world_1"} | 
(2 rows)
```

Read a message from a queue with message filtering

```sql
select * from pgmq.read('my_queue', 10, 2, '{"hello": "world_1"}');
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |       message        |       headers       
--------+---------+-------------------------------+-------------------------------+-------------------------------+----------------------+---------------------
      2 |       2 | 2026-01-23 20:07:48.083773-06 | 2026-01-23 20:08:30.829749-06 | 2026-01-23 20:08:40.829749-06 | {"hello": "world_1"} | 
      4 |       1 | 2026-01-23 20:07:50.824379-06 | 2026-01-23 20:08:30.829765-06 | 2026-01-23 20:08:40.829765-06 | {"hello": "world_1"} | {"trace_id": "def"}
(2 rows)
```

---

### read_with_poll

Same as read(). Also provides convenient long-poll functionality.
 When there are no messages in the queue, the function call will wait for `max_poll_seconds` in duration before returning.
 If messages reach the queue during that duration, they will be read and returned immediately.

<pre>
 <code>
 pgmq.read_with_poll(
    queue_name text,
    vt integer,
    qty integer,
    max_poll_seconds integer DEFAULT 5,
    poll_interval_ms integer DEFAULT 100,
    conditional jsonb DEFAULT '{}'
)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| vt   | integer       | Time in seconds that the message become invisible after reading.      |
| qty   | integer        | The number of messages to read from the queue      |
| max_poll_seconds   | integer        | Time in seconds to wait for new messages to reach the queue. Defaults to 5.      |
| poll_interval_ms   | integer        | Milliseconds between the internal poll operations. Defaults to 100.      |
| conditional | jsonb    | Filters the messages by their json content. Defaults to '{}' - no filtering. **This feature is experimental, and the API is subject to change in future releases** |

Example:

```sql
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |       message        | headers 
--------+---------+-------------------------------+-------------------------------+-------------------------------+----------------------+---------
      1 |       2 | 2026-01-23 20:07:48.083773-06 | 2026-01-23 20:09:09.588292-06 | 2026-01-23 20:09:10.588292-06 | {"hello": "world_0"} | 
(1 row)
```

---

### read_grouped

Read messages from a queue with AWS SQS FIFO-style batch retrieval behavior. This function attempts to return as many messages as possible from the same message group, filling the batch from the earliest available group first. Messages with the same FIFO group ID (specified in the `x-pgmq-group` header) will be processed in order. Messages without FIFO headers are treated as belonging to a default group.

<pre>
 <code>
pgmq.read_grouped(
    queue_name text,
    vt integer,
    qty integer
)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter   | Type     | Description |
| :---        |  :----   |  :--- |
| queue_name  | text     | The name of the queue   |
| vt          | integer  | Time in seconds that the message become invisible after reading |
| qty         | integer  | The number of messages to read from the queue |

**FIFO Behavior (Fill from same group):**
- Messages with the same `x-pgmq-group` header value are processed in strict order
- Only the oldest unprocessed message from each FIFO group can be read
- The batch is filled preferentially from the earliest group (by oldest msg_id)
- Messages from different FIFO groups can be processed in parallel
- Messages without FIFO headers are treated as a single default group

Examples:

Send messages with FIFO grouping:

```sql
-- Send messages to the same FIFO group
select pgmq.send('my_queue', msg=>'{"order": 1}', headers=>'{"x-pgmq-group": "user123"}');
select pgmq.send('my_queue', msg=>'{"order": 2}', headers=>'{"x-pgmq-group": "user123"}');
select pgmq.send('my_queue', msg=>'{"order": 3}', headers=>'{"x-pgmq-group": "user123"}');
select pgmq.send('my_queue', msg=>'{"order": 1}', headers=>'{"x-pgmq-group": "user456"}');

-- Read with FIFO grouped ordering - tries to fill batch from earliest group
select * from pgmq.read_grouped('my_queue', 10, 5);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       2 | 2026-01-23 20:10:17.015888-06 | 2026-01-23 20:13:22.965962-06 | 2026-01-23 20:13:32.965962-06 | {"order": 1} | {"x-pgmq-group": "user123"}
      2 |       2 | 2026-01-23 20:11:15.841062-06 | 2026-01-23 20:13:22.965977-06 | 2026-01-23 20:13:32.965977-06 | {"order": 2} | {"x-pgmq-group": "user123"}
      3 |       2 | 2026-01-23 20:11:18.718345-06 | 2026-01-23 20:13:22.96598-06  | 2026-01-23 20:13:32.96598-06  | {"order": 3} | {"x-pgmq-group": "user123"}
      4 |       2 | 2026-01-23 20:11:21.338077-06 | 2026-01-23 20:13:22.965982-06 | 2026-01-23 20:13:32.965982-06 | {"order": 1} | {"x-pgmq-group": "user456"}
```

---

### read_grouped_with_poll

Same as read_grouped(). Also provides convenient long-poll functionality for FIFO queues.
 When there are no messages available that respect FIFO ordering, the function call will wait for `max_poll_seconds` in duration before returning.

<pre>
 <code>
 pgmq.read_grouped_with_poll(
    queue_name text,
    vt integer,
    qty integer,
    max_poll_seconds integer DEFAULT 5,
    poll_interval_ms integer DEFAULT 100
)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| vt   | integer       | Time in seconds that the message become invisible after reading.      |
| qty   | integer        | The number of messages to read from the queue.      |
| max_poll_seconds   | integer        | Time in seconds to wait for new messages to reach the queue. Defaults to 5.      |
| poll_interval_ms   | integer        | Milliseconds between the internal poll operations. Defaults to 100.      |

Example:

```sql
 select * from pgmq.read_grouped_with_poll('my_queue', 10, 1, 5, 100);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       5 | 2026-01-23 20:10:17.015888-06 | 2026-01-23 20:15:43.446826-06 | 2026-01-23 20:15:53.446826-06 | {"order": 1} | {"x-pgmq-group": "user123"}
(1 row)
```

---

### read_grouped_rr

Read messages from a queue while respecting FIFO (First-In-First-Out) ordering within message groups and using round-robin interleaving across groups. Messages with the same FIFO group ID (specified in the `x-pgmq-group` header) will be processed in strict order. Messages without FIFO headers are treated as belonging to a default group.

<pre>
 <code>
pgmq.read_grouped_rr(
    queue_name text,
    vt integer,
    qty integer
)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter   | Type     | Description |
| :---        |  :----   |  :--- |
| queue_name  | text     | The name of the queue   |
| vt          | integer  | Time in seconds that the message become invisible after reading |
| qty         | integer  | The number of messages to read from the queue |

**FIFO Behavior (Round Robin across groups):**
- Messages with the same `x-pgmq-group` header value are processed in strict order
- Only the oldest unprocessed message from each FIFO group can be read
- Messages from different FIFO groups can be processed in parallel
- Messages without FIFO headers are treated as a single default group

Examples:

Send messages with FIFO grouping:

```sql
-- Send messages to the same FIFO group
select pgmq.send('my_queue', msg=>'{"order": 1}', headers=>'{"x-pgmq-group": "user123"}');
select pgmq.send('my_queue', msg=>'{"order": 2}', headers=>'{"x-pgmq-group": "user123"}');
select pgmq.send('my_queue', msg=>'{"order": 1}', headers=>'{"x-pgmq-group": "user456"}');

-- Read with FIFO RR ordering - interleaves by group layers
select * from pgmq.read_grouped_rr('my_queue', 10, 5);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       1 | 2026-01-23 20:17:53.298176-06 | 2026-01-23 20:18:01.152451-06 | 2026-01-23 20:18:11.152451-06 | {"order": 1} | {"x-pgmq-group": "user123"}
      3 |       1 | 2026-01-23 20:17:53.302222-06 | 2026-01-23 20:18:01.152471-06 | 2026-01-23 20:18:11.152471-06 | {"order": 1} | {"x-pgmq-group": "user456"}
      2 |       1 | 2026-01-23 20:17:53.300656-06 | 2026-01-23 20:18:01.152468-06 | 2026-01-23 20:18:11.152468-06 | {"order": 2} | {"x-pgmq-group": "user123"}
(3 rows)
```

---

### read_grouped_rr_with_poll

Same as read_grouped_rr(). Also provides convenient long-poll functionality for FIFO queues.
 When there are no messages available that respect FIFO ordering, the function call will wait for `max_poll_seconds` in duration before returning.

<pre>
 <code>
 pgmq.read_grouped_rr_with_poll(
    queue_name text,
    vt integer,
    qty integer,
    max_poll_seconds integer DEFAULT 5,
    poll_interval_ms integer DEFAULT 100
)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| vt   | integer       | Time in seconds that the message become invisible after reading.      |
| qty   | integer        | The number of messages to read from the queue.      |
| max_poll_seconds   | integer        | Time in seconds to wait for new messages to reach the queue. Defaults to 5.      |
| poll_interval_ms   | integer        | Milliseconds between the internal poll operations. Defaults to 100.      |

Example:

```sql
select * from pgmq.read_grouped_rr_with_poll('my_queue', 10, 1, 5, 100);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       2 | 2026-01-23 20:17:53.298176-06 | 2026-01-23 20:18:35.678351-06 | 2026-01-23 20:18:45.678351-06 | {"order": 1} | {"x-pgmq-group": "user123"}
```

---

### pop

Reads one or more messages from a queue and deletes them upon read.

Note: utilization of pop() results in at-most-once delivery semantics if the consuming application does not guarantee processing of the message.

<pre>
 <code>
pgmq.pop(queue_name text, qty integer DEFAULT 1)
RETURNS SETOF <a href="../types/#message_record">pgmq.message_record</a>
 </code>
</pre>

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| qty      | integer       | The number of messages to pop from the queue. Defaults to 1.   |

Example:

```sql
pgmq=# select * from pgmq.pop('my_queue');
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       2 | 2026-01-23 20:17:53.298176-06 | 2026-01-23 20:18:35.678351-06 | 2026-01-23 20:18:45.678351-06 | {"order": 1} | {"x-pgmq-group": "user123"}
```

---

## Deleting/Archiving Messages

### delete (single)

Deletes a single message from a queue.

```text
pgmq.delete (queue_name text, msg_id: bigint)
RETURNS boolean
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_id      | bigint       | Message ID of the message to delete   |

Example:

```sql
select pgmq.delete('my_queue', 5);
 delete
--------
 t
```

---

### delete (batch)

Delete one or many messages from a queue.

```text
pgmq.delete (queue_name text, msg_ids: bigint[])
RETURNS SETOF bigint
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_ids      | bigint[]       | Array of message IDs to delete   |

Examples:

Delete two messages that exist.

```sql
select * from pgmq.delete('my_queue', ARRAY[2, 3]);
 delete
--------
      2
      3
```

Delete two messages, one that exists and one that does not. Message `999` does not exist.

```sql
select * from pgmq.delete('my_queue', ARRAY[6, 999]);
 delete
--------
      6
```

---

### purge_queue

Permanently deletes all messages in a queue. Returns the number of messages that were deleted.

```text
pgmq.purge_queue(queue_name text)
RETURNS bigint
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

Example:

Purge the queue when it contains 8 messages;

```sql
select * from pgmq.purge_queue('my_queue');
 purge_queue
-------------
           8
```

---

### archive (single)

Removes a single requested message from the specified queue and inserts it into the queue's archive.

```text
pgmq.archive(queue_name text, msg_id bigint)
RETURNS boolean
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_id      | bigint       | Message ID of the message to archive   |

**Returns:**
Boolean value indicating success or failure of the operation.

**Note:** Archived messages are stored in the archive table with an additional `archived_at` timestamp field indicating when the message was archived.

Example; remove message with ID 1 from queue `my_queue` and archive it:

```sql
SELECT * FROM pgmq.archive('my_queue', 1);
 archive
---------
       t
```

---

### archive (batch)

Deletes a batch of requested messages from the specified queue and inserts them into the queue's archive.
 Returns an ARRAY of message ids that were successfully archived.

```text
pgmq.archive(queue_name text, msg_ids bigint[])
RETURNS SETOF bigint
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_ids      | bigint[]       | Array of message IDs to archive   |

Examples:

Delete messages with ID 1 and 2 from queue `my_queue` and move to the archive.

```sql
SELECT * FROM pgmq.archive('my_queue', ARRAY[1, 2]);
 archive
---------
       1
       2
```

Delete messages 4, which exists and 999, which does not exist.

```sql
select * from pgmq.archive('my_queue', ARRAY[4, 999]);
 archive
---------
       4
```

---

## Queue Management

### create

Create a new queue.

```text
pgmq.create(queue_name text)
RETURNS VOID
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue (max 47 characters)   |

Example:

```sql
select from pgmq.create('my_queue');
 create
--------
```

---

### create_non_partitioned

Create a non-partitioned queue. This is the same as `create()`, but more explicit about the queue type.

```text
pgmq.create_non_partitioned(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue (max 47 characters)   |

Example:

```sql
select from pgmq.create_non_partitioned('my_queue');
 create_non_partitioned
------------------------
```

---

### create_partitioned

Create a partitioned queue. Requires the `pg_partman` extension to be installed.

```text
pgmq.create_partitioned (
    queue_name text,
    partition_interval text DEFAULT '10000'::text,
    retention_interval text DEFAULT '100000'::text
)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue (max 47 characters)   |
| partition_interval      | text       | Partition size - numeric value for msg_id-based partitioning (e.g., '10000'), or time interval for timestamp-based partitioning (e.g., '1 day'). Defaults to '10000'.   |
| retention_interval      | text       | How long/how many messages to retain before deleting old partitions. Same format as partition_interval. Defaults to '100000'.   |

Example:

Create a queue with 100,000 messages per partition, and will retain 10,000,000 messages on old partitions. Partitions greater than this will be deleted.

```sql
select from pgmq.create_partitioned(
    'my_partitioned_queue',
    '100000',
    '10000000'
);
 create_partitioned
--------------------
```

---

### create_unlogged

Creates an unlogged table. This is useful when write throughput is more important that durability.
See Postgres documentation for [unlogged tables](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED) for more information.

```text
pgmq.create_unlogged(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

Example:

```sql
select pgmq.create_unlogged('my_unlogged');
 create_unlogged
-----------------
```

---

### convert_archive_partitioned

Convert an existing non-partitioned archive table to a partitioned one. Requires the `pg_partman` extension to be installed. This is useful for migrating queues to partitioned archives after they have been created.

```text
pgmq.convert_archive_partitioned(
    table_name text,
    partition_interval text DEFAULT '10000'::text,
    retention_interval text DEFAULT '100000'::text,
    leading_partition integer DEFAULT 10
)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| table_name      | text       | The name of the queue whose archive should be partitioned   |
| partition_interval      | text       | Partition size - numeric value for msg_id-based partitioning (e.g., '10000'), or time interval for timestamp-based partitioning (e.g., '1 day'). Defaults to '10000'.   |
| retention_interval      | text       | How long/how many messages to retain before deleting old partitions. Same format as partition_interval. Defaults to '100000'.   |
| leading_partition      | integer       | Number of partitions to create in advance. Defaults to 10.   |

**Note:** This function renames the existing archive table to `<table_name>_old` and creates a new partitioned table. You may need to migrate data from the old table to the new one.

Example:

```sql
select from pgmq.convert_archive_partitioned('my_queue', '10000', '100000');
 convert_archive_partitioned
-----------------------------
```

---

### detach_archive

**⚠️ DEPRECATED:** This function is deprecated and is now a no-op (does nothing). It will be removed in PGMQ v2.0. Archive tables are no longer member objects of the extension.

Drop the queue's archive table as a member of the PGMQ extension. Useful for preventing the queue's archive table from being drop when `DROP EXTENSION pgmq` is executed.
 This does not prevent the further archives() from appending to the archive table.

```text
pgmq.detach_archive(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

Example:

```sql
select * from pgmq.detach_archive('my_queue');
 detach_archive
----------------
```

---

### drop_queue

Deletes a queue and its archive table.

```text
pgmq.drop_queue(queue_name text)
RETURNS boolean
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

**Note:** There is a deprecated 2-parameter version `drop_queue(queue_name, partitioned)` that will be removed in PGMQ v2.0. Use the single-parameter version instead, which automatically detects whether the queue is partitioned.

Example:

```sql
select * from pgmq.drop_queue('my_unlogged');
 drop_queue
------------
 t
```

## Utilities

### set_vt (single)

Sets the visibility timeout of a message to a specified time duration in the future. Returns the record of the message that was updated.

```text
pgmq.set_vt(queue_name text, msg_id bigint, vt integer)
pgmq.set_vt(queue_name text, msg_id bigint, vt timestamp with time zone)

RETURNS SETOF pgmq.message_record
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name  | text         | The name of the queue   |
| msg_id      | bigint       | ID of the message to set visibility time  |
| vt   | integer      | Duration from now, in seconds, that the message's VT should be set to   |
| vt   | timestamp with time zone | Timestamp when the message becomes visible   |

Example:

Set the visibility timeout of message 1 to 30 seconds from now.

```sql
select * from pgmq.set_vt('my_queue', 1, 30);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       1 | 2026-01-23 20:17:53.300656-06 | 2026-01-23 20:18:01.152468-06 | 2026-01-23 20:24:49.398759-06 | {"order": 2} | {"x-pgmq-group": "user123"}
```

---

### set_vt (batch)

Sets the visibility timeout of multiple messages to a specified time duration in the future. Returns the records of the messages that were updated.

```text
pgmq.set_vt(queue_name text, msg_ids bigint[], vt integer)
pgmq.set_vt(queue_name text, msg_ids bigint[], vt timestamp with time zone)

RETURNS SETOF pgmq.message_record
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name  | text         | The name of the queue   |
| msg_ids      | bigint[]       | Array of message IDs to set visibility time  |
| vt   | integer      | Duration from now, in seconds, that the messages' VT should be set to   |
| vt   | timestamp with time zone | Timestamp when the messages become visible   |

Example:

Set the visibility timeout of messages 1 and 2 to 60 seconds from now.

```sql
select * from pgmq.set_vt('my_queue', ARRAY[1, 2], 60);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |   message    |           headers           
--------+---------+-------------------------------+-------------------------------+-------------------------------+--------------+-----------------------------
      1 |       1 | 2026-01-23 20:17:53.302222-06 | 2026-01-23 20:18:01.152471-06 | 2026-01-23 20:25:53.789922-06 | {"order": 1} | {"x-pgmq-group": "user456"}
      2 |       1 | 2026-01-23 20:17:53.300656-06 | 2026-01-23 20:18:01.152468-06 | 2026-01-23 20:25:53.789922-06 | {"order": 2} | {"x-pgmq-group": "user123"}
```

---

### list_queues

List all the queues that currently exist.

```sql
pgmq.list_queues()
RETURNS SETOF pgmq.queue_record
```

Example:

```sql
select * from pgmq.list_queues();
      queue_name      |          created_at           | is_partitioned | is_unlogged
----------------------+-------------------------------+----------------+-------------
 my_queue             | 2023-10-28 14:13:17.092576-05 | f              | f
 my_partitioned_queue | 2023-10-28 19:47:37.098692-05 | t              | f
 my_unlogged          | 2023-10-28 20:02:30.976109-05 | f              | t
```

---

### metrics

Get metrics for a specific queue.

```text
pgmq.metrics(queue_name text)
RETURNS pgmq.metrics_result
```

**Parameters:**

| Parameter   | Type  | Description             |
| :---        | :---- |                    :--- |
| queue_name  | text  | The name of the queue   |

**Returns:**

| Attribute      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name  | text         | The name of the queue   |
| queue_length      | bigint       | Number of messages currently in the queue  |
| newest_msg_age_sec   | integer \| null     | Age of the newest message in the queue, in seconds   |
| oldest_msg_age_sec   | integer \| null    | Age of the oldest message in the queue, in seconds   |
| total_messages   | bigint      | Total number of messages that have passed through the queue over all time   |
| scrape_time   | timestamp with time zone      | The current timestamp   |
| queue_visible_length | bigint | Number of messages currently visible (vt <= now) |

Example:

```sql
select * from pgmq.metrics('my_queue');
 queue_name | queue_length | newest_msg_age_sec | oldest_msg_age_sec | total_messages |          scrape_time
------------+--------------+--------------------+--------------------+----------------+-------------------------------
 my_queue   |           16 |               2445 |               2447 |             35 | 2023-10-28 20:23:08.406259-05
```

---

### metrics_all

Get metrics for all existing queues.

```text
pgmq.metrics_all()
RETURNS SETOF pgmq.metrics_result
```

**Returns:**

| Attribute      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name  | text         | The name of the queue   |
| queue_length      | bigint       | Number of messages currently in the queue  |
| newest_msg_age_sec   | integer \| null     | Age of the newest message in the queue, in seconds   |
| oldest_msg_age_sec   | integer \| null    | Age of the oldest message in the queue, in seconds   |
| total_messages   | bigint      | Total number of messages that have passed through the queue over all time   |
| scrape_time   | timestamp with time zone      | The current timestamp   |
| queue_visible_length | bigint | Number of messages currently visible (vt <= now) |

```sql
select * from pgmq.metrics_all();
      queue_name      | queue_length | newest_msg_age_sec | oldest_msg_age_sec | total_messages |          scrape_time
----------------------+--------------+--------------------+--------------------+----------------+-------------------------------
 my_queue             |           16 |               2563 |               2565 |             35 | 2023-10-28 20:25:07.016413-05
 my_partitioned_queue |            1 |                 11 |                 11 |              1 | 2023-10-28 20:25:07.016413-05
 my_unlogged          |            1 |                  3 |                  3 |              1 | 2023-10-28 20:25:07.016413-05
```
---

### enable_notify_insert

Enable PostgreSQL NOTIFY triggers for a queue with optional throttling. When enabled, a notification is sent on the channel `pgmq.<queue_table>.INSERT` every time a message is inserted (subject to throttling). This allows applications to use `LISTEN` to be notified immediately when new messages arrive, instead of polling.

```text
pgmq.enable_notify_insert(queue_name text, throttle_interval_ms integer DEFAULT 250)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| throttle_interval_ms | integer | Minimum milliseconds between notifications. Set to 0 to disable throttling. Defaults to 250. |

**Notes:**

- The notification channel will be named `pgmq.q_<queue_name>.INSERT` where `q_<queue_name>` is the internal table name.

- **Throttling behavior**: Throttling prevents excessive notifications during high-volume inserts. When multiple messages are inserted rapidly, only one notification per throttle interval will be sent. This protects your system from notification overhead when message volume is high.

- **When to use notifications**: Notifications are most valuable for queues with sporadic or low-volume traffic. During high-volume periods, consumers can continuously poll and expect work to be present. However, when there are longer gaps between messages, notifications allow the consumer to wait idle and only poll when it receives a notification, significantly reducing unnecessary polling overhead. The throttling feature ensures that during burst traffic, you don't create excessive notifications while still maintaining the notification behavior during low-volume periods.

Examples:

```sql
-- Enable notifications with default 250ms throttling
select pgmq.enable_notify_insert('my_queue');
 enable_notify_insert
----------------------

-- Enable notifications with custom 500ms throttling
select pgmq.enable_notify_insert('my_queue', 500);
 enable_notify_insert
----------------------

-- Enable notifications with no throttling (0ms)
select pgmq.enable_notify_insert('my_queue', 0);
 enable_notify_insert
----------------------

-- In another session, listen for notifications:
-- LISTEN "pgmq.q_my_queue.INSERT";
```

**Changing throttling after enabling notifications:**

Use `update_notify_insert()` to modify the throttling interval for an existing queue:

```sql
-- Change throttling to 1000ms (1 second)
SELECT pgmq.update_notify_insert('my_queue', 1000);

-- Disable throttling (set to 0ms)
SELECT pgmq.update_notify_insert('my_queue', 0);
```

See [`update_notify_insert`](#update_notify_insert) for more details.

---

### disable_notify_insert

Disable PostgreSQL NOTIFY triggers for a queue that were enabled with `enable_notify_insert()`.

```text
pgmq.disable_notify_insert(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

Example:

```sql
select pgmq.disable_notify_insert('my_queue');
 disable_notify_insert
-----------------------
```

---

### update_notify_insert

Update the throttle interval for a queue that has notifications enabled. This allows you to change the throttling rate without disabling and re-enabling notifications.

```text
pgmq.update_notify_insert(queue_name text, throttle_interval_ms integer)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| throttle_interval_ms | integer | Minimum milliseconds between notifications (0 = no throttling) |

**Notes:**

- The queue must have notifications enabled via `enable_notify_insert()` first
- Setting to 0 disables throttling (sends notification on every insert)
- Updating the throttle resets `last_notified_at` to ensure immediate notification on next insert
- `throttle_interval_ms` must be non-negative

Example:

```sql
-- Change throttling to 1 second
select pgmq.update_notify_insert('my_queue', 1000);
 update_notify_insert
----------------------
```

---

### list_notify_insert_throttles

Returns all notification throttle configurations for queues that have notifications enabled.

```text
pgmq.list_notify_insert_throttles()
RETURNS TABLE (
    queue_name text,
    throttle_interval_ms integer,
    last_notified_at timestamp with time zone
)
```

**Returns:**

| Column      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| throttle_interval_ms | integer | Minimum milliseconds between notifications |
| last_notified_at | timestamp with time zone | Timestamp of the last notification sent |

**Notes:**

- Only returns queues that have notifications enabled
- Results are ordered by queue_name
- Empty result if no queues have notifications enabled

Example:

```sql
select * from pgmq.list_notify_insert_throttles();
   queue_name   | throttle_interval_ms |      last_notified_at
----------------+----------------------+----------------------------
 my_queue       |                 1000 | 2024-01-15 10:30:45.123456
 another_queue  |                    0 | 1970-01-01 00:00:00.000000
```

---

### create_fifo_index

Creates a GIN index on the headers column for a specific queue to improve FIFO read performance. This is recommended when using FIFO functionality frequently on a queue.

```text
pgmq.create_fifo_index(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

Example:

```sql
select pgmq.create_fifo_index('my_queue');
 create_fifo_index
-------------------
```

---

### create_fifo_indexes_all

Creates FIFO indexes on all existing queues. This is a convenience function to optimize all queues for FIFO operations.

```text
pgmq.create_fifo_indexes_all()
RETURNS void
```

Example:

```sql
select pgmq.create_fifo_indexes_all();
 create_fifo_indexes_all
-------------------------
```
