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
 msg_id | read_ct |          enqueued_at          |              vt               |       message        | headers
--------+---------+-------------------------------+-------------------------------+----------------------+---------
      1 |       1 | 2023-10-28 19:14:47.356595-05 | 2023-10-28 19:17:08.608922-05 | {"hello": "world_0"} |
      2 |       1 | 2023-10-28 19:14:47.356595-05 | 2023-10-28 19:17:08.608974-05 | {"hello": "world_1"} |
(2 rows)
```

Read a message from a queue with message filtering

```sql
select * from pgmq.read('my_queue', 10, 2, '{"hello": "world_1"}');
 msg_id | read_ct |          enqueued_at          |              vt               |       message        | headers
--------+---------+-------------------------------+-------------------------------+----------------------+---------
      2 |       1 | 2023-10-28 19:14:47.356595-05 | 2023-10-28 19:17:08.608974-05 | {"hello": "world_1"} |
(1 row)
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
select * from pgmq.read_with_poll('my_queue', 1, 1, 5, 100);
 msg_id | read_ct |          enqueued_at          |              vt               |      message       | headers
--------+---------+-------------------------------+-------------------------------+--------------------+---------
      1 |       1 | 2023-10-28 19:09:09.177756-05 | 2023-10-28 19:27:00.337929-05 | {"hello": "world"} |
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
 msg_id | read_ct |          enqueued_at          |              vt               |      message       | headers
--------+---------+-------------------------------+-------------------------------+--------------------+---------
      1 |       2 | 2023-10-28 19:09:09.177756-05 | 2023-10-28 19:27:00.337929-05 | {"hello": "world"} |
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

### set_vt

Sets the visibility timeout of a message to a specified time duration in the future. Returns the record of the message that was updated.

```text
pgmq.set_vt(
    queue_name text,
    msg_id bigint,
    vt integer
)
RETURNS SETOF pgmq.message_record
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name  | text         | The name of the queue   |
| msg_id      | bigint       | ID of the message to set visibility time  |
| vt   | integer      | Duration from now, in seconds, that the message's VT should be set to   |

Example:

Set the visibility timeout of message 1 to 30 seconds from now.

```sql
select * from pgmq.set_vt('my_queue', 11, 30);
 msg_id | read_ct |          enqueued_at          |              vt               |       message        | headers
--------+---------+-------------------------------+-------------------------------+----------------------+---------
     1 |       0 | 2023-10-28 19:42:21.778741-05 | 2023-10-28 19:59:34.286462-05 | {"hello": "world_0"} |
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

Enable PostgreSQL NOTIFY triggers for a queue. When enabled, a notification is sent on the channel `pgmq.<queue_table>.INSERT` every time a message is inserted. This allows applications to use `LISTEN` to be notified immediately when new messages arrive, instead of polling.

```text
pgmq.enable_notify_insert(queue_name text)
RETURNS void
```

**Parameters:**

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

**Note:** The notification channel will be named `pgmq.q_<queue_name>.INSERT` where `q_<queue_name>` is the internal table name.

Example:

```sql
select pgmq.enable_notify_insert('my_queue');
 enable_notify_insert
----------------------

-- In another session, listen for notifications:
-- LISTEN "pgmq.q_my_queue.INSERT";
```

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
