# Types

## message_record

The complete representation of a message in a queue.

| Attribute Name   | Type       | Description                |
| :---             |    :----   |                       :--- |
| msg_id           | bigint     | Unique ID of the message   |
| read_ct          | integer     | Number of times the message has been read. Increments on read().   |
| enqueued_at           |  timestamp with time zone     | time that the message was inserted into the queue   |
| vt           | timestamp with time zone      | Timestamp when the message will become available for consumers to read   |
| message           | jsonb      | The message payload   |
| headers           | jsonb      | Optional message headers/metadata   |


Example:

```text
 msg_id | read_ct |          enqueued_at          |              vt               |      message        | headers
--------+---------+-------------------------------+-------------------------------+---------------------+---------
      1 |       1 | 2023-10-28 19:06:19.941509-05 | 2023-10-28 19:06:27.419392-05 | {"hello": "world"}  |
```

## queue_record

Represents metadata about a queue.

| Attribute Name   | Type       | Description                |
| :---             |    :----   |                       :--- |
| queue_name       | varchar    | Name of the queue   |
| is_partitioned   | boolean    | Whether the queue is partitioned   |
| is_unlogged      | boolean    | Whether the queue is unlogged (higher performance, less durability)   |
| created_at       | timestamp with time zone | When the queue was created   |

Example:

```text
      queue_name      |          created_at           | is_partitioned | is_unlogged
----------------------+-------------------------------+----------------+-------------
 my_queue             | 2023-10-28 14:13:17.092576-05 | f              | f
```

## metrics_result

Contains metrics and statistics for a queue.

| Attribute Name   | Type       | Description                |
| :---             |    :----   |                       :--- |
| queue_name       | text       | Name of the queue   |
| queue_length     | bigint     | Total number of messages currently in the queue   |
| newest_msg_age_sec | integer  | Age of the newest message in seconds (null if queue is empty)   |
| oldest_msg_age_sec | integer  | Age of the oldest message in seconds (null if queue is empty)   |
| total_messages   | bigint     | Total number of messages that have ever been in the queue   |
| scrape_time      | timestamp with time zone | Timestamp when metrics were collected   |
| queue_visible_length | bigint | Number of messages currently visible (vt <= now)   |

Example:

```text
 queue_name | queue_length | newest_msg_age_sec | oldest_msg_age_sec | total_messages |          scrape_time          | queue_visible_length
------------+--------------+--------------------+--------------------+----------------+-------------------------------+---------------------
 my_queue   |           16 |               2445 |               2447 |             35 | 2023-10-28 20:23:08.406259-05 |                  12
```