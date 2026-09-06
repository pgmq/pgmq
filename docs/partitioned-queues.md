
# Partitioned Queues

You will need to install [pg_partman](https://github.com/pgpartman/pg_partman/) if you want to use `pgmq` partitioned queues.


## Overview

`pgmq` queue tables can be created as a partitioned table by using `pgmq.create_partitioned()`. [pg_partman](https://github.com/pgpartman/pg_partman/)
handles all maintenance of queue tables. This includes creating new partitions and dropping old partitions.

Partitions behavior is configured at the time queues are created, via `pgmq.create_partitioned()`. This function has four parameters:

## Parameters

`queue_name: text`: The name of the queue. Queues are Postgres tables prepended with `q_`. For example, `q_my_queue`. The archive is instead prefixed by `a_`, for example `a_my_queue`.

`partition_interval: text` - The interval at which partitions are created. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, queues are partitioned by the time at which messages are sent to the table (`enqueued_at`). A value of `'daily'` would create a new partition each day. When it is an integer value, queues are partitioned by the `msg_id`. A value of `'100'` will create a new partition every 100 messages. The value must agree with `retention_interval` (time based or numeric). The default value is `'10000'`. For archive table, when interval is an integer value, then it will be partitioned by `msg_id`. In case of duration it will be partitioned on `archived_at` unlike queue table.

`retention_interval: text` - The interval for retaining partitions. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, partitions containing data greater than the duration will be dropped. When it is an integer value, any messages that have a `msg_id` less than `max(msg_id) - retention_interval` will be dropped. For example, if the max `msg_id` is 100 and the `retention_interval` is 60, any partitions with `msg_id` values less than 40 will be dropped. The value must agree with `partition_interval` (time based or numeric). The default is `'100000'`. Note: `retention_interval` does not apply to messages that have been deleted via `pgmq.delete()` or archived with `pgmq.archive()`. `pgmq.delete()` removes messages forever and `pgmq.archive()` moves messages to the corresponding archive table forever (for example, `a_my_queue`).

`premake: integer` - How many partitions `pg_partman` keeps created ahead of the one currently receiving messages. The default is `4`, which is `pg_partman`'s own default. Together with `partition_interval` this sets how many messages (or how much time) a queue can absorb between two maintenance runs; see [Runway and overrun](#runway-and-overrun) below.

## Partition Maintenance

In order for automatic partition maintenance to take place, several settings must be added to the `postgresql.conf` file, which is typically located in the postgres `DATADIR`.
`pg_partman_bgw.interval`
in `postgresql.conf`. Below are the default configuration values set in pgmq docker images.

Add the following to `postgresql.conf`. Note, changing `shared_preload_libraries` requires a restart of Postgres.

`pg_partman_bgw.interval` sets the interval at which `pg_partman` conducts maintenance. This creates new partitions and dropping of partitions falling out of the `retention_interval`. By default, `pg_partman` will keep 4 partitions "ahead" of the currently active partition; `pgmq.create_partitioned()` exposes this as `premake`.

```text
shared_preload_libraries = 'pg_partman_bgw' # requires restart of Postgres
pg_partman_bgw.interval = 60
pg_partman_bgw.role = 'postgres'
pg_partman_bgw.dbname = 'postgres'
```

## Runway and overrun

Maintenance creates partitions ahead of the current one, so between two maintenance runs a queue has a fixed runway: roughly `partition_interval × premake` messages for `msg_id`-partitioned queues, or that much time for `enqueued_at`-partitioned ones. With the defaults (`partition_interval = '10000'`, `premake = 4`, `pg_partman_bgw.interval = 60`) that is about 40,000 messages per minute. A sustained rate above that, or a single burst larger than the runway, crosses it.

Crossing it does not fail. `pgmq.send()` keeps succeeding, because `pg_partman` creates a default partition and the messages that have no partition of their own land there. Reads keep working too. What breaks is maintenance: `pg_partman` cannot create a partition for a range whose rows are already sitting in the default partition, so `run_maintenance` starts failing for that queue, no further partitions are created ahead, and retention stops dropping anything.

Size the runway for the traffic you expect. `premake` is the cheap knob: an unused partition costs almost nothing, and ten intervals of runway are not unreasonable for a busy queue.

### Monitoring

`pgmq.metrics()` reports `default_partition_length`, the messages sitting in the default partitions of a partitioned queue and of its archive (`null` for queues that are not partitioned). Zero is the only healthy value. A non-zero value means maintenance for that queue is already failing, not that some messages are merely misfiled, so alert on it directly. The value is the planner's estimate rather than a count, so a large spill does not make every scrape slow; it catches up when autovacuum or `ANALYZE` next visits the default partition.

### Recovery

Move the messages out of the default partition into the partitions they belong to, then let maintenance catch up. `pg_partman` provides the procedure for this; run it outside a transaction block, because it commits as it goes. The `ANALYZE` at the end refreshes the statistics `default_partition_length` is read from, so the metric returns to zero right away rather than at the next autovacuum:

```sql
CALL partman.partition_data_proc('pgmq.q_my_queue');
SELECT partman.run_maintenance('pgmq.q_my_queue');
ANALYZE pgmq.q_my_queue_default;
```

The archive table `pgmq.a_my_queue` is partitioned the same way; if it has spilled too, run the same three statements against it.

`msg_id` on partitioned queues is declared `GENERATED BY DEFAULT AS IDENTITY` (rather than `ALWAYS`) precisely so that this move can reinsert messages with their existing ids. The trade-off is that PostgreSQL no longer rejects an explicit `msg_id` in an `INSERT`. `pgmq` never supplies one, but if something writes to a queue table directly and provides an id ahead of the sequence, a later `pgmq.send()` will collide with it. Go through the `pgmq` API.
