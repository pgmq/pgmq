
# Partitioned Queues

You will need to install [pg_partman](https://github.com/pgpartman/pg_partman/) if you want to use `pgmq` partitioned queues.


## Overview

`pgmq` queue tables can be created as a partitioned table by using `pgmq.create_partitioned()`. [pg_partman](https://github.com/pgpartman/pg_partman/)
handles all maintenance of queue tables. This includes creating new partitions and dropping old partitions.

Partitions behavior is configured at the time queues are created, via `pgmq.create_partitioned()`. This function has three parameters:

## Parameters

`queue_name: text`: The name of the queue. Queues are Postgres tables prepended with `q_`. For example, `q_my_queue`. The archive is instead prefixed by `a_`, for example `a_my_queue`.

`partition_interval: text` - The interval at which partitions are created. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, queues are partitioned by the time at which messages are sent to the table (`enqueued_at`). A value of `'daily'` would create a new partition each day. When it is an integer value, queues are partitioned by the `msg_id`. A value of `'100'` will create a new partition every 100 messages. The value must agree with `retention_interval` (time based or numeric). The default value is `'10000'`. For archive table, when interval is an integer value, then it will be partitioned by `msg_id`. In case of duration it will be partitioned on `archived_at` unlike queue table.

`retention_interval: text` - The interval for retaining partitions. This can be either any valid Postgres `Duration` supported by pg_partman, or an integer value. When it is a duration, partitions containing data greater than the duration will be dropped. When it is an integer value, any messages that have a `msg_id` less than `max(msg_id) - retention_interval` will be dropped. For example, if the max `msg_id` is 100 and the `retention_interval` is 60, any partitions with `msg_id` values less than 40 will be dropped. The value must agree with `partition_interval` (time based or numeric). The default is `'100000'`. Note: `retention_interval` does not apply to messages that have been deleted via `pgmq.delete()` or archived with `pgmq.archive()`. `pgmq.delete()` removes messages forever and `pgmq.archive()` moves messages to the corresponding archive table forever (for example, `a_my_queue`).

## Partition Maintenance

In order for automatic partition maintenance to take place, several settings must be added to the `postgresql.conf` file, which is typically located in the postgres `DATADIR`.
`pg_partman_bgw.interval`
in `postgresql.conf`. Below are the default configuration values set in pgmq docker images.

Add the following to `postgresql.conf`. Note, changing `shared_preload_libraries` requires a restart of Postgres.

`pg_partman_bgw.interval` sets the interval at which `pg_partman` conducts maintenance. This creates new partitions and dropping of partitions falling out of the `retention_interval`. By default, `pg_partman` will keep 4 partitions "ahead" of the currently active partition.

```text
shared_preload_libraries = 'pg_partman_bgw' # requires restart of Postgres
pg_partman_bgw.interval = 60
pg_partman_bgw.role = 'postgres'
pg_partman_bgw.dbname = 'postgres'
```