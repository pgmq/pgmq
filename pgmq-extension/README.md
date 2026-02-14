# Postgres Message Queue (PGMQ)

A lightweight message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%20%7C%2014%20%7C%2015%20%7C%2016%20%7C%2017%20%7C%2018-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![PGXN version](https://badge.fury.io/pg/pgmq.svg)](https://pgxn.org/dist/pgmq/)

**Documentation**: https://pgmq.github.io/pgmq/

**Source**: https://github.com/pgmq/pgmq

<img src="https://github.com/user-attachments/assets/3e6d23c5-83fa-4c4a-98ae-77f305e3cd5c" width="75%" alt="pgmq demo gif">

## Features

- Lightweight - No background worker or external dependencies, just Postgres SQL objects
- Guaranteed "exactly once" delivery of messages to a consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- [FIFO](docs/fifo-queues.md#overview) (First-In-First-Out) queues with message group keys for ordered processing
- [Topic-based](docs/topics.md#topic-based-routing) routing with wildcard patterns for publish-subscribe and content-based routing
- Messages stay in the queue until explicitly removed
- Messages can be archived, instead of deleted, for long-term retention and replayability

Supported on Postgres 14-18.

## Table of Contents

- [Postgres Message Queue (PGMQ)](#postgres-message-queue-pgmq)
  - [Features](#features)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Docker](#docker)
    - [SQL Only](#sql-only)
    - [Updating](#updating)
  - [Client Libraries](#client-libraries)
  - [SQL Examples](#sql-examples)
    - [Creating a queue](#creating-a-queue)
    - [Send two messages](#send-two-messages)
    - [Read messages](#read-messages)
    - [Pop a message](#pop-a-message)
    - [Archive a message](#archive-a-message)
    - [Delete a message](#delete-a-message)
    - [Drop a queue](#drop-a-queue)
  - [Visibility Timeout (vt)](#visibility-timeout-vt)
  - [Who uses pgmq?](#who-uses-pgmq)
  - [✨ Contributors](#-contributors)

## Installation

PGMQ can be run on any existing Postgres instance or installed as a Postgres Extension. See [INSTALLATION.md](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) for the full installation guide including a [comparison](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md#considerations) of the Postgres Extension vs the SQL-only installation.

### Docker

The fastest way to get started is by running the Docker image, where PGMQ comes pre-installed as an extension in Postgres.

```bash
docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

Then connect and enable PGMQ:

```bash
psql postgres://postgres:postgres@localhost:5432/postgres
```

```sql
CREATE EXTENSION pgmq;
```

### SQL Only

You can also use [psql](https://www.tigerdata.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows) to install PGMQ's objects directly into the pgmq schema in Postgres. Use this method if you are running someplace that does not natively support the PGMQ Extension. Read these [considerations](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md#considerations) before you decide.

```bash
git clone https://github.com/pgmq/pgmq.git

cd pgmq

psql -f pgmq-extension/sql/pgmq.sql postgres://postgres:postgres@localhost:5432/postgres
```

### Updating

To update PGMQ versions, follow the instructions in [UPDATING.md](pgmq-extension/UPDATING.md).

## Client Libraries

- [Rust](https://github.com/pgmq/pgmq/tree/main/pgmq-rs)
- [Python (only for psycopg3)](https://github.com/pgmq/pgmq-py)

Community

- [.NET](https://github.com/brianpursley/Npgmq)
- [Dart](https://github.com/Ofceab-Studio/dart_pgmq)
- [Elixir + Broadway](https://github.com/v0idpwn/off_broadway_pgmq)
- [Elixir](https://github.com/v0idpwn/pgmq-elixir)
- [Go](https://github.com/craigpastro/pgmq-go)
- [Haskell](https://github.com/MichelBoucey/stakhanov)
- [Java (JDBC)](https://github.com/roy20021/pgmq-jdbc-client)
- [Java (Spring Boot)](https://github.com/adamalexandru4/pgmq-spring)
- [Javascript (NodeJs)](https://github.com/Muhammad-Magdi/pgmq-js)
- [Kotlin JVM (JDBC)](https://github.com/vdsirotkin/pgmq-kotlin-jvm)
- [Kotlin Multiplatform (sqlx4k)](https://github.com/smyrgeorge/sqlx4k/tree/main/sqlx4k-postgres-pgmq)
- [PHP (non blocking)](https://github.com/thesis-php/pgmq)
- [Python (with SQLAlchemy)](https://github.com/jason810496/pgmq-sqlalchemy)
- [REST-API (Bun + Elysia)](https://github.com/eichenroth/pgmq-rest)
- [Ruby](https://github.com/mensfeld/pgmq-ruby)
- [TypeScript (Deno)](https://github.com/tmountain/deno-pgmq)
- [TypeScript (NodeJs + Prisma)](https://github.com/dvlkv/prisma-pgmq) 
- [TypeScript (NodeJs + Midway.js)](https://github.com/waitingsong/pgmq-js)

## SQL Examples

```bash
# Connect to Postgres
psql postgres://postgres:postgres@0.0.0.0:5432/postgres
```

```sql
-- create the extension in the "pgmq" schema
CREATE EXTENSION pgmq;
```

### Creating a queue

Every queue is its own table in the `pgmq` schema. The table name is the queue name prefixed with `q_`.
For example, `pgmq.q_my_queue` is the table for the queue `my_queue`.

```sql
-- creates the queue
SELECT pgmq.create('my_queue');
```

```text
 create
-------------

(1 row)
```

### Send two messages

```sql
-- messages are sent as JSON
SELECT * from pgmq.send(
  queue_name  => 'my_queue',
  msg         => '{"foo": "bar1"}'
);
```

The message id is returned from the send function.

```text
 send
-----------
         1
(1 row)
```

```sql
-- Optionally provide a delay
-- this message will be on the queue but unable to be consumed for 5 seconds
SELECT * from pgmq.send(
  queue_name => 'my_queue',
  msg        => '{"foo": "bar2"}',
  delay      => 5
);
```

```text
 send
-----------
         2
(1 row)
```

### Read messages

Read `2` message from the queue. Make them invisible for `30` seconds.
If the messages are not deleted or archived within 30 seconds, they will become visible again
and can be read by another consumer.

```sql
SELECT * FROM pgmq.read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 2
);
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |              vt               |     message     | headers 
--------+---------+-------------------------------+-------------------------------+-------------------------------+-----------------+---------
      1 |       1 | 2026-01-23 20:27:21.7741-06   | 2026-01-23 20:27:31.605236-06 | 2026-01-23 20:28:01.605236-06 | {"foo": "bar1"} | 
      2 |       1 | 2026-01-23 20:27:26.505063-06 | 2026-01-23 20:27:31.605252-06 | 2026-01-23 20:28:01.605252-06 | {"foo": "bar2"} | 
```

If the queue is empty, or if all messages are currently invisible, no rows will be returned.

```sql
SELECT * FROM pgmq.read(
  queue_name => 'my_queue',
  vt         => 30,
  qty        => 1
);
```

```text
 msg_id | read_ct | enqueued_at | last_read_at | vt | message | headers 
--------+---------+-------------+--------------+----+---------+---------
```

### Pop a message

```sql
-- Read a message and immediately delete it from the queue. Returns an empty record if the queue is empty or all messages are invisible.
SELECT * FROM pgmq.pop('my_queue');
```

```text
 msg_id | read_ct |         enqueued_at         |         last_read_at          |              vt               |     message     | headers 
--------+---------+-----------------------------+-------------------------------+-------------------------------+-----------------+---------
      1 |       1 | 2026-01-23 20:27:21.7741-06 | 2026-01-23 20:27:31.605236-06 | 2026-01-23 20:28:01.605236-06 | {"foo": "bar1"} | 
```

### Archive a message

Archiving a message removes it from the queue and inserts it to the archive table.

```sql
-- Archive message with msg_id=2.
SELECT pgmq.archive(
  queue_name => 'my_queue',
  msg_id     => 2
);
```

```text
 archive
--------------
 t
(1 row)
```

Or archive several messages in one operation using `msg_ids` (plural) parameter:

First, send a batch of messages

```sql
SELECT pgmq.send_batch(
  queue_name => 'my_queue',
  msgs       => ARRAY['{"foo": "bar3"}','{"foo": "bar4"}','{"foo": "bar5"}']::jsonb[]
);
```

```text
 send_batch 
------------
          3
          4
          5
(3 rows)
```

Then archive them by using the msg_ids (plural) parameter.

```sql
SELECT pgmq.archive(
  queue_name => 'my_queue',
  msg_ids    => ARRAY[3, 4, 5]
);
```

```text
 archive 
---------
       3
       4
       5
(3 rows)
```

Archive tables can be inspected directly with SQL.
 Archive tables have the prefix `a_` in the `pgmq` schema.

```sql
SELECT * FROM pgmq.a_my_queue;
```

```text
 msg_id | read_ct |          enqueued_at          |         last_read_at          |          archived_at          |              vt               |     message     | headers 
--------+---------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+-----------------+---------
      2 |       1 | 2026-01-23 20:32:35.291971-06 | 2026-01-23 20:32:42.938473-06 | 2026-01-23 20:33:20.297454-06 | 2026-01-23 20:33:12.938473-06 | {"foo": "bar2"} | 
      3 |       0 | 2026-01-23 20:33:25.414914-06 |                               | 2026-01-23 20:33:30.318465-06 | 2026-01-23 20:33:25.415035-06 | {"foo": "bar3"} | 
      4 |       0 | 2026-01-23 20:33:25.414914-06 |                               | 2026-01-23 20:33:30.318465-06 | 2026-01-23 20:33:25.415035-06 | {"foo": "bar4"} | 
      5 |       0 | 2026-01-23 20:33:25.414914-06 |                               | 2026-01-23 20:33:30.318465-06 | 2026-01-23 20:33:25.415035-06 | {"foo": "bar5"} | 
```

### Delete a message

Send another message, so that we can delete it.

```sql
SELECT pgmq.send('my_queue', '{"foo": "bar6"}');
```

```text
 send
-----------
        6
(1 row)
```

Delete the message with id `6` from the queue named `my_queue`.

```sql
SELECT pgmq.delete('my_queue', 6);
```

```text
 delete
-------------
 t
(1 row)
```

### Drop a queue

Delete the queue `my_queue`.

```sql
SELECT pgmq.drop_queue('my_queue');
```

```text
 drop_queue
-----------------
 t
(1 row)
```

## Visibility Timeout (vt)

pgmq guarantees exactly once delivery of a message within a visibility timeout. The visibility timeout is the amount of time a message is invisible to other consumers after it has been read by a consumer. If the message is NOT deleted or archived within the visibility timeout, it will become visible again and can be read by another consumer. The visibility timeout is set when a message is read from the queue, via `pgmq.read()`. It is recommended to set a `vt` value that is greater than the expected time it takes to process a message. After the application successfully processes the message, it should call `pgmq.delete()` to completely remove the message from the queue or `pgmq.archive()` to move it to the archive table for the queue.

## Who uses pgmq?

As the pgmq community grows, we'd love to see who is using it. Please send a PR with your company name and @githubhandle.

Currently, officially using pgmq:

1. [Tembo](https://tembo.io) [[@Tembo-io](https://github.com/tembo-io)]
2. [Supabase](https://supabase.com) [[@Supabase](https://github.com/supabase)]
3. [Sprinters](https://sprinters.sh) [[@sprinters-sh](https://github.com/sprinters-sh)]
4. [pgflow](https://pgflow.dev) [[@pgflow-dev/pgflow](https://github.com/pgflow-dev/pgflow)]

## ✨ Contributors

Thanks goes to these incredible people:

<a href="https://github.com/pgmq/pgmq/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=pgmq/pgmq" />
</a>
