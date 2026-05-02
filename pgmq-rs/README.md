# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

PGMQ is a lightweight, distributed message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but native to Postgres.

This crate is the official Rust client for PGMQ. It provides an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.

**Extension Documentation**: https://pgmq.github.io/pgmq/

## Features

- Lightweight - No background worker or external dependencies, just Postgres SQL objects
- Guaranteed "exactly once" delivery of messages to a consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- [FIFO](https://github.com/pgmq/pgmq/blob/main/docs/fifo-queues.md#overview) (First-In-First-Out) queues with message group keys for ordered processing
- [Topic-based](https://github.com/pgmq/pgmq/blob/main/docs/topics.md#topic-based-routing) routing with wildcard patterns for publish-subscribe and content-based routing
- Messages stay in the queue until explicitly removed
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Asynchronous API

Supported on Postgres 14-18.

## Table of Contents

- [Postgres Message Queue (PGMQ)](#postgres-message-queue-pgmq)
  - [Features](#features)
  - [Installation](#installation)
    - [Docker](#docker)
    - [SQL Only](#sql-only)
    - [Installing PGMQ via the Rust Client](#installing-pgmq-via-the-rust-client)
      - [Create the DB](#create-the-db)
      - [Initialize applied migrations table](#initialize-applied-migrations-table)
      - [Install using the embedded scripts](#install-using-the-embedded-scripts)
      - [Install using the scripts fetched from GitHub](#install-using-the-scripts-fetched-from-github)
  - [Client Libraries](#client-libraries)
  - [Examples](#examples)
    - [Minimal example at a glance](#minimal-example-at-a-glance)
  - [Sending messages](#sending-messages)
  - [Reading messages](#reading-messages)
  - [Archive or Delete a message](#archive-or-delete-a-message)
  - [Serialization and Deserialization](#serialization-and-deserialization)
  - [License](#license)

## Installation

PGMQ can be run on any existing Postgres instance or installed as a Postgres Extension. See [INSTALLATION.md](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) for the full installation guide including a [comparison](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md#considerations) of the Postgres Extension vs the SQL-only installation.

### Docker

The fastest way to get started is by running the Docker image, where PGMQ comes pre-installed as an extension in Postgres.

```bash
docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=*** -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

Then connect and enable PGMQ:

```bash
psql postgres://postgres:***@localhost:5432/postgres
```

```sql
CREATE EXTENSION pgmq;
```

### SQL Only

> ⚠️ This installation approach is not versioned and only works for a fresh installation of `pgmq`.

You can also use [psql](https://www.tigerdata.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows) to install PGMQ's objects directly into the pgmq schema in Postgres. Use this method if you are running someplace that does not natively support the PGMQ Extension.

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq
psql -f pgmq-extension/sql/pgmq.sql postgres://postgres:***@localhost:5432/postgres
```

### Installing PGMQ via the Rust Client

PGMQ can be installed into any existing Postgres database using this Rust client. This is useful if the PGMQ extension
is not supported by your PostgreSQL instance. The installation performed by the Rust client is versioned, which means
it can be used to perform a fresh installation of PGMQ, or it can upgrade an existing installation to a newer version.

Two installation methods are supported. One method uses SQL scripts embedded in the Rust crate, while the other fetches
the SQL scripts from the PGMQ GitHub repo. The embedded approach does not require external network requests but only supports
installing (or upgrading to) the version bundled with the crate. The GitHub approach requires several network requests to GitHub,
but allows installing (or upgrading to) any version available in the repo.

#### Unversioned Installation

The following installation methods are unversioned and only work for a fresh installation of `pgmq`.

##### Via CLI

```bash
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url
pgmq-cli install -d postgres://postgres:***@localhost:5432/postgres install-from-embedded
```

##### In Rust

See also, the [install example](examples/install.rs)

Add PGMQ to your `Cargo.toml` with the `install-sql-embedded` feature enabled:

```bash
cargo add pgmq --features install-sql-embedded
```

```rust
async fn install_sql(pool: sqlx::Pool<sqlx::Postgres>) -> Result<(), pgmq::PgmqError> {
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    queue.install_sql_from_embedded().await?;
    Ok(())
}
```

#### Versioned Installation

The following installation methods are versioned and can be used to perform a fresh installation of PGMQ, or to upgrade an existing installation to a newer version.

##### Via CLI

```bash
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url and the version
pgmq-cli install -d postgres://postgres:***@localhost:5432/postgres install-from-github -v 1.9.0
```

##### In Rust

See also, the [install example](examples/install.rs)

Add PGMQ to your `Cargo.toml` with the `install-sql-github` feature enabled:

```bash
cargo add pgmq --features install-sql-github
```

```rust
async fn install_sql(pool: sqlx::Pool<sqlx::Postgres>) -> Result<(), pgmq::PgmqError> {
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    queue.install_sql_from_github(Some("1.9.0")).await?;
    Ok(())
}
```

## Client Libraries

- [Rust](https://github.com/pgmq/pgmq/tree/main/pgmq-rs) (this crate)
- [Python (only for psycopg3)](https://github.com/pgmq/pgmq-py)
- [TypeScript / Node.js](https://github.com/tembo-io/pgmq-ts)

Community

- [.NET](https://github.com/brianpursley/Npgmq)
- [C++](https://github.com/Ferdi265/pgmqpp)
- [C#](https://github.com/tmckenna-petro/Npgmq)
- [Dart](https://github.com/Ofceab-Studio/dart_pgmq)
- [Elixir + Broadway](https://github.com/v0idpwn/off_broadway_pgmq)
- [Elixir](https://github.com/v0idpwn/pgmq-elixir)
- [Go](https://github.com/craigpastro/pgmq-go)
- [Haskell](https://github.com/MichelBoucey/stakhanov)
- [Java (JDBC)](https://github.com/roy20021/pgmq-jdbc-client)
- [Java (Spring Boot)](https://github.com/adamalexandru4/pgmq-spring)
- [Lua](https://github.com/waynegemmell/pgmq-lua)
- [PHP](https://github.com/tembo-io/pgmq-php)
- [Ruby](https://github.com/tembo-io/pgmq-ruby)

## Examples

The project contains several [examples](./examples/). You can run these using Cargo.

A basic example displaying the primary features:

```bash
cargo run --example basic
```

How to install PGMQ using the Rust client from within your application:

```bash
cargo run --example install --features install-sql-github,install-sql-embedded
```

### Minimal example at a glance

See the [basic example](https://github.com/pgmq/pgmq/blob/main/pgmq-rs/examples/basic.rs) for a complete, working example.

## Sending messages

You can send one message at a time with `queue.send()` or several with `queue.send_batch()`.
These methods can be passed any type that implements `serde::Serialize`. This means you can prepare your messages as JSON or as a struct.

## Reading messages

Reading a message will make it invisible (unavailable for consumption) for the duration of the visibility timeout (vt).
No messages are returned when the queue is empty or all messages are invisible.

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue.

Note that when parsing into a `struct`, the operation will return an error if
the message can not be parsed as the type specified. For example, if the message expected is
`MyMessage{foo: "bar"}` but `{"hello": "world"}` is received, the operation will return an error.

Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.

## Archive or Delete a message

Remove the message from the queue when you are done with it. You can either `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.

Archive tables can be inspected directly with SQL. Archive tables have the prefix `a_` in the pgmq schema.

```sql
SELECT *
FROM a_{your_queue_name};
```

## Serialization and Deserialization

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue.

## License

[PostgreSQL](LICENSE)
