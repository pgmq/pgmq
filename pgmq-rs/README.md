# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

A lightweight message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

The Rust client for PGMQ. This gives you an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.

**Documentation**: https://pgmq.github.io/pgmq/

**Source**: https://github.com/pgmq/pgmq

## Features

- Lightweight - No background worker or external dependencies, just Postgres SQL objects
- Guaranteed "exactly once" delivery of messages to a consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- [FIFO](https://github.com/pgmq/pgmq/blob/main/docs/fifo-queues.md#overview) (First-In-First-Out) queues with message group keys for ordered processing
- [Topic-based](https://github.com/pgmq/pgmq/blob/main/docs/topics.md#topic-based-routing) routing with wildcard patterns for publish-subscribe and content-based routing
- Messages stay in the queue until explicitly removed
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Completely asynchronous API

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
  - [Quick Start](#quick-start)
    - [Minimal example at a glance](#minimal-example-at-a-glance)
  - [Sending messages](#sending-messages)
  - [Reading messages](#reading-messages)
  - [Archive or Delete a message](#archive-or-delete-a-message)
  - [Serialization and Deserialization](#serialization-and-deserialization)
  - [License](#license)

## Installation

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

#### Create the DB

Run standard Postgres using Docker:

```bash
docker run -d -e POSTGRES_PASSWORD=*** -p 5432:5432 postgres:latest
```

#### Initialize applied migrations table

In crate versions <= 0.32.1, the crate did not track which SQL scripts had already been run, which makes upgrading to a
new version difficult. To switch from the old approach to the new approach, first perform the "initialize applied migrations table"
workflow.

This method is not needed for fresh installations, or if the new SQL-only installation method was used to install PGMQ.

##### Via the CLI

```shell
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url and the version
pgmq-cli install -d postgres://postgres:***@localhost:5432/postgres init-migrations-table -v 1.9.0
```

##### In Rust

Add PGMQ to your `Cargo.toml` with the `install-sql` feature enabled:

```bash
cargo add pgmq --features install-sql
```

```rust
async fn init_migrations_table(pool: sqlx::Pool<sqlx::Postgres>) -> Result<(), pgmq::PgmqError> {
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    // Replace the version
    queue.init_migrations_table("1.9.0").await?;
    Ok(())
}
```

#### Install using the embedded scripts

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

#### Install using the scripts fetched from GitHub

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

Community

- [.NET](https://github.com/brianpursley/Npgmq)
- [Dart](https://github.com/Ofceab-Studio/dart_pgmq)
- [Elixir + Broadway](https://github.com/v0idpwn/off_broadway_pgmq)
- [Elixir](https://github.com/v0idpwn/pgmq-elixir)
- [Go](https://github.com/craigpastro/pgmq-go)
- [Haskell](https://github.com/MichelBoucey/stakhanov)
- [Java (JDBC)](https://github.com/roy20021/pgmq-jdbc-client)
- [Java (Spring Boot)](https://github.com/adamalexandru4/pgmq-spring)

## Quick Start

The project contains several [examples](./examples/). You can run these using Cargo.

A basic example displaying the primary features:

```bash
cargo run --example basic
```

How to install PGMQ using the Rust client from within your application:

```bash
cargo run --example install --features install-sql-github,install-sql-embedded
```

First, you will need Postgres. We use a container in this example.

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=*** -p 5432:5432 postgres
```

If you don't have Docker installed, it can be found [here](https://docs.docker.com/get-docker/).

Make sure you have the Rust toolchain installed:

```bash
cargo --version
```

This example was written with version 1.67.0, but the latest stable should work. You can go [here](https://www.rust-lang.org/tools/install) to install Rust if you don't have it already, then run `rustup install stable` to install the latest, stable toolchain.

Change directory to the example project:

```bash
cd examples/basic
```

Run the project!

```bash
cargo run
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
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).

Note that when parsing into a `struct`, the operation will return an error if
parsed as the type specified. For example, if the message expected is
`MyMessage{foo: "bar"}` but `{"hello": "world"}` is received, the application will panic.

Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.

## Archive or Delete a message

Remove the message from the queue when you are done with it. You can either completely `.delete()`, or `.archive()` the message. Archived messages are deleted from the queue and inserted to the queue's archive table. Deleted messages are just deleted.

Read messages from the queue archive with SQL:

```sql
SELECT *
FROM pgmq_{your_queue_name}_archive;
```

## Serialization and Deserialization

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).

## License

[PostgreSQL](LICENSE)
