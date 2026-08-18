# Postgres Message Queue (PGMQ)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13%20%7C%2014%20%7C%2015%20%7C%2016%20%7C%2017%20%7C%2018-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![PGXN version](https://badge.fury.io/pg/pgmq.svg)](https://pgxn.org/dist/pgmq/)
[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)
[![docs.rs](https://img.shields.io/docsrs/pgmq?logo=docsdotrs)](https://docs.rs/pgmq)
[![Crates.io MSRV](https://img.shields.io/crates/msrv/pgmq)](https://crates.io/crates/pgmq)

A lightweight message queue. Like [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq) but on Postgres.

## Features

- Lightweight - No background worker or external dependencies, just Postgres and Rust
- Guaranteed "exactly once" delivery of messages to a consumer within a visibility timeout
- API parity with [AWS SQS](https://aws.amazon.com/sqs/) and [RSMQ](https://github.com/smrchy/rsmq)
- [FIFO](docs/fifo-queues.md#overview) (First-In-First-Out) queues with message group keys for ordered processing
- [Topic-based](docs/topics.md#topic-based-routing) routing with wildcard patterns for publish-subscribe and content-based routing
- Messages stay in the queue until explicitly removed
- Messages can be archived, instead of deleted, for long-term retention and replayability
- Completely asynchronous API

Supported on Postgres 14-18.

Not building in Rust? Try the [PGMQ Postgres extension](https://pgt.dev/extensions/pgmq).

## Installing PGMQ

PGMQ can be installed on any existing Postgres instance or installed as a Postgres Extension.
See [INSTALLATION.md](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md) for the full
installation guide including a [comparison](https://github.com/pgmq/pgmq/blob/main/INSTALLATION.md#considerations)
of the Postgres Extension vs the SQL-only installation.

### Postgres Extension

The fastest way to get started with the extension is by running the Docker image, where PGMQ
comes pre-installed as an extension in Postgres.

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

### Versioned SQL-only installation

PGMQ can also be installed into any existing Postgres database using this Rust client. This is useful if the PGMQ extension
is not supported by your PostgreSQL instance. The installation performed by the Rust client is versioned, which means
it can be used to perform a fresh installation of PGMQ, or it can upgrade an existing installation to a newer version.

Two installation methods are supported. One method uses SQL scripts embedded in the Rust crate, while the other fetches
the SQL scripts from the PGMQ GitHub repo. The embedded approach does not require external network requests but only
supports installing (or upgrading to) the version bundled with the crate. The GitHub approach requires several network
requests to GitHub, but allows installing (or upgrading to) any version available in the repo.

#### Create the DB

Run standard Postgres using Docker:

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:latest
```

#### Initialize applied migrations table

In crate versions < 0.33.0, the crate did not track which SQL scripts had already been run, which makes upgrading to a
new version difficult. To switch from the old approach to the new approach, first perform the "initialize applied
migrations table" workflow.

This method is not needed for fresh installations, or if the new SQL-only installation method was used to install PGMQ.

##### Via the CLI

```shell
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url and the version
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres init-migrations-table -v 1.9.0
```

##### In Rust

Add PGMQ to your `Cargo.toml` with the `install-sql` feature enabled:

```bash
cargo add pgmq --features install-sql
```

```rust
// Requires the `install-sql` feature
#[cfg(feature = "install-sql")]
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
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-embedded
```

##### In Rust

See also, the [install example](examples/install.rs)

Add PGMQ to your `Cargo.toml` with the `install-sql-embedded` feature enabled:

```bash
cargo add pgmq --features install-sql-embedded
```

```rust
// Requires the `install-sql-embedded` feature
#[cfg(feature = "install-sql-embedded")]
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
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-github -v 1.9.0
```

##### In Rust

See also, the [install example](examples/install.rs)

Add PGMQ to your `Cargo.toml` with the `install-sql-github` feature enabled:

```bash
cargo add pgmq --features install-sql-github
```

```rust
// Requires the `install-sql-github` feature
#[cfg(feature = "install-sql-github")]
async fn install_sql(pool: sqlx::Pool<sqlx::Postgres>) -> Result<(), pgmq::PgmqError> {
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    queue.install_sql_from_github(Some("1.9.0")).await?;
    Ok(())
}
```

## Sending messages

You can send one message at a time with `queue.send()` or several with `queue.send_batch()`.
The message can be any type that implements `serde::Serialize`. This means you can prepare
your messages as JSON with `serde_json::json!(...)` or as a dedicated struct.

## Reading messages

Messages can be parsed as `serde_json::Value` or into a struct. `queue.read()` returns a
`Result<Option<Message<T>>, PGMQError>` where `T` is the type of the message on the queue.
It returns an error when there is an issue parsing the message ([`PgmqError::JsonParsingError`]
or if PGMQ is unable to reach postgres ([`PgmqError::DatabaseError`]).

Read a single message with `queue.read()` or as many as you want with `queue.read_batch()`.

## Visibility Timeout (vt)

PGMQ guarantees exactly once delivery of a message within a visibility timeout (`vt`). The
visibility timeout is the amount of time a message is invisible to other consumers after it has
been read by a consumer. If the message is NOT deleted or archived within the visibility timeout,
it will become visible again and can be read by another consumer. The visibility timeout is set
when a message is read from the queue, via `queue.read()`. It is recommended to set a `vt` value
that is greater than the expected time it takes to process a message. After the application
successfully processes the message, it should call `queue.delete()` to completely remove the
message from the queue or `queue.archive()` to move it to the archive table for the queue.

## Archive or Delete a message

Remove the message from the queue when you are done with it. You can either completely
delete the message using `queue.delete()`, or archive it using `queue.archive()`. Archived
messages are deleted from the queue and inserted to the queue's archive table. Deleted messages
are simply deleted.

The Rust client does not provide a method to retrieve message from the archive. If necessary,
they can be retrieved using plain SQL:

```sql
SELECT * FROM pgmq.a_{your_queue_name};
```

## Minimal example

Below is a minimal example of how to use the crate. More advanced examples can be found in
[examples](https://github.com/pgmq/pgmq/tree/main/pgmq-rs/examples) and
[tests](https://github.com/pgmq/pgmq/tree/main/pgmq-rs/tests).

```rust
use pgmq::{PgmqError, Message, PGMQueueExt};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), PgmqError> {

    // Initialize a connection to Postgres
    println!("Connecting to Postgres");
    let queue: PGMQueueExt = PGMQueueExt::new("postgres://postgres:postgres@0.0.0.0:5432".to_owned(), 1)
        .await
        .expect("Failed to connect to postgres");

    // Create a queue
    println!("Creating a queue 'my_queue'");
    let my_queue = "my_example_queue".to_string();
    queue.create(&my_queue).await?;

    // Structure a message
    #[derive(Debug, Serialize, Deserialize)]
    struct MyMessage {
        foo: String,
    }
    let message = MyMessage {
        foo: "bar".to_string(),
    };
    // Send the message
    let message_id: i64 = queue.send(&my_queue, &message).await?;

    // Use a visibility timeout of 30 seconds. Once read, the message will be unable to be read
    // until the visibility timeout expires.
    let visibility_timeout_seconds: i32 = 30;

    // Read a message
    let received_message: Message<MyMessage> = queue
        .read(&my_queue, visibility_timeout_seconds)
        .await?
        .expect("No messages available to read from the queue");
    println!("Received a message: {:?}", received_message);

    assert_eq!(received_message.msg_id, message_id);

    // archive the messages
    queue.archive(&my_queue, received_message.msg_id).await?;
    println!("archived the messages from the queue");

    Ok(())
}
```

License: PostgreSQL
