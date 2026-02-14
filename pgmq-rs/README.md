# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

The Rust client for PGMQ. This gives you an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.


## Installing PGMQ

PGMQ can be installed into any existing Postgres database using this Rust client.

Run standard Postgres using Docker:

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:latest
```

### Via CLI

Install the PGMQ Rust CLI:

```bash
cargo install pgmq --features cli --bin pgmq-cli

pgmq-cli install postgres://postgres:postgres@localhost:5432/postgres
```

### In Rust

Refer to the  [install example](examples/install.rs), or add PGMQ to your Cargo.toml with the cli feature enabled:

```bash
cargo add pgmq --features cli
```

```rust
use pgmq::PGMQueueExt;

let db_url = "postgres://postgres:postgres@localhost:5432/postgres".to_string();
let queue = pgmq::PGMQueueExt::new(db_url, 2)
    .await
    .expect("failed to connect to postgres");

queue.install_sql(Some(&"1.10.0".to_string())).await;
```

## Examples

The project contains several [examples](./examples/). You can run these using Cargo.

A basic example displaying the primary features:
```bash
cargo run --example basic
```

How to install PGMQ using the Rust client from within your application:

```bash
cargo run --example install --features cli
```

## Serialization and Deserialization

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).

License: [PostgreSQL](LICENSE)
