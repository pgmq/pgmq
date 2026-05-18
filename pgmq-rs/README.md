# Postgres Message Queue (PGMQ)

[![Latest Version](https://img.shields.io/crates/v/pgmq.svg)](https://crates.io/crates/pgmq)

The Rust client for PGMQ. This gives you an ORM-like experience with the Postgres extension and makes managing connection pools, transactions, and serialization/deserialization much easier.


## Installing PGMQ

PGMQ can be installed into any existing Postgres database using this Rust client. This is useful if the PGMQ extension
is not supported by your PostgreSQL instance. The installation performed by the Rust client is versioned, which means
it can be used to perform a fresh installation of PGMQ, or it can upgrade an existing installation to a newer version.

Two installation methods are supported. One method uses SQL scripts embedded in the Rust crate, while the other fetches
the SQL scripts from the PGMQ GitHub repo. The embedded approach does not require external network requests but only supports
installing (or upgrading to) the version bundled with the crate. The GitHub approach requires several network requests to GitHub,
but allows installing (or upgrading to) any version available in the repo.

### Create the DB

Run standard Postgres using Docker:

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:latest
```

### Initialize applied migrations table

In crate versions <= 0.32.1, the crate did not track which SQL scripts had already been run, which makes upgrading to a
new version difficult. To switch from the old approach to the new approach, first perform the "initialize applied migrations table"
workflow.

This method is not needed for fresh installations, or if the new SQL-only installation method was used to install PGMQ.


#### Via the CLI

```shell
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url and the version
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres init-migrations-table -v 1.9.0
```

#### In Rust

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

### Install using the embedded scripts
#### Via CLI

```bash
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-embedded
```

#### In Rust

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

### Install using the scripts fetched from GitHub
#### Via CLI

```bash
# Install the PGMQ Rust CLI
cargo install pgmq --features cli --bin pgmq-cli
# Replace the DB url and the version
pgmq-cli install -d postgres://postgres:postgres@localhost:5432/postgres install-from-github -v 1.9.0
```

#### In Rust

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

## Serialization and Deserialization

Messages can be parsed as `serde_json::Value` or into a struct of your design. `queue.read()` returns an `Result<Option<Message<T>>, PgmqError>`
where `T` is the type of the message on the queue. It returns an error when there is an issue parsing the message (`PgmqError::JsonParsingError`) or if PGMQ is unable to reach postgres (`PgmqError::DatabaseError`).

License: [PostgreSQL](LICENSE)
