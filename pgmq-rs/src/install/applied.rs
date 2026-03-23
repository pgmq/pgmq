use crate::install::script::ParsedScriptName;
use crate::install::version::Version;
use crate::PgmqError;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{Acquire, FromRow, Postgres, Transaction};

/// Advisory lock key used to ensure only one transaction can run the `pgmq` installation process
/// at once. Select a random large negative `bigint` value to minimize the chances of conflicting
/// with another advisory lock used by the actual application.
const ADVISORY_LOCK_KEY: i64 = -9223372036854775808 + 4149;

/// Struct to represent a row of the DB table that tracks which migration scripts have been applied.
#[derive(FromRow)]
pub struct AppliedMigration {
    /// The name of the migration script.
    pub name: String,
    /// The version of `pgmq` after the migration script was applied. Mainly useful to record
    /// which version was installed by the `pgmq.sql` script for a fresh installation -- this
    /// script does not embed the `pgmq` version in its name, unlike the other migration scripts.
    pub version: Version,
}

impl AppliedMigration {
    /// Create the DB table used to keep track of which migration scripts have been applied.
    pub async fn create_table(tx: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
        /*
        Acquire an advisory lock to be sure that only one transaction can run the pgmq SQL
        installation/upgrade process at once. Without this, it's possible for multiple transactions
        to attempt to perform the `pgmq` SQL installation/upgrade process at the same time, and they
        may conflict when creating the `pgmq` schema and/or `pgmq.__pgmq_migrations` table. This is
        the case even with `IF NOT EXISTS` in the SQL query.
         */
        sqlx::query("SELECT pg_advisory_xact_lock($1);")
            .bind(ADVISORY_LOCK_KEY)
            .execute(tx.acquire().await?)
            .await?;

        /*
        The `pgmq` schema will not exist yet if we're currently performing a fresh installation
        of `pgmq`, so we first need to make sure the schema exists.
         */
        sqlx::query("CREATE SCHEMA IF NOT EXISTS pgmq;")
            .execute(tx.acquire().await?)
            .await?;

        sqlx::query(
        "CREATE TABLE IF NOT EXISTS pgmq.__pgmq_migrations ( name TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL, run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp());",
        )
        .execute(tx.acquire().await?)
        .await?;

        /*
        The advisory lock above is probably sufficient, but we also lock on the
        `pgmq.__pgmq_migrations` table to be sure that only one transaction can access the
        list of applied migrations at once.
         */
        sqlx::query("LOCK TABLE pgmq.__pgmq_migrations IN ACCESS EXCLUSIVE MODE;")
            .execute(tx.acquire().await?)
            .await?;

        Ok(())
    }

    /// Fetch all of the migrations that were previously applied.
    pub async fn fetch_all(
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<Vec<AppliedMigration>, PgmqError> {
        let applied_migrations = sqlx::query_as("SELECT name, version FROM pgmq.__pgmq_migrations")
            .fetch_all(tx.acquire().await?)
            .await?;
        Ok(applied_migrations)
    }

    /// Record that a script with the provided [`ParsedScriptName`] was applied.
    pub fn insert_script(
        name: &'_ ParsedScriptName,
    ) -> Result<Query<'_, Postgres, PgArguments>, PgmqError> {
        let query =
            sqlx::query("INSERT INTO pgmq.__pgmq_migrations ( name, version ) VALUES ( $1, $2 );")
                .bind(&name.original)
                .bind(&name.to);
        Ok(query)
    }
}
