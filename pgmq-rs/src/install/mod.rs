mod applied;
#[cfg(feature = "install-sql-embedded")]
mod embedded;
#[cfg(feature = "install-sql-github")]
mod github;
mod script;
mod version;

pub use version::Version;

use crate::errors::PgmqError;
use crate::install::applied::AppliedMigration;
use crate::install::script::{ParsedScriptName, ScriptFetcher};
use itertools::Itertools;
use sqlx::{Acquire, Pool, Postgres};

#[cfg(feature = "install-sql")]
#[doc = include_str!("init_migrations_table.md")]
pub async fn init_migrations_table(
    pool: &Pool<Postgres>,
    version: Version,
) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;
    AppliedMigration::create_table(&mut tx).await?;
    if !AppliedMigration::fetch_all(&mut tx).await?.is_empty() {
        // If the migration table already has items in it, it does not need to be initialized
        return Ok(());
    }
    AppliedMigration::insert_script(&ParsedScriptName::init_script(version))?
        .execute(tx.acquire().await?)
        .await?;

    tx.commit().await?;
    Ok(())
}

#[cfg(feature = "install-sql")]
#[doc = include_str!("installed_version.md")]
pub async fn installed_version(pool: &Pool<Postgres>) -> Result<Option<Version>, PgmqError> {
    let mut tx = pool.begin().await?;
    AppliedMigration::create_table(&mut tx).await?;
    let installed_version = AppliedMigration::fetch_all(&mut tx)
        .await?
        .into_iter()
        .map(|applied| applied.version)
        .max();
    tx.commit().await?;
    Ok(installed_version)
}

#[cfg(feature = "install-sql-github")]
#[doc = include_str!("./github/install_sql_github.md")]
pub async fn install_sql_from_github(
    pool: &Pool<Postgres>,
    version: Option<&str>,
) -> Result<(), PgmqError> {
    install_sql(pool, github::GitHubScriptFetcher::new(version).await?).await
}

#[cfg(feature = "install-sql-embedded")]
#[doc = include_str!("./embedded/install_sql_embedded.md")]
pub async fn install_sql_from_embedded(pool: &Pool<Postgres>) -> Result<(), PgmqError> {
    install_sql(pool, embedded::EmbeddedScriptFetcher).await
}

async fn install_sql(
    pool: &Pool<Postgres>,
    script_fetcher: impl ScriptFetcher,
) -> Result<(), PgmqError> {
    let mut tx = pool.begin().await?;

    AppliedMigration::create_table(&mut tx).await?;

    let applied_migrations = AppliedMigration::fetch_all(&mut tx).await?;
    let installed_version = applied_migrations
        .iter()
        .map(|applied| &applied.version)
        .max();

    let scripts = script_fetcher
        .fetch(installed_version)
        .await?
        .into_iter()
        // Filter out scripts that were already applied.
        .filter(|script| {
            !applied_migrations
                .iter()
                .any(|applied| applied.name == script.name.original)
        })
        .sorted();

    for script in scripts {
        script.run(&mut tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Helper method to reduce the boilerplate required to create a [`PgmqError::InstallationError`].
fn install_err(err: impl ToString) -> PgmqError {
    PgmqError::InstallationError(err.to_string())
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    #[test]
    fn install_err() {
        let err = super::install_err("Some error");
        assert_debug_snapshot!(err);
    }
}
