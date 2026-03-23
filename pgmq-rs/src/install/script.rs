use crate::install::applied::AppliedMigration;
use crate::install::install_err;
use crate::install::version::Version;
use crate::PgmqError;
use futures_util::StreamExt;
use regex::Regex;
use sqlx::{Acquire, Executor, Postgres, Transaction};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::OnceLock;

/// The name of the migration script used to perform a fresh installation of `pgmq`.
pub static INIT_SCRIPT_NAME: &str = "pgmq.sql";

/// Regex to match a migration script name, e.g., `pgmq--1.2.3--1.3.4.sql`
static MIGRATION_SCRIPT_NAME_REGEX: OnceLock<Result<Regex, regex::Error>> = OnceLock::new();

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedScriptName {
    pub original: String,
    pub from: Version,
    pub to: Version,
}

impl ParsedScriptName {
    /// Create a [`ParsedScriptName`] that represents the initialization script (with name [`INIT_SCRIPT_NAME`]).
    /// Since this would be the first script run in a fresh installation, we use `0.0.0` as
    /// the `from` field.
    pub fn init_script(version: Version) -> Self {
        ParsedScriptName {
            original: INIT_SCRIPT_NAME.to_string(),
            from: Version {
                major: 0,
                minor: 0,
                patch: 0,
            },
            to: version,
        }
    }
}

impl FromStr for ParsedScriptName {
    type Err = PgmqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let captures = MIGRATION_SCRIPT_NAME_REGEX
            .get_or_init(|| Regex::new(r"^pgmq--(?<from>.*)--(?<to>.*)\.sql$"))
            .as_ref()
            .map_err(install_err)?
            .captures(s)
            .ok_or_else(|| install_err(format!("Invalid script name: '{}'", s)))?;
        Ok(Self {
            original: s.to_string(),
            from: Version::from_str(&captures["from"])?,
            to: Version::from_str(&captures["to"])?,
        })
    }
}

impl Ord for ParsedScriptName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.from.cmp(&other.from)
    }
}

impl PartialOrd for ParsedScriptName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait ScriptFetcher {
    async fn fetch(
        &self,
        installed_version: Option<&Version>,
    ) -> Result<Vec<MigrationScript>, PgmqError>;
}

/// Struct to contain metadata for a pgmq extension migration script along with its content.
#[derive(Debug, Eq)]
pub struct MigrationScript {
    pub name: ParsedScriptName,
    pub content: Cow<'static, str>,
}

impl MigrationScript {
    /// Run this script and mark it as applied in the DB.
    pub async fn run(&self, tx: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
        {
            let mut stream = tx.fetch_many(self.content.as_ref());
            while let Some(step) = stream.next().await {
                let _ = step?;
            }
        }

        AppliedMigration::insert_script(&self.name)?
            .execute(tx.acquire().await?)
            .await?;

        Ok(())
    }
}

impl PartialEq for MigrationScript {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Ord for MigrationScript {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for MigrationScript {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use include_dir::{include_dir, Dir};

    static TEST_MIGRATION_SCRIPTS: Dir<'static> =
        include_dir!("$CARGO_MANIFEST_DIR/src/install/embedded/test_migrations/");

    mod parsed_script_name {
        use crate::install::script::ParsedScriptName;
        use crate::install::version::Version;
        use insta::assert_debug_snapshot;
        use std::str::FromStr;

        #[test]
        fn from_static_str() {
            let name = ParsedScriptName::from_str("pgmq--1.2.3--1.3.0.sql").unwrap();
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_both_versions() {
            let name = ParsedScriptName::from_str("pgmq.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_wrong_extension() {
            let name = ParsedScriptName::from_str("pgmq--1.2.3--4.5.6.sqlx");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_first_version() {
            let name = ParsedScriptName::from_str("pgmq----1.2.3.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_second_version() {
            let name = ParsedScriptName::from_str("pgmq--1.2.3--.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_wrong_number_of_dashes() {
            let name = ParsedScriptName::from_str("pgmq-1.2.3-4.5.6.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_first_version_invalid() {
            let name = ParsedScriptName::from_str("pgmq--a.b.c--1.2.3.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_second_version_invalid() {
            let name = ParsedScriptName::from_str("pgmq--1.2.3--a.b.c.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn init_script() {
            let name = ParsedScriptName::init_script(Version::from_str("1.2.3").unwrap());
            assert_debug_snapshot!(name)
        }
    }
}
