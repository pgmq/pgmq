use crate::install::applied::AppliedMigration;
use crate::install::install_err;
use crate::install::version::Version;
use crate::PgmqError;
use futures_util::StreamExt;
use include_dir::{include_dir, Dir};
use itertools::Itertools;
use regex::Regex;
use sqlx::{Acquire, Executor, Postgres, Transaction};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::OnceLock;

/// The name of the migration script used to perform a fresh installation of `pgmq`.
static INIT_SCRIPT_NAME: &str = "pgmq.sql";

/// All of the extension's migration scripts.
static MIGRATION_SCRIPTS: Dir<'static> = include_dir!("$OUT_DIR/sql/");

/// Regex to match a migration script name, e.g., `pgmq--1.2.3--1.3.4.sql`
static MIGRATION_SCRIPT_NAME_REGEX: OnceLock<Result<Regex, regex::Error>> = OnceLock::new();

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedScriptName {
    pub original: &'static str,
    pub from: Version,
    pub to: Version,
}

impl ParsedScriptName {
    /// Get all of the migration scripts from the given [`Dir`].
    fn all_in_directory(
        migration_script_dir: &Dir<'static>,
    ) -> Result<impl Iterator<Item = Self>, PgmqError> {
        let scripts = migration_script_dir
            .entries()
            .iter()
            .map(|entry| {
                let name = entry
                    .path()
                    .file_name()
                    .ok_or_else(|| {
                        install_err(format!(
                            "Unable to get filename for entry: {:?}",
                            entry.path()
                        ))
                    })?
                    .to_str()
                    .ok_or_else(|| {
                        install_err(format!(
                            "Unable to convert file name to str: {:?}",
                            entry.path()
                        ))
                    })?;
                Ok(name)
            })
            .collect::<Result<Vec<&'static str>, PgmqError>>()?
            .into_iter()
            .filter_map(|name| ParsedScriptName::from_static_str(name).ok())
            .sorted();

        Ok(scripts)
    }

    fn from_static_str(name: &'static str) -> Result<Self, PgmqError> {
        let captures = MIGRATION_SCRIPT_NAME_REGEX
            .get_or_init(|| Regex::new(r"^pgmq--(?<from>.*)--(?<to>.*)\.sql$"))
            .as_ref()
            .map_err(install_err)?
            .captures(name)
            .ok_or_else(|| install_err(format!("Invalid script name: '{}'", name)))?;
        Ok(Self {
            original: name,
            from: Version::from_str(&captures["from"])?,
            to: Version::from_str(&captures["to"])?,
        })
    }

    /// Create a [`ParsedScriptName`] that represents the initialization script (with name [`INIT_SCRIPT_NAME`]).
    /// Since this would be the first script run in a fresh installation, we use `0.0.0` as
    /// the `from` field.
    fn init_script(version: Version) -> Self {
        ParsedScriptName {
            original: INIT_SCRIPT_NAME,
            from: Version {
                major: 0,
                minor: 0,
                patch: 0,
            },
            to: version,
        }
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

/// Struct to contain metadata for a pgmq extension migration script along with its content.
#[derive(Debug, Eq)]
pub struct MigrationScript {
    pub name: ParsedScriptName,
    pub content: Cow<'static, str>,
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

impl MigrationScript {
    /// Fetch the given script from the embedded directory of migration scripts.
    fn new(migration_script_dir: &Dir<'static>, name: ParsedScriptName) -> Result<Self, PgmqError> {
        let script = Self {
            content: migration_script_dir
                .get_file(name.original)
                .ok_or_else(|| {
                    install_err(format!(
                        "Migration script file not found: {}",
                        name.original
                    ))
                })?
                .contents_utf8()
                .ok_or_else(|| {
                    install_err(format!("Unable to read file contents: {}", name.original))
                })?
                .into(),
            name,
        };
        Ok(script)
    }

    /// Get all SQL scripts required to install and/or upgrade the `pgmq` extension.
    pub async fn get_scripts(
        tx: &mut Transaction<'static, Postgres>,
    ) -> Result<Vec<MigrationScript>, PgmqError> {
        AppliedMigration::create_table(tx).await?;

        let applied_migrations = AppliedMigration::fetch_all(tx).await.map_err(install_err)?;

        let scripts = Self::get_scripts_internal(
            Version::get_pgmq_version()?,
            &MIGRATION_SCRIPTS,
            applied_migrations,
        )?;

        Ok(scripts)
    }

    fn get_scripts_internal(
        pgmq_version: Version,
        migration_script_dir: &Dir<'static>,
        applied_migrations: Vec<AppliedMigration>,
    ) -> Result<Vec<MigrationScript>, PgmqError> {
        // Get the version that is currently installed, or the current pgmq version that will be
        // installed in a fresh installation by running the `pgmq.sql` script. We will not run
        // migration scripts for versions lower than this.
        let current_version = applied_migrations
            .iter()
            .map(|migration| &migration.version)
            .max()
            .unwrap_or(&pgmq_version);

        // Get all migration scripts for versions after `current_version`
        let scripts: Vec<ParsedScriptName> =
            ParsedScriptName::all_in_directory(migration_script_dir)?
                .filter(|name| name.from >= *current_version)
                .collect();

        // The `pgmq.sql` initialization script follows a different naming pattern than the rest of
        // the migration scripts, so we manually insert it at the front of the iterator.
        let scripts = [ParsedScriptName::init_script(pgmq_version)]
            .into_iter()
            .chain(scripts)
            // Filter out scripts that were already applied.
            .filter(|script| {
                !applied_migrations
                    .iter()
                    .any(|applied| applied.name == script.original)
            })
            .sorted()
            .map(|name| MigrationScript::new(migration_script_dir, name))
            .collect::<Result<Vec<MigrationScript>, PgmqError>>()?;

        Ok(scripts)
    }

    /// Run this script and mark it as applied in the DB.
    pub async fn run(&self, tx: &mut Transaction<'static, Postgres>) -> Result<(), PgmqError> {
        {
            let mut stream = tx.fetch_many(self.content.as_ref());
            while let Some(step) = stream.next().await {
                let _ = step?;
            }
        }

        AppliedMigration::insert_script(self)?
            .execute(tx.acquire().await?)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use include_dir::{include_dir, Dir};

    static TEST_MIGRATION_SCRIPTS: Dir<'static> =
        include_dir!("$CARGO_MANIFEST_DIR/src/install/test_migrations/");

    mod parsed_script_name {
        use super::TEST_MIGRATION_SCRIPTS;
        use crate::install::script::{ParsedScriptName, MIGRATION_SCRIPTS};
        use crate::install::version::Version;
        use insta::assert_debug_snapshot;
        use std::str::FromStr;

        #[test]
        fn all_in_directory_sorted() {
            let scripts = ParsedScriptName::all_in_directory(&TEST_MIGRATION_SCRIPTS)
                .unwrap()
                .collect::<Vec<ParsedScriptName>>();

            assert!(scripts.is_sorted());
        }

        #[test]
        fn all_in_directory_actual_scripts_have_single_upgrade_path() {
            /*
            We currently assume that the migration scripts only contain a single upgrade path, e.g.:

            pgmq--1.1.0--1.1.1.sql
            pgmq--1.1.1--1.2.0.sql
            pgmq--1.2.0--1.2.1.sql

            If multiple upgrade paths are introduced, we will need to change our implementation
            to account for that. Example:

            pgmq--1.1.0--1.1.1.sql
            pgmq--1.1.1--1.2.0.sql
            pgmq--1.2.0--1.2.1.sql
            pgmq--1.1.0--1.2.1.sql <- This secondary upgrade path for 1.1.0 -> 1.2.1 is not supported.
            */
            let scripts = ParsedScriptName::all_in_directory(&MIGRATION_SCRIPTS)
                .unwrap()
                .collect::<Vec<ParsedScriptName>>();

            scripts
                .windows(2)
                .for_each(|window| assert_eq!(window[0].to, window[1].from));
        }

        #[test]
        fn from_static_str() {
            let name = ParsedScriptName::from_static_str("pgmq--1.2.3--1.3.0.sql").unwrap();
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_both_versions() {
            let name = ParsedScriptName::from_static_str("pgmq.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_wrong_extension() {
            let name = ParsedScriptName::from_static_str("pgmq--1.2.3--4.5.6.sqlx");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_first_version() {
            let name = ParsedScriptName::from_static_str("pgmq----1.2.3.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_missing_second_version() {
            let name = ParsedScriptName::from_static_str("pgmq--1.2.3--.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_wrong_number_of_dashes() {
            let name = ParsedScriptName::from_static_str("pgmq-1.2.3-4.5.6.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_first_version_invalid() {
            let name = ParsedScriptName::from_static_str("pgmq--a.b.c--1.2.3.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn from_static_str_err_second_version_invalid() {
            let name = ParsedScriptName::from_static_str("pgmq--1.2.3--a.b.c.sql");
            assert_debug_snapshot!(name);
        }

        #[test]
        fn init_script() {
            let name = ParsedScriptName::init_script(Version::from_str("1.2.3").unwrap());
            assert_debug_snapshot!(name)
        }
    }

    mod migrations_script {
        use super::TEST_MIGRATION_SCRIPTS;
        use crate::install::applied::AppliedMigration;
        use crate::install::script::{MigrationScript, ParsedScriptName, INIT_SCRIPT_NAME};
        use crate::install::version::Version;
        use insta::assert_debug_snapshot;

        const PGMQ_VERSION: Version = Version {
            major: 1,
            minor: 11,
            patch: 0,
        };

        #[test]
        fn new() {
            let script = MigrationScript::new(
                &TEST_MIGRATION_SCRIPTS,
                ParsedScriptName::from_static_str("pgmq--1.11.0--1.11.1.sql").unwrap(),
            )
            .unwrap();
            assert_debug_snapshot!(script);
        }

        #[test]
        fn new_script_not_found() {
            let script = MigrationScript::new(
                &TEST_MIGRATION_SCRIPTS,
                ParsedScriptName::from_static_str("pgmq--111.111.111--222.222.222.sql").unwrap(),
            );
            assert_debug_snapshot!(script);
        }

        #[test]
        fn get_scripts() {
            let scripts = MigrationScript::get_scripts_internal(
                PGMQ_VERSION,
                &TEST_MIGRATION_SCRIPTS,
                vec![],
            )
            .unwrap();
            assert!(scripts.is_sorted());
            assert_debug_snapshot!(scripts);
        }

        #[test]
        fn get_scripts_init_migration_already_applied() {
            let scripts = MigrationScript::get_scripts_internal(
                PGMQ_VERSION,
                &TEST_MIGRATION_SCRIPTS,
                vec![AppliedMigration {
                    name: INIT_SCRIPT_NAME.to_string(),
                    version: PGMQ_VERSION,
                }],
            )
            .unwrap();
            assert_debug_snapshot!(scripts);
        }
    }
}
