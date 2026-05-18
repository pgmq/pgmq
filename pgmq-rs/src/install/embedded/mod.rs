use crate::install::install_err;
use crate::install::script::{MigrationScript, ParsedScriptName, ScriptFetcher};
use crate::install::version::Version;
use crate::PgmqError;
use include_dir::{include_dir, Dir};
use itertools::Itertools;
use std::str::FromStr;

/// All of the extension's migration scripts. This will embed the scripts directly in the crate,
/// which allows installing `pgmq` without performing network requests to an external site such
/// as GitHub.
static MIGRATION_SCRIPTS: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/src/install/embedded/sql");

/// The `pgmq` extension control file. Used to determine which version of `pgmq` would be
/// installed by running the `pgmq.sql` script.
static EXTENSION_CONFIG: &str = include_str!("pgmq.control");

pub struct EmbeddedScriptFetcher;

impl ScriptFetcher for EmbeddedScriptFetcher {
    async fn fetch(
        &self,
        installed_version: Option<&Version>,
    ) -> Result<Vec<MigrationScript>, PgmqError> {
        get_migration_scripts_from_dir(
            get_version_from_embedded_extension_config()?,
            &MIGRATION_SCRIPTS,
            installed_version,
        )
    }
}

/// Get the current version of the `pgmq` installation scripts that are embedded in the crate.
fn get_version_from_embedded_extension_config() -> Result<Version, PgmqError> {
    Version::get_version_from_extension_config(EXTENSION_CONFIG)
}

fn get_migration_scripts_from_dir(
    pgmq_version: Version,
    migration_script_dir: &Dir<'static>,
    installed_version: Option<&Version>,
) -> Result<Vec<MigrationScript>, PgmqError> {
    // Get the version that is currently installed, or the current pgmq version that will be
    // installed in a fresh installation by running the `pgmq.sql` script. We will not run
    // migration scripts for versions lower than this.
    let current_version = installed_version.unwrap_or(&pgmq_version);

    // Get all migration scripts (except the `pgmq.sql` initialization script).
    let scripts = list_all_scripts_in_dir(migration_script_dir)?
        .filter(|name| name.from >= *current_version)
        .collect_vec();

    // The `pgmq.sql` initialization script follows a different naming pattern than the rest of
    // the migration scripts, so we manually insert it at the front of the iterator.
    let scripts = [ParsedScriptName::init_script(pgmq_version)]
        .into_iter()
        .chain(scripts)
        .map(|name| get_script_from_dir(migration_script_dir, name))
        .collect::<Result<Vec<MigrationScript>, PgmqError>>()?;

    Ok(scripts)
}

/// Get the list all of the migration scripts in the given [`Dir`].
fn list_all_scripts_in_dir(
    migration_script_dir: &Dir<'static>,
) -> Result<impl Iterator<Item = ParsedScriptName>, PgmqError> {
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
        .filter_map(|name| ParsedScriptName::from_str(name).ok());

    Ok(scripts)
}

/// Fetch the given script from the embedded directory of migration scripts.
fn get_script_from_dir(
    migration_script_dir: &Dir<'static>,
    name: ParsedScriptName,
) -> Result<MigrationScript, PgmqError> {
    let script = MigrationScript {
        content: migration_script_dir
            .get_file(&name.original)
            .ok_or_else(|| {
                install_err(format!(
                    "Migration script file not found: {}",
                    name.original
                ))
            })?
            .contents_utf8()
            .ok_or_else(|| install_err(format!("Unable to read file contents: {}", name.original)))?
            .into(),
        name,
    };
    Ok(script)
}

#[cfg(test)]
mod tests {
    use include_dir::{include_dir, Dir};

    #[test]
    fn get_pgmq_version_actual_config_file() {
        let version = super::get_version_from_embedded_extension_config();
        // Don't check for a specific version, just check that the version was successfully parsed.
        // Otherwise, this test will fail every time the version is updated.
        assert!(version.is_ok());
    }

    static TEST_MIGRATION_SCRIPTS: Dir<'static> =
        include_dir!("$CARGO_MANIFEST_DIR/src/install/embedded/test_migrations/");

    mod parsed_script_name {
        use crate::install::embedded::{list_all_scripts_in_dir, MIGRATION_SCRIPTS};
        use itertools::Itertools;

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
            let scripts = list_all_scripts_in_dir(&MIGRATION_SCRIPTS)
                .unwrap()
                .into_iter()
                .sorted()
                .collect_vec();

            scripts
                .windows(2)
                .for_each(|window| assert_eq!(window[0].to, window[1].from));
        }
    }

    mod migrations_script {
        use super::TEST_MIGRATION_SCRIPTS;
        use crate::install::embedded::{get_migration_scripts_from_dir, get_script_from_dir};
        use crate::install::script::ParsedScriptName;
        use crate::install::version::Version;
        use insta::assert_debug_snapshot;
        use itertools::Itertools;
        use std::str::FromStr;

        const PGMQ_VERSION: Version = Version {
            major: 1,
            minor: 11,
            patch: 0,
        };

        #[test]
        fn get_scripts_from_dir() {
            let script = get_script_from_dir(
                &TEST_MIGRATION_SCRIPTS,
                ParsedScriptName::from_str("pgmq--1.11.0--1.11.1.sql").unwrap(),
            )
            .unwrap();
            assert_debug_snapshot!(script);
        }

        #[test]
        fn get_scripts_from_dir_not_found() {
            let script = get_script_from_dir(
                &TEST_MIGRATION_SCRIPTS,
                ParsedScriptName::from_str("pgmq--111.111.111--222.222.222.sql").unwrap(),
            );
            assert_debug_snapshot!(script);
        }

        #[test]
        fn get_scripts() {
            let scripts = get_migration_scripts_from_dir(
                PGMQ_VERSION,
                &TEST_MIGRATION_SCRIPTS,
                Some(&PGMQ_VERSION),
            )
            .unwrap()
            .into_iter()
            .sorted()
            .collect_vec();

            assert!(scripts.is_sorted());
            assert_debug_snapshot!(scripts);
        }
    }
}
