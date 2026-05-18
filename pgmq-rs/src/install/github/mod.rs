use crate::install::install_err;
use crate::install::script::{MigrationScript, ParsedScriptName, ScriptFetcher, INIT_SCRIPT_NAME};
use crate::install::version::Version;
use crate::PgmqError;
use itertools::Itertools;
use reqwest::Client;
use std::str::FromStr;
use url::Url;

pub struct GitHubScriptFetcher {
    github_version: String,
}

impl ScriptFetcher for GitHubScriptFetcher {
    async fn fetch(
        &self,
        installed_version: Option<&Version>,
    ) -> Result<Vec<MigrationScript>, PgmqError> {
        self.fetch_migration_scripts_from_github(installed_version)
            .await
    }
}

impl GitHubScriptFetcher {
    pub async fn new(version: Option<&str>) -> Result<Self, PgmqError> {
        let version = if let Some(version) = version {
            Version::from_str(version)
                .map(|v| format!("v{v}"))
                .unwrap_or(version.to_string())
        } else {
            get_latest_release_tag().await?
        };
        Ok(Self {
            github_version: version,
        })
    }

    async fn fetch_migration_scripts_from_github(
        &self,
        installed_version: Option<&Version>,
    ) -> Result<Vec<MigrationScript>, PgmqError> {
        let client = Client::new();
        let pgmq_version = self.get_version_from_extension_config(&client).await?;
        // Get the version that is currently installed, or the current pgmq version that will be
        // installed in a fresh installation by running the `pgmq.sql` script. We will not run
        // migration scripts for versions lower than this.
        let current_version = installed_version.unwrap_or(&pgmq_version);

        // Get all migration scripts (except the `pgmq.sql` initialization script).
        let scripts = self
            .list_all_scripts_in_dir(&client)
            .await?
            .filter(|(name, _download_url)| name.from >= *current_version)
            .collect_vec();

        // The `pgmq.sql` initialization script follows a different naming pattern than the rest of
        // the migration scripts, so we manually insert it at the front of the iterator.
        let init_script = (
            ParsedScriptName::init_script(pgmq_version),
            Url::parse(&format!(
                "https://raw.githubusercontent.com/pgmq/pgmq/{}/pgmq-extension/sql/{}",
                self.github_version, INIT_SCRIPT_NAME
            ))
            .map_err(install_err)?,
        );
        let scripts = [init_script].into_iter().chain(scripts);
        let mut result = Vec::new();
        for (name, download_url) in scripts {
            result.push(download_migration_script(&client, name, download_url).await?)
        }

        Ok(result)
    }

    async fn get_version_from_extension_config(
        &self,
        client: &Client,
    ) -> Result<Version, PgmqError> {
        let url = format!(
            "https://raw.githubusercontent.com/pgmq/pgmq/{}/pgmq-extension/pgmq.control",
            self.github_version
        );

        let response = client.get(url).send().await.map_err(install_err)?;
        if !response.status().is_success() {
            return Err(install_err(format!(
                "Failed to download SQL file: HTTP {}",
                response.status()
            )));
        }
        let sql_content = response.text().await.map_err(install_err)?;

        Version::get_version_from_extension_config(&sql_content)
    }

    /// Get the list all of the migration scripts in the GitHub source directory.
    async fn list_all_scripts_in_dir(
        &self,
        client: &Client,
    ) -> Result<impl Iterator<Item = (ParsedScriptName, Url)>, PgmqError> {
        let url = format!(
            "https://api.github.com/repos/pgmq/pgmq/contents/pgmq-extension/sql?ref={}",
            self.github_version
        );

        let response = client
            .get(url)
            .header("User-Agent", "pgmq-install")
            .header("Accept", "application/vnd.github.object+json")
            .header("X-GitHub-Api-Version", "2026-03-10")
            .send()
            .await
            .map_err(install_err)?;

        if !response.status().is_success() {
            return Err(install_err(format!(
                "Failed to list files in directory: HTTP {}",
                response.status()
            )));
        }

        let response: GitHubSourceEntry = response.json().await.map_err(install_err)?;
        let dir = match response {
            GitHubSourceEntry::Dir(dir) => dir,
            GitHubSourceEntry::File(_) => {
                return Err(install_err("Expected directory, received a file"))
            }
        };

        let scripts = dir
            .children
            .into_iter()
            .filter_map(|entry| match entry {
                GitHubSourceEntry::File(file) => Some(file),
                _ => None,
            })
            .filter_map(|entry| {
                let parsed_name = ParsedScriptName::from_str(&entry.entry.name);
                match parsed_name {
                    Ok(name) => Some((name, entry.download_url)),
                    Err(_) => None,
                }
            });

        Ok(scripts)
    }
}

async fn get_latest_release_tag() -> Result<String, PgmqError> {
    log::info!("Getting latest PGMQ release...");

    let client = reqwest::Client::new();
    let response = client
        .get("https://api.github.com/repos/pgmq/pgmq/releases/latest")
        .header("User-Agent", "pgmq-cli")
        .send()
        .await
        .map_err(install_err)?;

    if !response.status().is_success() {
        return Err(install_err(format!(
            "Failed to fetch latest release: HTTP {}",
            response.status()
        )));
    }

    let release: GitHubRelease = response.json().await.map_err(install_err)?;
    log::info!("Latest release: {}", release.tag_name);

    Ok(release.tag_name)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GitHubRelease {
    tag_name: String,
    name: String,
}

/// Fetch the given script from the given download URL.
async fn download_migration_script(
    client: &Client,
    name: ParsedScriptName,
    download_url: Url,
) -> Result<MigrationScript, PgmqError> {
    let response = client
        .get(download_url.as_str())
        .send()
        .await
        .map_err(install_err)?;

    if !response.status().is_success() {
        return Err(install_err(format!(
            "Failed to download file `{}`: HTTP {}",
            download_url,
            response.status()
        )));
    }

    let script = response.text().await.map_err(install_err)?;

    Ok(MigrationScript {
        name,
        content: script.into(),
    })
}

/// Enum to parse the response from the [GitHub source contents API](https://docs.github.com/en/rest/repos/contents?apiVersion=2026-03-10#get-repository-content).
#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GitHubSourceEntry {
    Dir(GitHubSourceDirEntry),
    File(GitHubSourceFileEntry),
}

#[derive(serde::Deserialize)]
struct GitHubSourceEntryCommon {
    name: String,
}

#[derive(serde::Deserialize)]
struct GitHubSourceDirEntry {
    #[serde(rename = "entries")]
    children: Vec<GitHubSourceEntry>,
}

#[derive(serde::Deserialize)]
struct GitHubSourceFileEntry {
    #[serde(flatten)]
    entry: GitHubSourceEntryCommon,
    download_url: Url,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn sql_script_count() {
        let count = std::fs::read_dir(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/install/embedded/sql"),
        )
        .unwrap()
        .count();

        assert!(count > 0);
        assert!(
            count <= 1000,
            r"
            The GitHub API used to list the extension's SQL migrations returns a maximum of 1000 items. If there are ever more than
            1000 migration files, then we will need to use the tree API instead: https://docs.github.com/en/rest/git/trees?apiVersion=2026-03-10#get-a-tree
            "
        )
    }
}
