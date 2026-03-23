use crate::install::install_err;
use crate::PgmqError;
use regex::Regex;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::{Database, Decode, Encode, Type};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::OnceLock;

/// Regex to match a basic semver string, e.g. `1.2.3`.
static VERSION_REGEX: OnceLock<Result<Regex, regex::Error>> = OnceLock::new();

/// Struct to represent a basic semver version, e.g. `1.2.3`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Version {
    /// The first segment of the version string, e.g., for version `1.2.3`, this would be set to `1`
    pub major: u32,
    /// The second segment of the version string, e.g., for version `1.2.3`, this would be set to `2`
    pub minor: u32,
    /// The third segment of the version string, e.g., for version `1.2.3`, this would be set to `3`
    pub patch: u32,
}

impl<'r, DB: Database> Decode<'r, DB> for Version
where
    &'r str: Decode<'r, DB>,
{
    fn decode(value: <DB as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&str as Decode<DB>>::decode(value)?;
        let value = Self::from_str(value)?;
        Ok(value)
    }
}

impl<'q, DB: Database> Encode<'q, DB> for Version
where
    String: Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        let value = self.to_string();
        <String as sqlx::Encode<'_, DB>>::encode_by_ref(&value, buf)
    }
}

impl<DB: Database> Type<DB> for Version
where
    String: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as sqlx::Type<DB>>::type_info()
    }
}

impl Version {
    /// Get the `pgmq` version from the provided `pgmq.control` file contents.
    pub fn get_version_from_extension_config(extension_config: &str) -> Result<Self, PgmqError> {
        let version_line = extension_config
            .lines()
            .find(|line| line.trim_start().starts_with("default_version"))
            .ok_or_else(|| install_err("Version is not present in extension config"))?;
        let re =
            Regex::new(r"^\s*default_version\s*=\s*'(?<version>.*)'\s*$").map_err(install_err)?;
        let captures = re.captures(version_line).ok_or_else(|| {
            install_err(format!(
                "Unable to extract version from extension config: {}",
                version_line
            ))
        })?;
        Self::from_str(&captures["version"])
    }
}

impl FromStr for Version {
    type Err = PgmqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let captures = VERSION_REGEX
            .get_or_init(|| Regex::new(r"^v?(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)$"))
            .as_ref()
            .map_err(install_err)?
            .captures(s)
            .ok_or_else(|| install_err(format!("Invalid version: '{}'", s)))?;
        Ok(Self {
            major: u32::from_str(&captures["major"]).map_err(install_err)?,
            minor: u32::from_str(&captures["minor"]).map_err(install_err)?,
            patch: u32::from_str(&captures["patch"]).map_err(install_err)?,
        })
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.major.cmp(&other.major);
        match cmp {
            Ordering::Less | Ordering::Greater => {
                return cmp;
            }
            Ordering::Equal => {}
        }

        let cmp = self.minor.cmp(&other.minor);
        match cmp {
            Ordering::Less | Ordering::Greater => {
                return cmp;
            }
            Ordering::Equal => {}
        }

        self.patch.cmp(&other.patch)
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::Version;
    use insta::assert_debug_snapshot;
    use itertools::Itertools;
    use std::str::FromStr;

    #[test]
    fn get_pgmq_version() {
        let extension_config = "default_version = '1.11.0'";
        let pgmq_version = Version::get_version_from_extension_config(extension_config).unwrap();
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn get_pgmq_version_extra_whitespace() {
        let extension_config = "    default_version   =    '1.11.0'    ";
        let pgmq_version = Version::get_version_from_extension_config(extension_config).unwrap();
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn get_pgmq_version_err_invalid_version() {
        let extension_config = "default_version = 'a.b.c'";
        let pgmq_version = Version::get_version_from_extension_config(extension_config);
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn get_pgmq_version_err_version_not_present() {
        let extension_config = "";
        let pgmq_version = Version::get_version_from_extension_config(extension_config);
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn get_pgmq_version_err_missing_quotes() {
        let extension_config = "default_version = 1.11.0";
        let pgmq_version = Version::get_version_from_extension_config(extension_config);
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn get_pgmq_version_no_whitespace() {
        let extension_config = "default_version='1.11.0'";
        let pgmq_version = Version::get_version_from_extension_config(extension_config).unwrap();
        assert_debug_snapshot!(pgmq_version);
    }

    #[test]
    fn from_str() {
        let version = Version::from_str("1.11.0").unwrap();
        assert_eq!(
            version,
            Version {
                major: 1,
                minor: 11,
                patch: 0
            }
        );
    }

    #[test]
    fn from_str_v_prefix() {
        let version = Version::from_str("v1.11.0").unwrap();
        assert_eq!(
            version,
            Version {
                major: 1,
                minor: 11,
                patch: 0
            }
        );
    }

    #[test]
    fn from_str_err_not_enough_segments() {
        let version = Version::from_str("1.2");
        assert_debug_snapshot!(version);
    }

    #[test]
    fn from_str_err_invalid_segment_values() {
        let version = Version::from_str("a.b.c");
        assert_debug_snapshot!(version);
    }

    #[test]
    fn sort_and_unique() {
        let versions = [
            "0.1.0", "1.1.1", "2.0.1", "2.0.0", "2.0.0", "1.11.1", "1.0.1",
        ]
        .iter()
        .map(|version| Version::from_str(version).unwrap())
        .sorted()
        .unique()
        .collect::<Vec<Version>>();
        assert_debug_snapshot!(versions);
    }
}
