use constants::MAX_PGMQ_QUEUE_LEN;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use thiserror::Error;

/// Private module to contain constants used to calculate [`MAX_PGMQ_QUEUE_LEN`]. Intermediary
/// constants are kept private to the module to ensure they are not referenced by accident.
///
/// Relevant docs: <https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS>
mod constants {
    /// Default value of `NAMEDATALEN`, set in `src/include/pg_config_manual.h`.
    const NAMEDATALEN: usize = 64;

    /// The maximum length of an identifier.
    /// Longer names can be used in commands, but they'll be truncated.
    const MAX_IDENTIFIER_LEN: usize = NAMEDATALEN - 1;

    const BIGGEST_CONCAT: &str = "archived_at_idx_";

    /// The max length of the name of a PGMQ queue, considering that the biggest postgres
    /// identifier created by PGMQ is the index on archived_at.
    pub(super) const MAX_PGMQ_QUEUE_LEN: usize = MAX_IDENTIFIER_LEN - BIGGEST_CONCAT.len();
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum QueueNameError {
    #[error("Queue name `{0}` length must be >0 and <={MAX_PGMQ_QUEUE_LEN}")]
    InvalidLength(String),

    #[error("Queue name `{0}` contains invalid character(s)")]
    InvalidCharacter(String),

    #[error("Queue name is invalid: {0}")]
    Other(String),
}

impl QueueNameError {
    /// Construct [`QueueNameError::Other`] from the given error/message.
    pub fn other(err: impl ToString) -> Self {
        Self::Other(err.to_string())
    }
}

/// A valid queue name.
#[derive(Debug, Clone, Copy)]
pub struct QueueName<'a>(&'a str);

impl<'a> QueueName<'a> {
    /// Create a new [`QueueName`], checking that the provided `queue_name` is valid.
    pub fn new(queue_name: &'a str) -> Result<Self, QueueNameError> {
        check_queue_name(queue_name)?;
        Ok(Self(queue_name))
    }
}

impl<'a> AsRef<&'a str> for QueueName<'a> {
    fn as_ref(&self) -> &&'a str {
        &self.0
    }
}

impl<'a> Deref for QueueName<'a> {
    type Target = &'a str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> TryFrom<&'a str> for QueueName<'a> {
    type Error = QueueNameError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        QueueName::new(value)
    }
}

impl<'a> TryFrom<&'a String> for QueueName<'a> {
    type Error = QueueNameError;

    fn try_from(value: &'a String) -> Result<Self, Self::Error> {
        QueueName::new(value)
    }
}

impl<'a> Display for QueueName<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Checks that the provided input is a valid queue name for usage with PGMQ.
pub fn check_queue_name(input: &str) -> Result<(), QueueNameError> {
    if input.is_empty() || input.len() > MAX_PGMQ_QUEUE_LEN {
        return Err(QueueNameError::InvalidLength(input.to_string()));
    }

    let has_valid_characters = input
        .as_bytes()
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || c == b'_');
    if !has_valid_characters {
        return Err(QueueNameError::InvalidCharacter(input.to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::constants::MAX_PGMQ_QUEUE_LEN;
    use crate::types::QueueName;
    use rstest::rstest;
    use std::assert_matches;

    #[rstest]
    #[case::single_character_alpha("a")]
    #[case::single_character_numeric("0")]
    #[case::single_character_underscore("_")]
    #[case::alpha("valid")]
    #[case::alphanumeric("valid2")]
    #[case::alphanumeric_underscore("valid_3")]
    #[case::max_length(std::iter::repeat_n("a", MAX_PGMQ_QUEUE_LEN).collect::<String>())]
    fn valid_queue_name(#[case] name: impl ToString) {
        let name = name.to_string();
        let queue_name = QueueName::new(&name);
        assert!(queue_name.is_ok());
    }

    #[rstest]
    #[case::empty("")]
    #[case::exceeds_maximum_length(std::iter::repeat_n("a", MAX_PGMQ_QUEUE_LEN + 1).collect::<String>())]
    fn queue_name_invalid_length(#[case] name: impl ToString) {
        let name = name.to_string();
        let queue_name = QueueName::new(&name);
        assert_matches!(queue_name, Err(super::QueueNameError::InvalidLength(_)));
    }

    #[rstest]
    #[case("-")]
    #[case(" ")]
    #[case("&")]
    #[case(".")]
    #[case(",")]
    #[case("/")]
    #[case(";")]
    #[case("#")]
    #[case("(")]
    #[case(")")]
    #[case("`")]
    #[case("~")]
    #[case("\"")]
    #[case("'")]
    #[case("\n")]
    fn queue_name_invalid_character(#[case] name: impl ToString) {
        let name = name.to_string();
        let queue_name = QueueName::new(&name);
        assert_matches!(queue_name, Err(super::QueueNameError::InvalidCharacter(_)));
    }
}
