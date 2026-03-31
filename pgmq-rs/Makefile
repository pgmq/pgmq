POSTGRES_PASSWORD:=postgres
DATABASE_URL:=postgres://postgres:postgres@0.0.0.0:5432/postgres
EXTENSION_DB:=postgres://postgres:postgres@0.0.0.0:5432/pgmq_ext_test

# Initialize a new installation of the repo (e.g., install deps)
init:
	cargo binstall cargo-insta cargo-semver-checks sqlx-cli cargo-readme cargo-minimal-versions cargo-hack

update.readme:
	cargo readme \
		--no-title \
		--no-indent-headings \
		> README.md

run.postgres:
	docker run --rm -d --name pgmq-pg -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} -p 5432:5432 ghcr.io/pgmq/pg17-pgmq:latest

test:
	sqlx migrate run --database-url ${DATABASE_URL}
	psql ${DATABASE_URL} -c "CREATE DATABASE pgmq_ext_test;"
	sqlx migrate run --database-url ${EXTENSION_DB}
	cargo test --lib --all-features
	cargo test

# Run the tests for `PGMQueueExt` with the embedded SQL-only installation approach. Assumes the DB is already running
# and `pg_partman` is an available extension in the DB.
test.pgmqext_sql:
	cargo test --features install-sql-embedded --test pg_ext_integration_test

setup.env:
	sqlx migrate run --database-url ${DATABASE_URL}

# Reformat all rust code
fmt:
	cargo fmt --all

# Check if there are any semver-incompatible changes. If there are, the crate's first non-zero semver value will need be
# bumped in order to release the changes.
# Note: This will catch most semver-incompatible changes, but not all.
check.semver:
	cargo semver-checks

# Check the crate does not have any Clippy errors
check.clippy:
	cargo clippy --all-features

# Check that the crate is formatted correctly
check.fmt:
	cargo fmt --all --check

check.docs:
	RUSTDOCFLAGS="-D rustdoc::all" cargo doc --all-features --lib --no-deps

# Check that our dependencies are declared as the correct minimal versions. For example, if we depend on a crate
# with version "1.0.0", ensure we don’t accidentally depend on a feature added in version "1.5.0".
check.minimal-versions:
	cargo minimal-versions check --direct --all-features
