Install PGMQ using the SQL-only approach. This method will perform a fresh installation if PGMQ is not installed, or
it will upgrade PGMQ to the latest version if it was previously installed and there's a newer version available.

If the previous SQL-only installation approach (in crate version <= 0.32.1) was used to install PGMQ,
run [`crate::PGMQueueExt::init_migrations_table`]/[`crate::PGMQueueExt::init_migrations_table_with_cxn`]/[
`crate::install::init_migrations_table`] before running this method.

This method fetches the PGMQ extension installation scripts from GitHub, which allows installing a specific version
of PGQM (or the latest version if no version is provided to the method). However, this approach performs several
network requests to fetch the scripts from GitHub. If this is not desirable, use
[`crate::PGMQueueExt::install_sql_from_embedded`]/[`crate::PGMQueueExt::install_sql_from_embedded_with_cxn`]/[
`crate::install::install_sql_from_embedded`] instead.

Note: This installation method should not be used if PGMQ was installed as an actual Postgres extension using
`CREATE EXTENSION pgmq;`.
