Initialize the DB table that tracks which SQL scripts have been run for the SQL-only installation method. This is
useful in order to switch from the previous SQL-only installation approach (in crate version <= 0.32.1) to the new
approach that tracks which scripts have been run. This method is not needed for fresh installations, or if the new
SQL-only installation method was used to install PGMQ.

To use, call this method with the version of PGMQ that was previously installed with the old SQL-only installation
method. Then it will be safe to invoke the new installation method, and any scripts that were previously run will
not be run again.
