use clap::{Parser, Subcommand};
use env_logger::Env;
use pgmq::install::Version;
use pgmq::PgmqError;
use sqlx::PgPool;
use url::Url;

#[derive(Debug, Parser)]
#[clap(author, version, about = "PGMQ CLI tool for installing and managing PostgreSQL message queues", long_about = None)]
struct Arguments {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Install PGMQ into a PostgreSQL database using the SQL-only installation approach
    Install(InstallArgs),
}

#[derive(Debug, Parser)]
struct InstallArgs {
    /// PostgreSQL connection URL. If not provided, will attempt to read from the `DATABASE_URL`
    /// environment variable.
    #[clap(short = 'd')]
    database_url: Option<Url>,
    /// Install commands
    #[command(subcommand)]
    command: InstallCommands,
}

#[derive(Debug, Subcommand)]
enum InstallCommands {
    /// Initialize the DB table that tracks which SQL scripts have been run for the SQL-only
    /// installation method. This is useful in order to switch from the previous SQL-only
    /// installation approach (in crate version <= 0.32.1) to the new approach that tracks which
    /// scripts have been run. This is not needed for fresh installations, or if the new SQL-only
    /// installation method was used to install PGMQ.
    InitMigrationsTable(InitMigrationsTableArgs),
    /// Get the version of PGMQ that is currently installed. Only supports the versioned SQL-only
    /// installation methods available in crate versions > 0.32.1.
    InstalledVersion,
    /// Install PGMQ using SQL installation scripts fetched from the PGMQ GitHub repo.
    InstallFromGithub(InstallFromGithubArgs),
    /// Install PGMQ using SQL installation scripts embedded directly in the `pgmq` Rust crate.
    InstallFromEmbedded,
}

#[derive(Debug, Parser)]
struct InitMigrationsTableArgs {
    /// The PGMQ version that was previously installed. Should be a semver string with an optional
    /// `v` prefix, such as `1.2.3` or `v1.2.3`.
    #[clap(short = 'v')]
    version: Version,
}

#[derive(Debug, Parser)]
struct InstallFromGithubArgs {
    /// The PGMQ version to install. Should match a git ref available in the PGMQ GitHub repo, such
    /// as a release version tag with an optional `v` prefix, e.g. `1.2.3` or `v1.2.3`, a branch
    /// name, or a commit hash. If not provided, the latest PGMQ release will be installed.
    #[clap(short = 'v')]
    version: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), PgmqError> {
    let log_env = Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(log_env);
    let args = Arguments::parse();

    match args.command {
        Commands::Install(args) => {
            let db_url = args.database_url.unwrap_or_else(|| {
                Url::parse(
                    &std::env::var("DATABASE_URL")
                        .expect("Unable to read DATABASE_URL environment variable"),
                )
                .expect("Unable to parse DATABASE_URL environment variable")
            });
            let pool = PgPool::connect(db_url.as_str())
                .await
                .expect("Failed to connect to database");

            match args.command {
                InstallCommands::InitMigrationsTable(args) => {
                    pgmq::install::init_migrations_table(&pool, args.version).await?;
                }
                InstallCommands::InstalledVersion => {
                    let version = pgmq::install::installed_version(&pool).await?;
                    if let Some(version) = version {
                        log::info!("Installed version: {version}");
                    } else {
                        log::info!("PGMQ is not currently installed, or was not installed using a versioned SQL-only installation method.");
                    }
                }
                InstallCommands::InstallFromGithub(args) => {
                    pgmq::install::install_sql_from_github(&pool, args.version.as_deref()).await?;
                }
                InstallCommands::InstallFromEmbedded => {
                    pgmq::install::install_sql_from_embedded(&pool).await?;
                }
            }
        }
    }

    Ok(())
}
