use clap::{Arg, Command};

use log;
use pgmq::util::install_pgmq;
use sqlx::PgPool;
use std::process;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let matches = Command::new("pgmq-cli")
        .about("PGMQ CLI tool for installing and managing PostgreSQL message queues")
        .subcommand(
            Command::new("install")
                .about("Install PGMQ into a PostgreSQL database")
                .arg(
                    Arg::new("database_url")
                        .help("PostgreSQL connection URL")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::new("version")
                        .long("version")
                        .short('v')
                        .help("PGMQ version to install (e.g., v1.5.1). Defaults to latest release")
                        .value_name("VERSION"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("install", sub_matches)) => {
            let database_url = sub_matches.get_one::<String>("database_url").unwrap();
            let version = sub_matches.get_one::<String>("version");

            let pool = PgPool::connect(database_url)
                .await
                .expect("Failed to connect to database");

            if let Err(e) = install_pgmq(&pool, version).await {
                log::error!("Error installing PGMQ: {}", e);
                process::exit(1);
            }
        }
        _ => {
            log::error!("No valid subcommand provided. Use --help for usage information.");
            process::exit(1);
        }
    }
}
