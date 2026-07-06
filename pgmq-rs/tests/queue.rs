//! Integration tests for the [`Queue`] trait and its implementations.
//!
//! Tests are written with the help of the custom [`pgmq_test_macro::queue_test`] macro -- the macro
//! generates individual tests for each type that implements the [`Queue`] trait.
//!
//! In order to prevent tests generating conflicting data in the DB, each test creates a temporary
//! test DB for itself. The temporary DBs are automatically removed if the tests pass, unless the
//! `PGMQ_KEEP_TEST_DB` env var is set to `true`.

#![cfg(feature = "queue-experimental")]

use initialization::ConnDetails;
use pgmq::queue::Queue;
use pgmq::Message;
use rand::RngExt;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;

static QUEUE: &str = "queue";

mod initialization {
    use chrono::Utc;
    use sqlx::AssertSqlSafe;
    use std::env;
    use std::sync::OnceLock;
    use url::Url;

    static KEEP_TEST_DB: OnceLock<bool> = OnceLock::new();
    static DB_URL: OnceLock<Url> = OnceLock::new();
    static TIMESTAMP: OnceLock<String> = OnceLock::new();

    const MAX_DB_NAME_LENGTH: usize = 63;

    fn test_db_name(test_name: &str) -> String {
        let timestamp = TIMESTAMP.get_or_init(|| Utc::now().timestamp().to_string());

        let db_name = format!("{test_name}/{timestamp}");
        if db_name.len() > MAX_DB_NAME_LENGTH {
            panic!("Test DB name `{db_name}` is too long! Max length is {MAX_DB_NAME_LENGTH}");
        }
        db_name
    }

    fn db_url() -> &'static Url {
        DB_URL.get_or_init(|| {
            let url = env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@localhost:5432/postgres".to_owned()
            });
            Url::parse(&url).unwrap()
        })
    }

    #[derive(Debug, Clone)]
    pub struct ConnDetails {
        pub original: &'static Url,
        pub test_db_name: String,
        pub test_db_url: Url,
    }

    impl ConnDetails {
        pub fn new() -> Self {
            let original = db_url();
            let test_name = std::thread::current().name().unwrap().to_string();
            let test_db_name = test_db_name(&test_name);
            let mut test_db_url = original.clone();
            test_db_url.set_path(&test_db_name);
            Self {
                original,
                test_db_name,
                test_db_url,
            }
        }

        async fn original_conn(&self) -> sqlx::postgres::PgConnection {
            sqlx_conn(self.original).await
        }
    }

    pub async fn before(conn_details: &ConnDetails) {
        let create_db_statement = format!("CREATE DATABASE \"{}\"", conn_details.test_db_name);
        sqlx::query(AssertSqlSafe(create_db_statement))
            .execute(&mut conn_details.original_conn().await)
            .await
            .unwrap();

        install_pgmq(conn_details).await;
    }

    async fn install_pgmq(conn_details: &ConnDetails) {
        // Todo: It's a little awkward to create an instance of `PGMQueueExt` just to init/install pgmq.
        //  In a future change, we could expand the `Queue` trait to include the init/install methods,
        //  then we could replace this method with a call to the client implementation.
        let queue = pgmq::PGMQueueExt::new(conn_details.test_db_url.to_string(), 1)
            .await
            .unwrap();

        #[cfg(feature = "install-sql-embedded")]
        let result = queue.install_sql_from_embedded().await.map(|_| true);
        #[cfg(not(feature = "install-sql"))]
        let result = queue.init().await;

        result.expect("failed to init pgmq");
    }

    pub async fn after(conn_details: &ConnDetails) {
        let keep_db = *KEEP_TEST_DB.get_or_init(|| {
            env::var("PGMQ_KEEP_TEST_DB")
                .ok()
                .and_then(|x| x.parse::<bool>().ok())
                .unwrap_or(false)
        });
        if keep_db {
            return;
        }
        let drop_db_statement = format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            conn_details.test_db_name
        );
        sqlx::query(AssertSqlSafe(drop_db_statement))
            .execute(&mut conn_details.original_conn().await)
            .await
            .unwrap();
    }

    pub async fn sqlx_conn(url: &Url) -> sqlx::postgres::PgConnection {
        use sqlx::ConnectOptions;
        sqlx::postgres::PgConnectOptions::from_url(url)
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    pub async fn pgmq_ext(url: &Url) -> pgmq::PGMQueueExt {
        pgmq::PGMQueueExt::new(url.to_string(), 2).await.unwrap()
    }

    #[cfg(feature = "rust-postgres")]
    pub fn rust_postgres(url: &Url) -> postgres::Client {
        postgres::Client::connect(url.as_str(), postgres::NoTls).unwrap()
    }

    #[cfg(feature = "tokio-postgres")]
    pub async fn tokio_postgres(
        url: &Url,
    ) -> (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ) {
        use std::str::FromStr;
        tokio_postgres::Config::from_str(url.as_str())
            .unwrap()
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap()
    }

    #[cfg(feature = "diesel-sync")]
    pub fn diesel_conn(url: &Url) -> diesel::PgConnection {
        use diesel::Connection;
        diesel::PgConnection::establish(url.as_str()).unwrap()
    }

    #[cfg(feature = "diesel-sync-pool")]
    pub fn diesel_pool(
        url: &Url,
    ) -> r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::PgConnection>> {
        let manager: diesel::r2d2::ConnectionManager<diesel::PgConnection> =
            diesel::r2d2::ConnectionManager::new(url.as_str());
        r2d2::Pool::builder().max_size(2).build(manager).unwrap()
    }

    #[cfg(feature = "diesel-async")]
    pub async fn diesel_async_conn(url: &Url) -> diesel_async::AsyncPgConnection {
        use diesel_async::AsyncConnection;
        diesel_async::AsyncPgConnection::establish(url.as_str())
            .await
            .unwrap()
    }

    #[cfg(feature = "diesel-async-pool")]
    pub async fn diesel_async_pool(
        url: &Url,
    ) -> diesel_async::pooled_connection::bb8::Pool<diesel_async::AsyncPgConnection> {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(url.as_str());

        diesel_async::pooled_connection::bb8::Pool::builder()
            .max_size(2)
            .build(manager)
            .await
            .unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestMessage {
    a: String,
    b: i32,
}

impl TestMessage {
    fn new() -> Self {
        Self {
            a: std::thread::current().name().unwrap().to_string(),
            b: rand::rng().random_range(0..i32::MAX),
        }
    }
}

#[pgmq_test_macro::queue_test]
async fn read(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg = TestMessage::new();
    let msg_id = queue.send(QUEUE, &msg, json!({}), 0).await.unwrap();

    // The first read should read the message
    let read_msg: Message<TestMessage> = queue
        .read(QUEUE, 10, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(msg_id, read_msg.msg_id);
    assert_eq!(read_msg.message, msg);

    // A second read should return no messages
    let read_msg: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 1).await.unwrap();
    assert!(read_msg.is_empty());
}

#[pgmq_test_macro::queue_test]
async fn read_multiple(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg_id1 = queue
        .send(QUEUE, TestMessage::new(), json!({}), 0)
        .await
        .unwrap();
    let msg_id2 = queue
        .send(QUEUE, TestMessage::new(), json!({}), 0)
        .await
        .unwrap();

    let messages: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 2).await.unwrap();
    assert_eq!(2, messages.len());
    assert!(messages.iter().any(|msg| msg.msg_id == msg_id1));
    assert!(messages.iter().any(|msg| msg.msg_id == msg_id2));
}
