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
use pgmq::queue::{Queue, QueueTransaction};
use pgmq::types::queue_name::QueueNameError;
use pgmq::types::EMPTY_HEADERS;
use pgmq::{Message, PgmqError};
use rand::RngExt;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use std::assert_matches;
use std::time::Duration;

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
        install_pg_partman(conn_details).await;
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

    async fn install_pg_partman(conn_details: &ConnDetails) {
        let mut conn = sqlx_conn(&conn_details.test_db_url).await;
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_partman")
            .execute(&mut conn)
            .await
            .expect("failed to create pg_partman extension");
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
async fn create_invalid_length(conn_details: ConnDetails, queue: impl Queue) {
    let queue_name = std::iter::repeat_n("a", pgmq::types::queue_name::MAX_PGMQ_QUEUE_LEN + 1)
        .collect::<String>();
    let result = queue.create(&queue_name).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidLength(_)))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_invalid_character(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue.create("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_duplicate(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    queue.create(QUEUE).await.unwrap();
}

#[pgmq_test_macro::queue_test]
async fn send_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue.send("invalid-queue-name", (), (), 0).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
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

#[pgmq_test_macro::queue_test]
async fn read_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> = queue.read("invalid-queue-name", 10, 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn pop(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg = TestMessage::new();
    let msg_id = queue.send(QUEUE, &msg, (), 0).await.unwrap();

    // The first pop should return the message
    let pop_msg: Message<TestMessage> = queue
        .pop(QUEUE, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(msg_id, pop_msg.msg_id);
    assert_eq!(pop_msg.message, msg);

    // A second pop should return no messages
    let pop_msg: Vec<Message<TestMessage>> = queue.pop(QUEUE, 1).await.unwrap();
    assert!(pop_msg.is_empty());
}

#[pgmq_test_macro::queue_test]
async fn pop_multiple(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let count = 2;
    let msgs = (0..count)
        .map(|_| TestMessage::new())
        .collect::<Vec<TestMessage>>();
    queue
        .send_batch(QUEUE, msgs, EMPTY_HEADERS, 0)
        .await
        .unwrap();

    let popped_msgs: Vec<Message<TestMessage>> = queue.pop(QUEUE, count).await.unwrap();
    assert_eq!(
        count as usize,
        popped_msgs.len(),
        "Pop should return multiple messages"
    );
}

#[pgmq_test_macro::queue_test]
async fn pop_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> = queue.pop("invalid-queue-name", 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn archive(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg_id1 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();
    let msg_id2 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();

    let archived = queue.archive(QUEUE, &[msg_id1, msg_id2]).await.unwrap();
    assert_eq!(archived, [msg_id1, msg_id2]);

    let archived = queue.archive(QUEUE, &[msg_id1]).await.unwrap();
    assert!(
        archived.is_empty(),
        "Attempting to archive a message that was already archived should return `false`"
    );

    let read_msg: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 1).await.unwrap();
    assert!(
        read_msg.is_empty(),
        "Attempting to read after archiving the message should return nothing"
    );
}

#[pgmq_test_macro::queue_test]
async fn archive_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue.archive("invalid-queue-name", &[]).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn delete(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg_id1 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();
    let msg_id2 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();

    let deleted = queue.delete(QUEUE, &[msg_id1, msg_id2]).await.unwrap();
    assert_eq!(deleted, [msg_id1, msg_id2]);

    let deleted = queue.delete(QUEUE, &[msg_id1]).await.unwrap();
    assert!(
        deleted.is_empty(),
        "Attempting to delete a message that was already deleted should return `false`"
    );

    let read_msg: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 1).await.unwrap();
    assert!(
        read_msg.is_empty(),
        "Attempting to read after deleting the message should return nothing"
    );
}

#[pgmq_test_macro::queue_test]
async fn delete_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue.delete("invalid-queue-name", &[]).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn set_vt(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let msg_id1 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();
    let msg_id2 = queue.send(QUEUE, TestMessage::new(), (), 0).await.unwrap();

    let duration = 5;

    let vt_updated: Vec<Message<TestMessage>> = queue
        .set_vt(QUEUE, &[msg_id1, msg_id2], duration)
        .await
        .unwrap();
    let vt_updated = vt_updated
        .into_iter()
        .map(|msg| msg.msg_id)
        .collect::<Vec<i64>>();
    assert_eq!(vt_updated, [msg_id1, msg_id2]);

    let read_msgs: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 2).await.unwrap();
    assert!(
        read_msgs.is_empty(),
        "Attempting to read messages with updated vt should return nothing"
    );

    tokio::time::sleep(Duration::from_secs((duration + 1) as u64)).await;

    let read_msgs: Vec<Message<TestMessage>> = queue.read(QUEUE, 10, 2).await.unwrap();
    assert_eq!(
        read_msgs.len(),
        2,
        "Attempting to read messages with updated vt after sleeping should return the messages"
    );
}

#[pgmq_test_macro::queue_test]
async fn set_vt_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> = queue.set_vt("invalid-queue-name", &[], 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn send_batch(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let count = 5;
    let msgs = (0..count)
        .map(|_| TestMessage::new())
        .collect::<Vec<TestMessage>>();

    let msg_ids = queue
        .send_batch(QUEUE, msgs, EMPTY_HEADERS, 0)
        .await
        .unwrap();

    assert_eq!(
        count,
        msg_ids.len(),
        "send_batch should return an ID for every sent message"
    );

    let read_msgs: Vec<Message<TestMessage>> =
        queue.read(QUEUE, 10, (count + 1) as i32).await.unwrap();
    assert_eq!(
        count,
        read_msgs.len(),
        "Read should return messages sent with send_batch"
    );
}

#[pgmq_test_macro::queue_test]
async fn send_batch_with_headers(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let count = 5;
    let msgs = (0..count)
        .map(|_| TestMessage::new())
        .collect::<Vec<TestMessage>>();
    let header_for_message = |msg: &TestMessage| json!({"a": msg.a});
    let headers = msgs.iter().map(header_for_message);

    let msg_ids = queue
        .send_batch(QUEUE, &msgs, Some(headers), 0)
        .await
        .unwrap();

    assert_eq!(
        count,
        msg_ids.len(),
        "send_batch should return an ID for every sent message"
    );

    let read_msgs: Vec<Message<TestMessage, serde_json::Value>> =
        queue.read(QUEUE, 10, (count + 1) as i32).await.unwrap();
    read_msgs.iter().for_each(|msg| {
        assert_eq!(
            msg.headers,
            Some(header_for_message(&msg.message)),
            "Read should properly deserialize headers sent with send_batch"
        )
    })
}

#[pgmq_test_macro::queue_test]
async fn send_batch_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue
        .send_batch("invalid-queue-name", [()], EMPTY_HEADERS, 0)
        .await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_fifo_index(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    queue.create_fifo_index(QUEUE).await.unwrap();
}

#[pgmq_test_macro::queue_test]
async fn create_fifo_index_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result = queue.create_fifo_index("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_fifo_indexes_all(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    queue.create_fifo_indexes_all().await.unwrap();
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> = queue.read_grouped("invalid-queue-name", 10, 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_default_group(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let msg1 = TestMessage::new();
    let id1 = queue.send(QUEUE, &msg1, EMPTY_HEADERS, 0).await.unwrap();
    let msg2 = TestMessage::new();
    let _ = queue.send(QUEUE, &msg2, EMPTY_HEADERS, 0).await.unwrap();

    {
        let read_msg1: Option<Message<TestMessage>> = queue
            .read_grouped(QUEUE, 100, 1)
            .await
            .unwrap()
            .into_iter()
            .next();
        let read_msg2: Option<Message<TestMessage>> = queue
            .read_grouped(QUEUE, 100, 1)
            .await
            .unwrap()
            .into_iter()
            .next();
        assert!(read_msg1.is_some());
        assert!(read_msg2.is_none(), "The second message should not become available until the first message has been processed");
    }

    {
        queue.archive(QUEUE, &[id1]).await.unwrap();
        let read_msg2: Option<Message<TestMessage>> = queue
            .read_grouped(QUEUE, 100, 1)
            .await
            .unwrap()
            .into_iter()
            .next();
        assert!(read_msg2.is_some());
    }
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_default_group_many(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let msg1 = TestMessage::new();
    queue.send(QUEUE, &msg1, EMPTY_HEADERS, 0).await.unwrap();
    let msg2 = TestMessage::new();
    queue.send(QUEUE, &msg2, EMPTY_HEADERS, 0).await.unwrap();

    let read_msgs: Vec<Message<TestMessage>> = queue.read_grouped(QUEUE, 100, 2).await.unwrap();
    assert_eq!(2, read_msgs.len());
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_custom_group(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let msg1 = TestMessage {
        a: "a".to_string(),
        b: 1,
    };
    let headers1 = json!({
        "x-pgmq-group": msg1.b
    });
    let msg2 = TestMessage {
        a: "b".to_string(),
        b: 2,
    };
    let headers2 = json!({
        "x-pgmq-group": msg2.b
    });
    queue
        .send_batch(QUEUE, &[msg1, msg2], Some(&[headers1, headers2]), 0)
        .await
        .unwrap();

    let read_msg1: Option<Message<TestMessage>> = queue
        .read_grouped(QUEUE, 100, 1)
        .await
        .unwrap()
        .into_iter()
        .next();
    let read_msg2: Option<Message<TestMessage>> = queue
        .read_grouped(QUEUE, 100, 1)
        .await
        .unwrap()
        .into_iter()
        .next();
    assert!(read_msg1.is_some());
    assert!(read_msg2.is_some());
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_head_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> =
        queue.read_grouped_head("invalid-queue-name", 10, 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_head_diff_groups(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let msg1 = TestMessage {
        a: "a".to_string(),
        b: 1,
    };
    let headers1 = json!({
        "x-pgmq-group": msg1.b
    });
    let msg2 = TestMessage {
        a: "b".to_string(),
        b: 1,
    };
    let headers2 = json!({
        "x-pgmq-group": msg2.b
    });
    let msg3 = TestMessage {
        a: "c".to_string(),
        b: 2,
    };
    let headers3 = json!({
        "x-pgmq-group": msg3.b
    });
    let msg4 = TestMessage {
        a: "d".to_string(),
        b: 2,
    };
    let headers4 = json!({
        "x-pgmq-group": msg4.b
    });
    queue
        .send_batch(
            QUEUE,
            &[msg1, msg2, msg3, msg4],
            Some(&[headers1, headers2, headers3, headers4]),
            0,
        )
        .await
        .unwrap();

    let read_msgs: Vec<Message<TestMessage>> =
        queue.read_grouped_head(QUEUE, 100, 2).await.unwrap();
    assert_eq!(2, read_msgs.len());
    assert_ne!(
        read_msgs.first().unwrap().message.b,
        read_msgs.get(1).unwrap().message.b
    );

    let read_msgs2: Vec<Message<TestMessage>> =
        queue.read_grouped_head(QUEUE, 100, 2).await.unwrap();
    assert!(read_msgs2.is_empty(), "The second message in each group should not become available until the first message has been processed");

    queue
        .archive(
            QUEUE,
            &read_msgs.iter().map(|msg| msg.msg_id).collect::<Vec<_>>(),
        )
        .await
        .unwrap();

    let read_msgs: Vec<Message<TestMessage>> =
        queue.read_grouped_head(QUEUE, 100, 2).await.unwrap();
    assert_eq!(2, read_msgs.len());
    assert_ne!(
        read_msgs.first().unwrap().message.b,
        read_msgs.get(1).unwrap().message.b
    );
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_rr_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<Vec<Message>, _> = queue.read_grouped_rr("invalid-queue-name", 10, 1).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn read_grouped_rr_diff_groups(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let msg1 = TestMessage {
        a: "a".to_string(),
        b: 1,
    };
    let headers1 = json!({
        "x-pgmq-group": msg1.b
    });
    let msg2 = TestMessage {
        a: "b".to_string(),
        b: 1,
    };
    let headers2 = json!({
        "x-pgmq-group": msg2.b
    });
    let msg3 = TestMessage {
        a: "c".to_string(),
        b: 2,
    };
    let headers3 = json!({
        "x-pgmq-group": msg3.b
    });
    let msg4 = TestMessage {
        a: "d".to_string(),
        b: 2,
    };
    let headers4 = json!({
        "x-pgmq-group": msg4.b
    });
    queue
        .send_batch(
            QUEUE,
            &[msg1, msg2, msg3, msg4],
            Some(&[headers1, headers2, headers3, headers4]),
            0,
        )
        .await
        .unwrap();

    let read_msgs: Vec<Message<TestMessage>> = queue.read_grouped_rr(QUEUE, 100, 2).await.unwrap();
    assert_eq!(2, read_msgs.len());
    assert_ne!(
        read_msgs.first().unwrap().message.b,
        read_msgs.get(1).unwrap().message.b
    );

    let read_msgs2: Vec<Message<TestMessage>> = queue.read_grouped_rr(QUEUE, 100, 2).await.unwrap();
    assert!(read_msgs2.is_empty(), "The second message in each group should not become available until the first message has been processed");

    queue
        .archive(
            QUEUE,
            &read_msgs.iter().map(|msg| msg.msg_id).collect::<Vec<_>>(),
        )
        .await
        .unwrap();

    let read_msgs: Vec<Message<TestMessage>> = queue.read_grouped_rr(QUEUE, 100, 2).await.unwrap();
    assert_eq!(2, read_msgs.len());
    assert_ne!(
        read_msgs.first().unwrap().message.b,
        read_msgs.get(1).unwrap().message.b
    );
}

#[pgmq_test_macro::queue_test]
async fn bind_topic_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.bind_topic("pattern", "invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn unbind_topic_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.unbind_topic("pattern", "invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn list_topic_bindings_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.list_topic_bindings("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn bind_and_list_topics(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let pattern = format!("{QUEUE}.*");

    queue.bind_topic(&pattern, QUEUE).await.unwrap();
    let topic_binding = queue
        .list_topic_bindings(QUEUE)
        .await
        .unwrap()
        .into_iter()
        .next();
    assert!(topic_binding.is_some());
    let topic_binding = topic_binding.unwrap();
    assert_eq!(topic_binding.queue_name, QUEUE);
    assert_eq!(topic_binding.pattern, pattern);
    assert_eq!(topic_binding.compiled_regex, format!("^{QUEUE}\\.[^.]+$"));
}

#[pgmq_test_macro::queue_test]
async fn bind_and_list_topics_all(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let pattern = format!("{QUEUE}.*");

    queue.bind_topic(&pattern, QUEUE).await.unwrap();
    let topic_bindings = queue.list_topic_bindings_all().await.unwrap();
    let topic_binding = topic_bindings
        .into_iter()
        .find(|binding| binding.queue_name == QUEUE)
        .unwrap();
    assert_eq!(topic_binding.queue_name, QUEUE);
    assert_eq!(topic_binding.pattern, pattern);
    assert_eq!(topic_binding.compiled_regex, format!("^{QUEUE}\\.[^.]+$"));
}

#[pgmq_test_macro::queue_test]
async fn unbind_topic(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let pattern = format!("{QUEUE}.*");

    queue.bind_topic(&pattern, QUEUE).await.unwrap();
    queue.unbind_topic(&pattern, QUEUE).await.unwrap();
    let topic_binding = queue
        .list_topic_bindings(QUEUE)
        .await
        .unwrap()
        .into_iter()
        .next();
    assert!(topic_binding.is_none());
}

#[pgmq_test_macro::queue_test]
async fn send_topic(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let pattern = format!("{QUEUE}.*");

    queue.bind_topic(&pattern, QUEUE).await.unwrap();

    let msg = TestMessage::new();
    let matched_queues = queue
        .send_topic(&format!("{QUEUE}.foo"), &msg, Option::<&()>::None, 0)
        .await
        .unwrap();
    assert_eq!(1, matched_queues);

    let read_msg: Vec<Message<TestMessage>> = queue.read(QUEUE, 100, 1).await.unwrap();
    assert!(!read_msg.is_empty());
}

#[pgmq_test_macro::queue_test]
async fn send_batch_topic(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let pattern = format!("{QUEUE}.*");

    queue.bind_topic(&pattern, QUEUE).await.unwrap();

    let msgs = [TestMessage::new(), TestMessage::new()];
    let send_batch_rows = queue
        .send_batch_topic(&format!("{QUEUE}.foo"), &msgs, EMPTY_HEADERS, 0)
        .await
        .unwrap();
    assert_eq!(msgs.len(), send_batch_rows.len());

    let read_msgs: Vec<Message<TestMessage>> = queue.read(QUEUE, 100, 2).await.unwrap();
    assert_eq!(msgs.len(), read_msgs.len());
}

#[pgmq_test_macro::queue_test]
async fn enable_notify_insert_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.enable_notify_insert("invalid-queue-name", 1000).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn enable_notify_insert(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    queue
        .enable_notify_insert(QUEUE, Duration::from_secs(1))
        .await
        .unwrap();

    let notify_insert_throttle = queue
        .list_notify_insert_throttles()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.queue_name == QUEUE)
        .unwrap();

    assert_eq!(1000, notify_insert_throttle.throttle_interval_ms);
}

#[pgmq_test_macro::queue_test]
async fn update_notify_insert_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.update_notify_insert("invalid-queue-name", 1000).await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn update_notify_insert(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    queue.enable_notify_insert(QUEUE, 1000).await.unwrap();

    queue.update_notify_insert(QUEUE, 2000).await.unwrap();

    let notify_insert_throttle = queue
        .list_notify_insert_throttles()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.queue_name == QUEUE)
        .unwrap();

    assert_eq!(2000, notify_insert_throttle.throttle_interval_ms);
}

#[pgmq_test_macro::queue_test]
async fn disable_notify_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.disable_notify_insert("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn disable_notify_insert(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    queue
        .enable_notify_insert(QUEUE, Duration::from_secs(1))
        .await
        .unwrap();

    queue.disable_notify_insert(QUEUE).await.unwrap();

    let notify_insert_throttle = queue
        .list_notify_insert_throttles()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.queue_name == QUEUE);

    assert!(notify_insert_throttle.is_none());
}

#[pgmq_test_macro::queue_test]
async fn create_unlogged_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.create_unlogged("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_unlogged(conn_details: ConnDetails, queue: impl Queue) {
    queue.create_unlogged(QUEUE).await.unwrap();

    let queues = queue.list_queues().await.unwrap();
    let unlogged_queue = queues
        .into_iter()
        .find(|queue| queue.queue_name == QUEUE)
        .unwrap();
    assert!(unlogged_queue.is_unlogged);

    let msg_id = queue
        .send(QUEUE, TestMessage::new(), json!({}), 0)
        .await
        .unwrap();
    let read_msg: Message<TestMessage> = queue
        .read(QUEUE, 10, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(msg_id, read_msg.msg_id);
}

#[pgmq_test_macro::queue_test]
async fn create_unlogged_duplicate(conn_details: ConnDetails, queue: impl Queue) {
    queue.create_unlogged(QUEUE).await.unwrap();
    queue.create_unlogged(QUEUE).await.unwrap();
}

#[pgmq_test_macro::queue_test]
async fn create_partitioned_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue
        .create_partitioned("invalid-queue-name", "1000", "10000")
        .await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn create_partitioned(conn_details: ConnDetails, queue: impl Queue) {
    queue
        .create_partitioned(QUEUE, "1000", "10000")
        .await
        .unwrap();

    let queues = queue.list_queues().await.unwrap();
    let partitioned_queue = queues
        .into_iter()
        .find(|queue| queue.queue_name == QUEUE)
        .unwrap();
    assert!(partitioned_queue.is_partitioned);

    let msg_id = queue
        .send(QUEUE, TestMessage::new(), json!({}), 0)
        .await
        .unwrap();
    let read_msg: Message<TestMessage> = queue
        .read(QUEUE, 10, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(msg_id, read_msg.msg_id);
}

#[pgmq_test_macro::queue_test]
async fn create_partitioned_duplicate(conn_details: ConnDetails, queue: impl Queue) {
    queue
        .create_partitioned(QUEUE, "1000", "10000")
        .await
        .unwrap();
    let result = queue.create_partitioned(QUEUE, "1000", "10000").await;
    /*
    At the time of writing this test, `pgmq.create_partitioned` can not be called twice for the
    same queue. If this test fails, then that means the issue has been fixed. In that case, we can
    update this test to simply `unwrap` the second `create_partitioned` call, and can also update
    `PGMQueueExt#create_partitioned` to not check if the queue already exists.
    */
    assert!(result.is_err());
}

#[pgmq_test_macro::queue_test]
async fn convert_arch_part_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue
        .convert_archive_partitioned("invalid-queue-name", "1000", "10000")
        .await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn convert_archive_partitioned(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let messages = [TestMessage::new(), TestMessage::new(), TestMessage::new()];
    let msg_ids = queue
        .send_batch(QUEUE, &messages, EMPTY_HEADERS, 0)
        .await
        .unwrap();
    queue.archive(QUEUE, &msg_ids).await.unwrap();

    queue
        .convert_archive_partitioned(QUEUE, "1000", "10000")
        .await
        .unwrap();
}

#[pgmq_test_macro::queue_test]
async fn list_queues_none(conn_details: ConnDetails, queue: impl Queue) {
    let queues = queue.list_queues().await.unwrap();
    assert!(queues.is_empty());
}

#[pgmq_test_macro::queue_test]
async fn list_queues(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();

    let unlogged_name = format!("{QUEUE}_unlogged");
    queue.create_unlogged(&unlogged_name).await.unwrap();

    let partitioned_name = format!("{QUEUE}_partitioned");
    queue
        .create_partitioned(&partitioned_name, "1000", "10000")
        .await
        .unwrap();

    let queues = queue.list_queues().await.unwrap();
    assert_eq!(3, queues.len());

    let normal_queue = queues
        .iter()
        .find(|queue| queue.queue_name == QUEUE)
        .unwrap();
    assert!(!normal_queue.is_unlogged, "Queue should not be unlogged");
    assert!(
        !normal_queue.is_partitioned,
        "Queue should not be partitioned"
    );

    let unlogged_queue = queues
        .iter()
        .find(|queue| queue.queue_name == unlogged_name)
        .unwrap();
    assert!(unlogged_queue.is_unlogged, "Queue should be unlogged");
    assert!(
        !unlogged_queue.is_partitioned,
        "Queue should not be partitioned"
    );

    let partitioned_queue = queues
        .iter()
        .find(|queue| queue.queue_name == partitioned_name)
        .unwrap();
    assert!(
        !partitioned_queue.is_unlogged,
        "Queue should not be unlogged"
    );
    assert!(
        partitioned_queue.is_partitioned,
        "Queue should be partitioned"
    );
}

#[pgmq_test_macro::queue_test]
async fn queue_metadata_invalid_queue_name(conn_details: ConnDetails, queue: impl Queue) {
    let result: Result<_, _> = queue.queue_metadata("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_test]
async fn queue_metadata_normal(conn_details: ConnDetails, queue: impl Queue) {
    queue.create(QUEUE).await.unwrap();
    let metadata = queue.queue_metadata(QUEUE).await.unwrap().unwrap();
    assert_eq!(QUEUE, metadata.queue_name);
    assert!(!metadata.is_unlogged, "Queue should not be unlogged");
    assert!(!metadata.is_partitioned, "Queue should not be partitioned");
}

#[pgmq_test_macro::queue_test]
async fn queue_metadata_does_not_exist(conn_details: ConnDetails, queue: impl Queue) {
    let metadata = queue.queue_metadata(QUEUE).await.unwrap();
    assert!(metadata.is_none(), "Queue should not exist");
}

#[pgmq_test_macro::queue_test]
async fn queue_metadata_unlogged(conn_details: ConnDetails, queue: impl Queue) {
    queue.create_unlogged(QUEUE).await.unwrap();
    let metadata = queue.queue_metadata(QUEUE).await.unwrap().unwrap();
    assert_eq!(QUEUE, metadata.queue_name);
    assert!(metadata.is_unlogged, "Queue should be unlogged");
    assert!(!metadata.is_partitioned, "Queue should not be partitioned");
}

#[pgmq_test_macro::queue_test]
async fn queue_metadata_partitioned(conn_details: ConnDetails, queue: impl Queue) {
    queue
        .create_partitioned(QUEUE, "1000", "10000")
        .await
        .unwrap();
    let metadata = queue.queue_metadata(QUEUE).await.unwrap().unwrap();
    assert_eq!(QUEUE, metadata.queue_name);
    assert!(!metadata.is_unlogged, "Queue should not be unlogged");
    assert!(metadata.is_partitioned, "Queue should be partitioned");
}

#[pgmq_test_macro::queue_transaction_test]
async fn acquire_queue_lock_invalid_queue_name(
    conn_details: ConnDetails,
    queue: impl QueueTransaction,
) {
    let result: Result<_, _> = queue.acquire_queue_lock("invalid-queue-name").await;
    assert_matches!(
        result,
        Err(PgmqError::QueueNameError(QueueNameError::InvalidCharacter(
            _
        )))
    );
}

#[pgmq_test_macro::queue_transaction_test]
async fn acquire_queue_lock(conn_details: ConnDetails, queue: impl QueueTransaction) {
    use sqlx::Connection;

    queue.acquire_queue_lock(QUEUE).await.unwrap();

    let mut other_conn = initialization::sqlx_conn(&(conn_details.test_db_url.clone())).await;
    let mut other_txn = other_conn.begin().await.unwrap();

    let result =
        tokio::time::timeout(Duration::from_secs(5), other_txn.acquire_queue_lock(QUEUE)).await;

    assert!(
        result.is_err(),
        "Attempting to acquire the lock while it's held by a different transaction should timeout."
    );
}
