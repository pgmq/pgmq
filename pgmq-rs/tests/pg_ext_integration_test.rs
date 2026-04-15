use pgmq::pg_ext::VisibilityTimeoutOffset;
use pgmq::types::{ARCHIVE_PREFIX, PGMQ_SCHEMA, QUEUE_PREFIX};
use pgmq::util::connect;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, Row};
use std::env;
use std::time::Duration;

// always test extension sdk in its own database
// to avoid conflict with client only sdk
fn replace_db_string(s: &str, replacement: &str) -> String {
    match s.rfind('/') {
        Some(pos) => {
            let prefix = &s[0..pos];
            format!("{prefix}{replacement}")
        }
        None => s.to_string(),
    }
}

async fn init_queue_ext(qname: &str) -> pgmq::PGMQueueExt {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let queue = pgmq::PGMQueueExt::new(db_url.clone(), 2)
        .await
        .expect("failed to connect to postgres");
    // ignore error if d already exists
    let _ = sqlx::query("CREATE DATABASE pgmq_ext_test;")
        .execute(&queue.connection)
        .await;
    let test_db_str = replace_db_string(&db_url, "/pgmq_ext_test");
    let queue = pgmq::PGMQueueExt::new(test_db_str.clone(), 2)
        .await
        .expect("failed to connect to test db");
    install_pgmq(&queue).await;
    // make sure queue doesn't exist before the test
    let _ = queue.drop_queue(qname).await;
    // CREATE QUEUE
    let q_success = queue.create(qname).await;
    println!("q_success: {q_success:?}");
    assert!(q_success.is_ok());
    queue
}

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
struct MyMessage {
    foo: String,
    num: u64,
}

impl Default for MyMessage {
    fn default() -> Self {
        MyMessage {
            foo: "bar".to_owned(),
            num: rand::thread_rng().gen_range(0..100),
        }
    }
}

#[derive(Serialize, Debug, Deserialize)]
struct YoloMessage {
    yolo: String,
}

async fn rowcount(qname: &str, connection: &Pool<Postgres>) -> i64 {
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{qname}");
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

async fn archive_rowcount(qname: &str, connection: &Pool<Postgres>) -> i64 {
    let row_ct_query = format!("SELECT count(*) as ct FROM {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{qname}");
    sqlx::query(&row_ct_query)
        .fetch_one(connection)
        .await
        .unwrap()
        .get::<i64, usize>(0)
}

async fn install_pgmq(queue: &pgmq::PGMQueueExt) -> bool {
    #[cfg(feature = "install-sql-embedded")]
    let result = queue.install_sql_from_embedded().await.map(|_| true);
    #[cfg(not(feature = "install-sql"))]
    let result = queue.init().await;

    result.expect("failed to init pgmq")
}

#[tokio::test]
async fn test_ext_create_list_drop() {
    let test_queue = format!(
        "test_ext_create_list_drop_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .expect("test queue was not created")
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<String>>();

    assert!(q_names.contains(&test_queue));

    queue
        .drop_queue(&test_queue)
        .await
        .expect("error dropping queue");

    let post_drop_q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .unwrap_or(vec![])
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<String>>();

    assert!(!post_drop_q_names.contains(&test_queue));
}

async fn test_ext_send_read_delete_core<T: Into<VisibilityTimeoutOffset>>(
    offset1: T,
    offset2: T,
    offset3: T,
    offset4: T,
    offset5: T,
) {
    let test_queue = format!(
        "test_ext_send_read_delete_{}",
        rand::thread_rng().gen_range(0..100000)
    );

    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    let num_rows_queue = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(num_rows_queue, 0);

    let msg_id = queue.send(&test_queue, &msg).await.unwrap();
    assert!(msg_id >= 1);

    let read_message = queue
        .read::<MyMessage>(&test_queue, offset1)
        .await
        .expect("error reading message");
    assert!(read_message.is_some());
    let read_message = read_message.unwrap();
    assert_eq!(read_message.msg_id, msg_id);
    assert_eq!(read_message.message, msg);

    // read again, assert no messages visible
    let read_message = queue
        .read::<MyMessage>(&test_queue, offset2)
        .await
        .expect("error reading message");
    assert!(read_message.is_none());

    // read with poll, blocks until message visible
    let start_poll = std::time::Instant::now();
    let read_with_poll = queue
        .read_batch_with_poll::<MyMessage>(
            &test_queue,
            offset3,
            1,
            Some(std::time::Duration::from_secs(6)),
            None,
        )
        .await
        .expect("error reading message")
        .expect("no message");

    let poll_duration = start_poll.elapsed();

    assert!(poll_duration.as_millis() > 1000);
    assert_eq!(read_with_poll.len(), 1);
    assert_eq!(read_with_poll[0].msg_id, msg_id);

    // change the VT to now
    let _vt_set = queue
        .set_vt::<MyMessage>(&test_queue, msg_id, offset4)
        .await
        .expect("failed to set VT");
    let read_message = queue
        .read::<MyMessage>(&test_queue, offset5)
        .await
        .expect("error reading message")
        .expect("expected a message");
    assert_eq!(read_message.msg_id, msg_id);

    // delete message
    let msg_id_del = queue.send(&test_queue, &msg).await.unwrap();

    let deleted = queue
        .delete(&test_queue, msg_id_del)
        .await
        .expect("failed to delete");
    assert!(deleted);

    // try to delete a message that doesn't exist
    let deleted = queue
        .delete(&test_queue, msg_id_del)
        .await
        .expect("failed to delete");
    assert!(!deleted);
}

#[tokio::test]
async fn test_ext_send_read_delete_i32() {
    test_ext_send_read_delete_core(5i32, 2i32, 5i32, 0i32, 1i32).await;
}

#[tokio::test]
async fn test_ext_send_read_delete_i64() {
    test_ext_send_read_delete_core(5i64, 2i64, 5i64, 0i64, 1i64).await;
}

#[tokio::test]
async fn test_ext_send_read_delete_u32() {
    test_ext_send_read_delete_core(5u32, 2u32, 5u32, 0u32, 1u32).await;
}

#[tokio::test]
async fn test_ext_send_read_delete_u64() {
    test_ext_send_read_delete_core(5u64, 2u64, 5u64, 0u64, 1u64).await;
}

#[tokio::test]
async fn test_ext_send_read_delete_chrono() {
    test_ext_send_read_delete_core(
        chrono::Duration::seconds(5),
        chrono::Duration::seconds(2),
        chrono::Duration::seconds(5),
        chrono::Duration::seconds(0),
        chrono::Duration::seconds(1),
    )
    .await;
}

#[tokio::test]
async fn test_ext_send_read_delete_std() {
    test_ext_send_read_delete_core(
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(2),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(0),
        std::time::Duration::from_secs(1),
    )
    .await;
}

#[tokio::test]
async fn test_ext_send_read_delete_vt_offset() {
    test_ext_send_read_delete_core(
        VisibilityTimeoutOffset::seconds(5),
        VisibilityTimeoutOffset::seconds(2),
        VisibilityTimeoutOffset::seconds(5),
        VisibilityTimeoutOffset::seconds(0),
        VisibilityTimeoutOffset::seconds(1),
    )
    .await;
}

async fn test_ext_send_delay_core(delay: impl Copy + Into<VisibilityTimeoutOffset>) {
    let test_queue = format!(
        "test_ext_send_delay_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let vt = 4;
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    queue.send_delay(&test_queue, &msg, delay).await.unwrap();

    // No messages are found due to visibility timeout
    let no_messages = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(no_messages.is_none());

    // After the delay, message is found
    let duration: VisibilityTimeoutOffset = delay.into();
    tokio::time::sleep(Duration::from_secs(duration.as_seconds() as u64)).await;

    let one_messages = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(one_messages.is_some());
}

#[tokio::test]
async fn test_ext_send_delay_i32() {
    test_ext_send_delay_core(5i32).await;
}

#[tokio::test]
async fn test_ext_send_delay_i64() {
    test_ext_send_delay_core(5i64).await;
}

#[tokio::test]
async fn test_ext_send_delay_u32() {
    test_ext_send_delay_core(5u32).await;
}

#[tokio::test]
async fn test_ext_send_delay_u64() {
    test_ext_send_delay_core(5u64).await;
}

#[tokio::test]
async fn test_ext_send_delay_chrono() {
    test_ext_send_delay_core(chrono::Duration::seconds(5)).await;
}

#[tokio::test]
async fn test_ext_send_delay_std() {
    test_ext_send_delay_core(std::time::Duration::from_secs(5)).await;
}

#[tokio::test]
async fn test_ext_send_delay_vt_offset() {
    test_ext_send_delay_core(VisibilityTimeoutOffset::seconds(5)).await;
}

#[tokio::test]
async fn test_ext_send_batch() {
    let test_queue = format!(
        "test_ext_send_batch_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msgs = [
        MyMessage::default(),
        MyMessage::default(),
        MyMessage::default(),
    ];
    let msg_ids = queue.send_batch(&test_queue, &msgs).await.unwrap();
    assert_eq!(3, msg_ids.len());

    let vt = 4;
    let msg1 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg2 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg3 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg4 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(msg1.is_some());
    assert!(msg2.is_some());
    assert!(msg3.is_some());
    assert!(msg4.is_none());
}

#[tokio::test]
async fn test_ext_send_batch_read_batch() {
    let test_queue = format!(
        "test_ext_send_batch_read_batch_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let vt = 4;
    let msgs_read = queue
        .read_batch::<MyMessage>(&test_queue, vt, 1)
        .await
        .unwrap();
    assert!(msgs_read.is_empty());

    let msgs_sent = [
        MyMessage::default(),
        MyMessage::default(),
        MyMessage::default(),
    ];
    let msg_ids = queue.send_batch(&test_queue, &msgs_sent).await.unwrap();
    assert_eq!(3, msg_ids.len());

    let msgs_read = queue
        .read_batch::<MyMessage>(&test_queue, vt, (msgs_sent.len() as i32) - 1)
        .await
        .expect("Should successfully read a batch of messages");
    assert_eq!(msgs_sent.len() - 1, msgs_read.len());

    let msgs_read = queue
        .read_batch::<MyMessage>(&test_queue, vt, 1)
        .await
        .unwrap();
    assert_eq!(1, msgs_read.len());

    let msgs_read = queue
        .read_batch::<MyMessage>(&test_queue, vt, 1)
        .await
        .unwrap();
    assert!(msgs_read.is_empty());
}

#[tokio::test]
async fn test_ext_read_with_poll() {
    let test_queue = format!(
        "test_ext_read_with_poll_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let vt = 4;
    let msg = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(msg.is_none());

    let msgs_sent = [
        MyMessage::default(),
        MyMessage::default(),
        MyMessage::default(),
    ];
    let msg_ids = queue.send_batch(&test_queue, &msgs_sent).await.unwrap();
    assert_eq!(3, msg_ids.len());

    let msg = queue
        .read_with_poll::<MyMessage>(&test_queue, vt, Some(Duration::from_secs(1)), None)
        .await
        .unwrap();
    assert!(msg.is_some());

    let msgs_read = queue
        .read_batch::<MyMessage>(&test_queue, vt, msgs_sent.len() as i32)
        .await
        .unwrap();
    assert_eq!(msgs_sent.len() - 1, msgs_read.len());
}

#[tokio::test]
async fn test_ext_read_batch_with_poll_empty_queue() {
    let test_queue = format!(
        "test_ext_read_batch_with_poll_empty_queue_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let vt = 4;

    // read_batch_with_poll should return Ok(Some(<empty vec>)) if no items are available to be read.
    // Todo: In a future SemVer breaking change, the expected return value would be Ok(<empty vec>)
    let msg_read = queue
        .read_batch_with_poll::<MyMessage>(&test_queue, vt, 1, Some(Duration::from_secs(1)), None)
        .await
        .unwrap();
    assert!(msg_read.is_some());
    assert!(msg_read.unwrap().is_empty());
}

#[tokio::test]
async fn test_ext_read_with_poll_empty_queue() {
    let test_queue = format!(
        "test_ext_read_with_poll_empty_queue_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;

    let vt = 4;

    let msg = queue
        .read_with_poll::<MyMessage>(&test_queue, vt, Some(Duration::from_secs(1)), None)
        .await
        .unwrap();
    assert!(msg.is_none());
}

async fn test_ext_send_batch_delay_core(delay: impl Copy + Into<VisibilityTimeoutOffset>) {
    let test_queue = format!(
        "test_ext_send_batch_delay_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msgs = [
        MyMessage::default(),
        MyMessage::default(),
        MyMessage::default(),
    ];
    let msg_ids = queue
        .send_batch_with_delay(&test_queue, &msgs, delay)
        .await
        .unwrap();
    assert_eq!(3, msg_ids.len());

    // No messages are found due to visibility timeout
    let vt = 4;
    let no_messages = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(no_messages.is_none());

    // After the delay, messages are found
    let duration: VisibilityTimeoutOffset = delay.into();
    tokio::time::sleep(Duration::from_secs(duration.as_seconds() as u64)).await;

    let msg1 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg2 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg3 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    let msg4 = queue.read::<MyMessage>(&test_queue, vt).await.unwrap();
    assert!(msg1.is_some());
    assert!(msg2.is_some());
    assert!(msg3.is_some());
    assert!(msg4.is_none());
}

#[tokio::test]
async fn test_ext_send_batch_delay_i32() {
    test_ext_send_batch_delay_core(5i32).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_i64() {
    test_ext_send_batch_delay_core(5i64).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_u32() {
    test_ext_send_batch_delay_core(5u32).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_u64() {
    test_ext_send_batch_delay_core(5u64).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_chrono() {
    test_ext_send_batch_delay_core(chrono::Duration::seconds(5)).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_std() {
    test_ext_send_batch_delay_core(std::time::Duration::from_secs(5)).await;
}

#[tokio::test]
async fn test_ext_send_batch_delay_vt_offset() {
    test_ext_send_batch_delay_core(VisibilityTimeoutOffset::seconds(5)).await;
}

#[tokio::test]
async fn test_ext_send_pop() {
    let test_queue = format!(
        "test_ext_send_pop_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let _ = queue.send(&test_queue, &msg).await.unwrap();

    let popped = queue
        .pop::<MyMessage>(&test_queue)
        .await
        .expect("failed to pop")
        .expect("no message to pop");
    assert_eq!(popped.message, msg);
}

#[tokio::test]
async fn test_ext_send_archive() {
    let test_queue = format!(
        "test_ext_send_archive_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let msg_id = queue.send(&test_queue, &msg).await.unwrap();

    let archived = queue
        .archive(&test_queue, msg_id)
        .await
        .expect("failed to archive");
    assert!(archived);
}

#[tokio::test]
async fn test_ext_archive_batch() {
    let test_queue = format!(
        "test_ext_archive_batch_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();

    let m1 = queue.send(&test_queue, &msg).await.unwrap();
    let m2 = queue.send(&test_queue, &msg).await.unwrap();
    let m3 = queue.send(&test_queue, &msg).await.unwrap();

    let archive_result = queue
        .archive_batch(&test_queue, &[m1, m2, m3])
        .await
        .expect("archive batch error");

    let post_archive_rowcount = rowcount(&test_queue, &queue.connection).await;

    assert_eq!(post_archive_rowcount, 0);
    assert_eq!(archive_result, 3);

    let post_archive_archive_rowcount = archive_rowcount(&test_queue, &queue.connection).await;
    assert_eq!(post_archive_archive_rowcount, 3);
}

#[tokio::test]
async fn test_ext_delete_batch() {
    let test_queue = format!(
        "test_ext_delete_batch{}",
        rand::thread_rng().gen_range(0..100000)
    );

    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    let m1 = queue.send(&test_queue, &msg).await.unwrap();
    let m2 = queue.send(&test_queue, &msg).await.unwrap();
    let m3 = queue.send(&test_queue, &msg).await.unwrap();
    let delete_result = queue
        .delete_batch(&test_queue, &[m1, m2, m3])
        .await
        .expect("delete batch error");
    let post_delete_rowcount = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(post_delete_rowcount, 0);
    assert_eq!(delete_result, 3);
}

#[tokio::test]
async fn test_ext_purge_queue() {
    let test_queue = format!(
        "test_ext_purge_queue{}",
        rand::thread_rng().gen_range(0..100000)
    );

    let queue = init_queue_ext(&test_queue).await;
    let msg = MyMessage::default();
    let _ = queue.send(&test_queue, &msg).await.unwrap();
    let _ = queue.send(&test_queue, &msg).await.unwrap();
    let _ = queue.send(&test_queue, &msg).await.unwrap();

    let purged_count = queue
        .purge_queue(&test_queue)
        .await
        .expect("purge queue error");

    assert_eq!(purged_count, 3);
    let post_purge_rowcount = rowcount(&test_queue, &queue.connection).await;
    assert_eq!(post_purge_rowcount, 0);
}

#[tokio::test]
async fn test_pgmq_init() {
    let test_queue = format!(
        "test_ext_init_queue{}",
        rand::thread_rng().gen_range(0..100000)
    );
    let queue = init_queue_ext(&test_queue).await;
    let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_partman")
        .execute(&queue.connection)
        .await
        .expect("failed to create extension");
    // error mode on queue partitioned create but already exists
    let qname = format!("test_dup_{}", rand::thread_rng().gen_range(0..100));
    let created = queue
        .create_partitioned(&qname)
        .await
        .expect("failed attempting to create queue");
    assert!(created, "did not create queue");
    // create again
    let created = queue
        .create_partitioned(&qname)
        .await
        .expect("failed attempting to create the duplicate queue");
    assert!(!created, "failed to detect duplicate queue");
}

/// test creating queue in transaction
#[tokio::test]
async fn test_create_txn() {
    // use test harness to create a connection pool
    let _q = format!("_q_{}", rand::thread_rng().gen_range(0..100000));
    let _queue = init_queue_ext(&_q).await;
    let pool = _queue.connection;

    // init a new queue
    let queue = init_queue_ext(&_q).await;
    // start a txn
    let mut tx = pool.begin().await.expect("failed to start transaction");
    let q = format!(
        "test_create_txn_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    // use the pool to create a new queue
    queue
        .create_with_cxn(&q, &mut *tx)
        .await
        .expect("failed to create queue in txn");
    // commit txn
    tx.commit().await.expect("failed to commit txn");
    // verify queue exists
    let q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .expect("test queue was not created")
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<_>>();
    assert!(q_names.contains(&q), "failed to find created queue");

    // rollback txn, verify queue not created
    let mut tx = pool.begin().await.expect("failed to start transaction");
    let q_rollback = format!(
        "test_create_txn_rb_{}",
        rand::thread_rng().gen_range(0..100000)
    );
    // use the pool to create a new queue
    queue
        .create_with_cxn(&q_rollback, &mut *tx)
        .await
        .expect("failed to create queue in txn");
    // rollback txn
    tx.rollback().await.expect("failed to rollback txn");
    // verify queue does not exist
    let q_names = queue
        .list_queues()
        .await
        .expect("error listing queues")
        .expect("test queue was not created")
        .iter()
        .map(|q| q.queue_name.clone())
        .collect::<Vec<_>>();
    assert!(
        !q_names.contains(&q_rollback),
        "found queue that should not exist"
    );
}

/// test "bring your own pool"
#[tokio::test]
async fn test_byop() {
    // use test harness to create a connection pool
    let _q = format!("test_byop_{}", rand::thread_rng().gen_range(0..100000));
    let _queue = init_queue_ext(&_q).await;
    let pool = _queue.connection;

    // use the pool to create a new queue
    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    let init = install_pgmq(&queue).await;
    assert!(init, "failed to create extension");

    // first time must return true
    let test_queue = format!("test_byop_{}", rand::thread_rng().gen_range(0..100000));
    let created = queue
        .create(&test_queue)
        .await
        .expect("failed to create queue");
    assert!(created, "failed to create queue_{}", test_queue);

    // second time must return false
    let created = queue
        .create(&test_queue)
        .await
        .expect("failed execute create queue");
    assert!(!created, "failed to detect duplicate queue");
}

#[tokio::test]
async fn test_transactional() {
    let test_queue = format!("test_tx_{}", rand::thread_rng().gen_range(0..100000));
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    // pool_0 for the queue object and transaction
    let pool_0 = connect(&db_url, 2)
        .await
        .expect("failed to connect to postgres");
    // pool_1 for querying outside of transaction
    let pool_1 = connect(&db_url, 2)
        .await
        .expect("failed to connect to postgres");

    // create queue using pool_0
    let queue = pgmq::PGMQueueExt::new_with_pool(pool_0.clone()).await;
    let init = install_pgmq(&queue).await;
    assert!(init, "failed to create extension");

    let created = queue
        .create_with_cxn(&test_queue, &pool_0)
        .await
        .expect("failed to create queue");
    assert!(created);

    let mut tx = pool_0.begin().await.expect("failed to start transaction");

    // transaction still open, but message sent
    let sent_msg = queue
        .send_with_cxn(&test_queue, &MyMessage::default(), &mut *tx)
        .await
        .expect("failed to send message");
    assert_eq!(sent_msg, 1);

    // transaction still not closed, no rows yet
    let query = format!("SELECT count(*) FROM pgmq.q_{test_queue}");
    let rows = sqlx::query(&query)
        .fetch_one(&pool_1)
        .await
        .expect("failed to fetch row")
        .get::<i64, usize>(0);
    assert_eq!(rows, 0);

    tx.commit().await.expect("failed to commit transaction");

    // transaction now committed, row is available
    let rows = sqlx::query(&query)
        .fetch_one(&pool_1)
        .await
        .expect("failed to fetch row")
        .get::<i64, usize>(0);
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn test_create_queue_race_condition() {
    let queue_name = format!("test_tx_{}", rand::thread_rng().gen_range(0..100000));
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let pool = connect(&db_url, 2)
        .await
        .expect("failed to connect to postgres");

    let queue = pgmq::PGMQueueExt::new_with_pool(pool).await;
    let init = install_pgmq(&queue).await;
    assert!(init, "failed to create extension");

    let mut conn1 = queue.connection.acquire().await.unwrap();
    let mut conn2 = queue.connection.acquire().await.unwrap();

    let (result1, result2) = tokio::try_join!(
        queue.create_with_cxn(&queue_name, &mut conn1),
        queue.create_with_cxn(&queue_name, &mut conn2)
    )
    .unwrap();

    // If there's a race condition in `PGMQueueExt#create`, both results could be `true` (this
    // may not always occur due to the non-deterministic nature of race conditions).
    assert_ne!(result1, result2);
}
