use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;

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

#[ignore]
#[cfg(feature = "cli")]
#[tokio::test]
async fn test_sql_lifecycle() {
    let test_num = rand::thread_rng().gen_range(0..100000);
    let test_queue = format!("test_sql_lifecycle_{}", test_num);
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let test_db_name = format!("pgmq_test_{}", test_num);
    let test_db_url = replace_db_string(&db_url, &format!("/{test_db_name}"));
    println!("test_db_url: {test_db_url}");
    let pool = PgPool::connect(&db_url).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {test_db_name};"))
        .execute(&pool)
        .await
        .unwrap();

    let queue = pgmq::PGMQueueExt::new(test_db_url, 1).await.unwrap();
    // assign version from env
    let v = match env::var("PGMQ_VERSION") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    };
    queue.install_sql(v.as_ref()).await.unwrap();
    queue.create(&test_queue).await.unwrap();

    let sent_msg = MyMessage::default();
    let msg_id = queue.send(&test_queue, &sent_msg).await.unwrap();
    let read_msg = queue
        .read::<MyMessage>(&test_queue, 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg_id, read_msg.msg_id);
    assert_eq!(sent_msg, read_msg.message);
    queue.archive(&test_queue, msg_id).await.unwrap();
    let read_none = queue.read::<MyMessage>(&test_queue, 30).await.unwrap();
    assert!(read_none.is_none());
}
fn replace_db_string(s: &str, replacement: &str) -> String {
    match s.rfind('/') {
        Some(pos) => {
            let prefix = &s[0..pos];
            format!("{prefix}{replacement}")
        }
        None => s.to_string(),
    }
}
