use pgmq::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Deserialize, Eq, PartialEq)]
struct MyMessage {
    foo: String,
    num: u64,
}

#[tokio::main]
async fn main() {
    let db_url = "postgres://postgres:postgres@localhost:5432/postgres".to_string();
    let queue = pgmq::PGMQueueExt::new(db_url, 2)
        .await
        .expect("failed to connect to postgres");

    let _ = queue.install_sql(Some(&"1.10.0".to_string())).await;

    queue
        .create("my_queue")
        .await
        .expect("failed to create queue");

    let msg = MyMessage {
        foo: "hello".to_string(),
        num: 42,
    };
    queue
        .send("my_queue", &msg)
        .await
        .expect("failed to send message");
    let received_struct_message: Message<MyMessage> = queue
        .read::<MyMessage>(&"my_queue", 15)
        .await
        .unwrap()
        .expect("No messages in the queue");
    println!("Received a message: {received_struct_message:?}");
}
