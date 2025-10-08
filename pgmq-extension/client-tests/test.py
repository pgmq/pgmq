import json
import os
import pytest
import time

import psycopg


def listen_for_notification(
    db_connection: psycopg.Connection, channel_name: str, timeout: int = 5
) -> bool:
    # wait for notification with timeout using the built-in timeout
    try:
        for notification in db_connection.notifies(timeout=timeout):
            print(
                f"Received notification: channel={notification.channel}, payload={notification.payload}"
            )
            assert notification.channel == channel_name, (
                f"Expected channel {channel_name}, got {notification.channel}"
            )
            return True
    except TimeoutError:
        # No notification received within timeout
        return False


@pytest.fixture
def db_connection():
    """Create database connection using DATABASE_URL environment variable."""
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
    )

    conn = psycopg.Connection.connect(database_url, autocommit=True)
    yield conn
    conn.close()


def test_pgmq_basic_notification(db_connection: psycopg.Connection):
    """Test basic PGMQ operations with notifications."""
    now = int(time.time())
    queue_name = f"test_queue_{now}"

    # create queue
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.create(%s)", (queue_name,))
        result = cur.fetchone()
        print(f"Queue creation result: {result}")

    # enable notifications
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.enable_notify_insert(%s)", (queue_name,))
    # subsequent calls must not erorr
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.enable_notify_insert(%s)", (queue_name,))

    # start listening
    channel_name = f"pgmq.q_{queue_name}.INSERT"
    with db_connection.cursor() as cur:
        cur.execute(f"""LISTEN "{channel_name}";""")
        print(f"Started listening on channel: {channel_name}")

    # send a message
    message = {"hello": "world"}
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.send(%s, %s)", (queue_name, json.dumps(message)))
        result = cur.fetchone()
        msg_id = result[0] if result else None

    # assert message was sent successfully
    assert msg_id is not None
    print(f"Message sent with ID: {msg_id}")

    # wait for notification with timeout
    notification_received = listen_for_notification(
        db_connection, channel_name, timeout=5
    )

    # Assert we received the notification
    assert notification_received, (
        f"Should have received a notification on channel {channel_name} within 5 seconds"
    )

    # disable notification subscription
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.disable_notify_insert(%s)", (queue_name,))
    # subsequent calls must not erorr
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.disable_notify_insert(%s)", (queue_name,))

    # send a message should result in no notification
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.send(%s, %s)", (queue_name, json.dumps(message)))
        result = cur.fetchone()
        msg_id = result[0] if result else None
    assert msg_id is not None

    notification_received = listen_for_notification(
        db_connection, channel_name, timeout=2
    )
    assert not notification_received

    # cleanup
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.drop_queue(%s)", (queue_name,))
        print(f"Dropped queue: {queue_name}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
