#!/usr/bin/env python3
"""
Simple pytest test for PGMQ basic operations.

To run:
    pip install pytest psycopg[binary]
    DATABASE_URL="postgresql://user:pass@localhost:5432/db" pytest simple_test.py -v
"""

import json
import os
import pytest
import time

import psycopg


@pytest.fixture
def db_connection():
    """Create database connection using DATABASE_URL environment variable."""
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
    )

    conn = psycopg.Connection.connect(database_url, autocommit=True)
    yield conn
    conn.close()


def test_pgmq_basic_operations(db_connection):
    """Test basic PGMQ operations with notifications."""
    now = int(time.time())
    queue_name = f"test_queue_{now}"

    # Create queue
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.create(%s)", (queue_name,))
        result = cur.fetchone()
        print(f"Queue creation result: {result}")

    # Enable notifications
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.enable_notify_insert(%s)", (queue_name,))

    # Start listening
    channel_name = f"pgmq.q_{queue_name}.INSERT"
    with db_connection.cursor() as cur:
        cur.execute(f"""LISTEN "{channel_name}";""")
        print(f"Started listening on channel: {channel_name}")

    # Send a message
    message = {"hello": "world"}
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.send(%s, %s)", (queue_name, json.dumps(message)))
        result = cur.fetchone()
        msg_id = result[0] if result else None

    # Assert message was sent successfully
    assert msg_id is not None, "Message should be sent and return a message ID"
    print(f"Message sent with ID: {msg_id}")

    # Wait for notification with timeout
    notification_received = False
    timeout = 5  # seconds
    start_time = time.time()

    while time.time() - start_time < timeout and not notification_received:
        # Check for notifications
        notifications = db_connection.notifies
        if notifications:
            for notification in notifications():
                print(
                    f"Received notification: channel={notification.channel}, payload={notification.payload}"
                )
                assert notification.channel == channel_name, (
                    f"Expected channel {channel_name}, got {notification.channel}"
                )
                notification_received = True
                break
            time.sleep(0.1)

    # Assert we received the notification
    assert notification_received, (
        f"Should have received a notification on channel {channel_name} within {timeout} seconds"
    )

    # Cleanup
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.drop_queue(%s)", (queue_name,))
        print(f"Dropped queue: {queue_name}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
