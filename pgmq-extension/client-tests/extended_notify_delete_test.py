"""
PGMQ Extended Delete Notification Tests

This test suite comprehensively tests the PostgreSQL PGMQ delete notification
mechanism, which allows clients to be notified when messages are deleted from
queues (via pgmq.delete() or pgmq.pop()).

Tests cover:
- enable_notify_delete() and disable_notify_delete() functions
- Both function variations (with and without throttle parameter)
- Notification throttling mechanism (limiting notification frequency)
- Zero throttle behavior (no throttling, all deletes trigger notifications)
- Dynamic throttle interval changes
- Idempotent operations (safe to call multiple times)
- Input validation (rejecting negative throttle values)
- Cleanup: drop_queue() removes entries from notify_delete_throttle table

The notification channel name is: pgmq.q_{queue_name}.DELETE

Run tests with:
    pytest extended_notify_delete_test.py -v
    or
    uv run pytest extended_notify_delete_test.py -v
"""

import json
import os
import time

import psycopg
import pytest


@pytest.fixture
def db_connection():
    """Create database connection using DATABASE_URL environment variable."""
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
    )

    conn = psycopg.Connection.connect(database_url, autocommit=True)
    yield conn
    conn.close()


def listen_for_notification(
    db_connection: psycopg.Connection, channel_name: str, timeout: int = 5
) -> bool | None:
    """Wait for notification with timeout. Returns True if notification received, False otherwise."""
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


def count_notifications(
    db_connection: psycopg.Connection, channel_name: str, timeout: int = 5
) -> int:
    """Count how many notifications are received within the timeout period."""
    count = 0
    try:
        for notification in db_connection.notifies(timeout=timeout):
            if notification.channel == channel_name:
                count += 1
                print(
                    f"Received notification #{count}: channel={notification.channel}, payload={notification.payload}"
                )
    except TimeoutError:
        # Timeout reached, return count
        pass
    return count


# Helper functions to reduce code duplication
def create_queue(db_connection: psycopg.Connection, queue_name: str) -> None:
    """Create a PGMQ queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.create(%s)", (queue_name,))
        print(f"Created queue: {queue_name}")


def enable_delete_notifications(
    db_connection: psycopg.Connection, queue_name: str, throttle_ms: int | None = None
) -> None:
    """Enable delete notifications for a queue with optional throttle."""
    with db_connection.cursor() as cur:
        if throttle_ms is None:
            cur.execute("SELECT pgmq.enable_notify_delete(%s)", (queue_name,))
            print(f"Enabled delete notifications for queue: {queue_name}")
        else:
            cur.execute(
                "SELECT pgmq.enable_notify_delete(%s, %s)", (queue_name, throttle_ms)
            )
            print(
                f"Enabled delete notifications with {throttle_ms}ms throttle for queue: {queue_name}"
            )


def disable_delete_notifications(db_connection: psycopg.Connection, queue_name: str) -> None:
    """Disable delete notifications for a queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.disable_notify_delete(%s)", (queue_name,))
        print(f"Disabled delete notifications for queue: {queue_name}")


def start_listening(db_connection: psycopg.Connection, queue_name: str) -> str:
    """Start listening on a queue's delete notification channel. Returns the channel name."""
    channel_name = f"pgmq.q_{queue_name}.DELETE"
    with db_connection.cursor() as cur:
        cur.execute(f"""LISTEN \"{channel_name}\";""")
        print(f"Started listening on channel: {channel_name}")

    # Small delay to ensure listener is ready
    time.sleep(0.5)

    return channel_name


def send_message(
    db_connection: psycopg.Connection, queue_name: str, message: dict
) -> int:
    """Send a message to a queue. Returns the message ID."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.send(%s, %s)", (queue_name, json.dumps(message)))
        result = cur.fetchone()
        msg_id = result[0] if result else None
        if msg_id:
            print(f"Sent message with ID: {msg_id}")
        return msg_id


def delete_message(
    db_connection: psycopg.Connection, queue_name: str, msg_id: int
) -> bool:
    """Delete a message from a queue. Returns True if the message was deleted."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.delete(%s, %s)", (queue_name, msg_id))
        result = cur.fetchone()
        deleted = result[0] if result else False
        print(f"Deleted message ID {msg_id}: {deleted}")
        return deleted


def send_and_delete_messages(
    db_connection: psycopg.Connection, queue_name: str, message: dict, count: int
) -> None:
    """Send then individually delete `count` messages, producing `count` DELETE trigger firings."""
    msg_ids = []
    print(f"Sending {count} messages...")
    for i in range(count):
        msg_id = send_message(db_connection, queue_name, message)
        msg_ids.append(msg_id)
        print(f"  Message {i + 1}/{count} sent with ID: {msg_id}")
    for msg_id in msg_ids:
        delete_message(db_connection, queue_name, msg_id)


def drop_queue(db_connection: psycopg.Connection, queue_name: str) -> None:
    """Drop a PGMQ queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.drop_queue(%s)", (queue_name,))
        print(f"Dropped queue: {queue_name}")


# noinspection SqlNoDataSourceInspection
def get_delete_throttle_interval(db_connection: psycopg.Connection, queue_name: str) -> int:
    """Get the throttle interval for a queue from notify_delete_throttle table."""
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT throttle_interval_ms FROM pgmq.notify_delete_throttle WHERE queue_name = %s",
            (queue_name,),
        )
        result = cur.fetchone()
        return result[0] if result else None


def verify_delete_throttle_interval(
    db_connection: psycopg.Connection, queue_name: str, expected_ms: int
) -> None:
    """Verify that the delete throttle interval is set correctly."""
    throttle_ms = get_delete_throttle_interval(db_connection, queue_name)
    assert (
        throttle_ms == expected_ms
    ), f"Expected throttle_interval_ms = {expected_ms}, got {throttle_ms}"
    print(f"Verified: throttle_interval_ms = {throttle_ms}")


# noinspection SqlNoDataSourceInspection
def count_delete_throttle_entries(db_connection: psycopg.Connection, queue_name: str) -> int:
    """Count entries in notify_delete_throttle for a queue."""
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pgmq.notify_delete_throttle WHERE queue_name = %s",
            (queue_name,),
        )
        return cur.fetchone()[0]


def test_enable_disable_delete_notifications(db_connection: psycopg.Connection):
    """Test that enable_notify_delete and disable_notify_delete work correctly."""
    now = int(time.time())
    queue_name = f"test_del_enable_disable_{now}"

    try:
        create_queue(db_connection, queue_name)
        enable_delete_notifications(db_connection, queue_name)
        channel_name = start_listening(db_connection, queue_name)

        # Send a message - should trigger delete notification when deleted
        message = {"test": "enable_disable", "value": 1}
        msg_id = send_message(db_connection, queue_name, message)
        assert msg_id is not None

        delete_message(db_connection, queue_name, msg_id)

        # Should receive notification
        notification_received = listen_for_notification(db_connection, channel_name, timeout=5)
        assert notification_received, (
            "Should have received notification after enabling delete notifications"
        )

        # Disable notifications
        disable_delete_notifications(db_connection, queue_name)

        # Send and delete another message - should NOT trigger notification
        msg_id = send_message(db_connection, queue_name, message)
        assert msg_id is not None
        delete_message(db_connection, queue_name, msg_id)

        # Should NOT receive notification
        notification_received = listen_for_notification(db_connection, channel_name, timeout=2)
        assert not notification_received, (
            "Should NOT have received notification after disabling delete notifications"
        )

        print("Test passed: enable/disable delete notifications work correctly")

    finally:
        drop_queue(db_connection, queue_name)


def test_enable_delete_notify_variations(db_connection: psycopg.Connection):
    """Test both variations of enable_notify_delete function."""
    now = int(time.time())
    queue_name_no_param = f"test_del_no_param_{now}"
    queue_name_with_zero = f"test_del_with_zero_{now}"

    try:
        # Test 1: enable_notify_delete(queue_name) - no throttle parameter
        create_queue(db_connection, queue_name_no_param)
        enable_delete_notifications(db_connection, queue_name_no_param)
        verify_delete_throttle_interval(db_connection, queue_name_no_param, 250)  # 250ms default

        # Test 2: enable_notify_delete(queue_name, 0) - explicit zero
        create_queue(db_connection, queue_name_with_zero)
        enable_delete_notifications(db_connection, queue_name_with_zero, throttle_ms=0)
        verify_delete_throttle_interval(db_connection, queue_name_with_zero, 0)

        # Test both queues receive notifications
        channel_no_param = start_listening(db_connection, queue_name_no_param)
        channel_with_zero = start_listening(db_connection, queue_name_with_zero)

        # Send and delete messages on both queues
        message = {"test": "variations"}
        msg_id1 = send_message(db_connection, queue_name_no_param, message)
        msg_id2 = send_message(db_connection, queue_name_with_zero, message)
        delete_message(db_connection, queue_name_no_param, msg_id1)
        delete_message(db_connection, queue_name_with_zero, msg_id2)

        # Both should receive notifications
        notification1 = listen_for_notification(db_connection, channel_no_param, timeout=3)
        notification2 = listen_for_notification(db_connection, channel_with_zero, timeout=3)

        assert notification1, "Queue with no param should receive delete notification"
        assert notification2, "Queue with explicit 0 should receive delete notification"

        print("Test passed: both variations of enable_notify_delete work correctly")

    finally:
        drop_queue(db_connection, queue_name_no_param)
        drop_queue(db_connection, queue_name_with_zero)


def test_delete_throttling_mechanism(db_connection: psycopg.Connection):
    """Test that the throttling mechanism works correctly."""
    now = int(time.time())
    queue_name = f"test_del_throttle_{now}"
    throttle_ms = 1000  # 1 second throttle

    try:
        create_queue(db_connection, queue_name)
        enable_delete_notifications(db_connection, queue_name, throttle_ms=throttle_ms)
        verify_delete_throttle_interval(db_connection, queue_name, throttle_ms)

        channel_name = start_listening(db_connection, queue_name)

        # Send and delete multiple messages rapidly
        num_messages = 5
        send_and_delete_messages(db_connection, queue_name, {"test": "throttling"}, num_messages)

        # Count notifications received - should only receive 1 due to throttling
        notification_count = count_notifications(db_connection, channel_name, timeout=2)

        print(f"Received {notification_count} notification(s) for {num_messages} deletes")
        assert notification_count == 1, (
            f"Expected 1 notification due to throttling, got {notification_count}"
        )

        # Wait for throttle interval to elapse
        wait_time = (throttle_ms / 1000.0) + 0.5  # Add 0.5s buffer
        print(f"Waiting {wait_time}s for throttle interval to elapse...")
        time.sleep(wait_time)

        # Delete another message after throttle interval
        msg_id = send_message(db_connection, queue_name, {"test": "throttling"})
        delete_message(db_connection, queue_name, msg_id)

        # Should receive another notification now
        notification_received = listen_for_notification(db_connection, channel_name, timeout=3)
        assert notification_received, (
            "Should receive notification after throttle interval has elapsed"
        )

        print("Test passed: delete throttling mechanism works correctly")

    finally:
        drop_queue(db_connection, queue_name)


def test_no_delete_throttling_when_zero(db_connection: psycopg.Connection):
    """Test that when throttle is zero, all deletes trigger notifications (no throttling)."""
    now = int(time.time())
    queue_name = f"test_del_no_throttle_{now}"
    throttle_ms = 0  # No throttling

    try:
        create_queue(db_connection, queue_name)
        enable_delete_notifications(db_connection, queue_name, throttle_ms=throttle_ms)
        channel_name = start_listening(db_connection, queue_name)

        # Send and delete multiple messages rapidly
        num_messages = 5
        send_and_delete_messages(db_connection, queue_name, {"test": "no_throttling"}, num_messages)

        # Count notifications received - should receive ALL notifications
        notification_count = count_notifications(db_connection, channel_name, timeout=3)

        print(f"Received {notification_count} notification(s) for {num_messages} deletes")
        assert notification_count == num_messages, (
            f"Expected {num_messages} notifications with zero throttle, got {notification_count}"
        )

        print("Test passed: no throttling applied when throttle_interval_ms is 0")

    finally:
        drop_queue(db_connection, queue_name)


def test_delete_throttling_with_different_intervals(db_connection: psycopg.Connection):
    """Test throttling with different interval values."""
    now = int(time.time())
    queue_name = f"test_del_throttle_intervals_{now}"

    try:
        create_queue(db_connection, queue_name)

        # Test with 500ms throttle
        throttle_ms = 500
        enable_delete_notifications(db_connection, queue_name, throttle_ms=throttle_ms)
        channel_name = start_listening(db_connection, queue_name)

        # Send and delete a message
        msg_id = send_message(db_connection, queue_name, {"test": "interval_500"})
        delete_message(db_connection, queue_name, msg_id)

        # Should receive notification
        notification_received = listen_for_notification(db_connection, channel_name, timeout=2)
        assert notification_received, "Should receive first notification"

        # Change throttle interval to 2000ms
        new_throttle_ms = 2000
        enable_delete_notifications(db_connection, queue_name, throttle_ms=new_throttle_ms)
        verify_delete_throttle_interval(db_connection, queue_name, new_throttle_ms)

        # Send and delete multiple messages rapidly
        send_and_delete_messages(db_connection, queue_name, {"test": "interval_500"}, 3)

        # Should only get 1 notification
        notification_count = count_notifications(db_connection, channel_name, timeout=1)
        assert notification_count == 1, (
            f"Expected 1 notification with new throttle, got {notification_count}"
        )

        print("Test passed: delete throttle interval can be changed dynamically")

    finally:
        drop_queue(db_connection, queue_name)


def test_idempotent_enable_disable_delete(db_connection: psycopg.Connection):
    """Test that enable_notify_delete and disable_notify_delete are idempotent."""
    now = int(time.time())
    queue_name = f"test_del_idempotent_{now}"

    try:
        create_queue(db_connection, queue_name)

        # Enable notifications multiple times - should not error
        for i in range(3):
            enable_delete_notifications(db_connection, queue_name)
            print(f"Called enable_notify_delete #{i + 1}")

        # Verify only one entry exists
        count = count_delete_throttle_entries(db_connection, queue_name)
        assert count == 1, f"Expected 1 entry, got {count}"
        print(f"Verified: only 1 entry in notify_delete_throttle")

        # Disable notifications multiple times - should not error
        for i in range(3):
            disable_delete_notifications(db_connection, queue_name)
            print(f"Called disable_notify_delete #{i + 1}")

        # Verify entry is removed
        count = count_delete_throttle_entries(db_connection, queue_name)
        assert count == 0, f"Expected 0 entries, got {count}"
        print(f"Verified: entry removed from notify_delete_throttle")

        print("Test passed: enable/disable delete functions are idempotent")

    finally:
        drop_queue(db_connection, queue_name)


def test_negative_delete_throttle_validation(db_connection: psycopg.Connection):
    """Test that negative throttle_interval_ms is rejected."""
    now = int(time.time())
    queue_name = f"test_del_negative_throttle_{now}"

    try:
        create_queue(db_connection, queue_name)

        # Try to enable notifications with negative throttle - should raise exception
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.enable_notify_delete(%s, %s)", (queue_name, -100))

        assert "throttle_interval_ms must be non-negative" in str(exc_info.value)
        print("Test passed: negative throttle_interval_ms is rejected")

    finally:
        drop_queue(db_connection, queue_name)


def test_drop_queue_cleans_delete_throttle_table(db_connection: psycopg.Connection):
    """Test that dropping a queue removes its entry from notify_delete_throttle table."""
    now = int(time.time())
    queue_name = f"test_del_drop_cleanup_{now}"

    try:
        create_queue(db_connection, queue_name)

        # Enable notifications to create entry in notify_delete_throttle
        throttle_ms = 1000
        enable_delete_notifications(db_connection, queue_name, throttle_ms=throttle_ms)

        # Verify entry exists in notify_delete_throttle
        count = count_delete_throttle_entries(db_connection, queue_name)
        assert count == 1, f"Expected 1 entry in notify_delete_throttle, got {count}"
        print(f"Verified: entry exists in notify_delete_throttle before drop")

        # Drop the queue
        drop_queue(db_connection, queue_name)

        # Verify entry is deleted from notify_delete_throttle
        count = count_delete_throttle_entries(db_connection, queue_name)
        assert count == 0, f"Expected 0 entries in notify_delete_throttle after drop, got {count}"
        print(f"Verified: entry removed from notify_delete_throttle after drop")

        print("Test passed: drop_queue cleans up notify_delete_throttle table")

    finally:
        drop_queue(db_connection, queue_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
