"""
Tests that verify pgmq existing functionality survives and new functionality is properly
introduced in an extension upgrade.

Run in two phases controlled by the `UPGRADE_PHASE` environment variable:

  Phase "pre" (run on old version):
    - Creates queues and populates them with messages
    - Archives some messages, deletes others
    - Verifies basic operations work on the old version

  Phase "post" (run on the new version after ALTER EXTENSION pgmq UPDATE):
    - Verifies queues created pre-upgrade still exist
    - Verifies messages sent pre-upgrade are still readable
    - Verifies all operations (send, read, delete, archive, pop, set_vt,
      metrics, send_batch) work on pre-upgrade queues
    - Creates a new queue to verify queue creation works post-upgrade

Usage:
    UPGRADE_PHASE=pre  uv run pytest upgrade_test.py -v
    ... then upgrade pgmq
    UPGRADE_PHASE=post uv run pytest upgrade_test.py -v
"""

import os
import json

import psycopg
import pytest

PHASE = os.getenv("UPGRADE_PHASE", "").lower()
QUEUE_NAME = "upgrade_test_main"
QUEUE_UNLOGGED = "upgrade_test_unlogged"
QUEUE_PARTITIONED = "upgrade_test_partitioned"
POST_QUEUE_NAME = "upgrade_test_post"

is_pre = PHASE == "pre"
is_post = PHASE == "post"

pre_only = pytest.mark.skipif(not is_pre, reason="pre-upgrade only")
post_only = pytest.mark.skipif(not is_post, reason="post-upgrade only")


def require_phase():
    if PHASE not in ("pre", "post"):
        pytest.fail("Set UPGRADE_PHASE=pre or UPGRADE_PHASE=post to run upgrade tests")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_connection():
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    conn = psycopg.Connection.connect(database_url, autocommit=True)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def send_message(conn, queue_name, msg, headers=None):
    with conn.cursor() as cur:
        if headers is not None:
            cur.execute(
                "SELECT * FROM pgmq.send(queue_name => %s::text, msg => %s::jsonb, headers => %s::jsonb)",
                (queue_name, json.dumps(msg), json.dumps(headers)),
            )
        else:
            cur.execute(
                "SELECT * FROM pgmq.send(queue_name => %s::text, msg => %s::jsonb)",
                (queue_name, json.dumps(msg)),
            )
        return cur.fetchone()[0]


def read_messages(conn, queue_name, vt=0, qty=10):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
            FROM pgmq.read(queue_name => %s::text, vt => %s::integer, qty => %s::integer)
            """,
            (queue_name, vt, qty),
        )
        return cur.fetchall()


def read_messages_pre(conn, queue_name, vt=0, qty=10):
    """Read messages using the pre-upgrade schema (no last_read_at or headers columns)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT msg_id, read_ct, enqueued_at, vt, message
            FROM pgmq.read(queue_name => %s::text, vt => %s::integer, qty => %s::integer)
            """,
            (queue_name, vt, qty),
        )
        return cur.fetchall()


def pop_message(conn, queue_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
            FROM pgmq.pop(queue_name => %s::text)
            """,
            (queue_name,),
        )
        return cur.fetchone()


def archive_message(conn, queue_name, msg_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pgmq.archive(queue_name => %s::text, msg_id => %s::bigint)",
            (queue_name, msg_id),
        )
        return cur.fetchone()[0]


def delete_message(conn, queue_name, msg_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pgmq.delete(queue_name => %s::text, msg_id => %s::bigint)",
            (queue_name, msg_id),
        )
        return cur.fetchone()[0]


def set_vt(conn, queue_name, msg_id, vt):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
            FROM pgmq.set_vt(queue_name => %s::text, msg_id => %s::bigint, vt => %s::integer)
            """,
            (queue_name, msg_id, vt),
        )
        return cur.fetchone()


def get_metrics(conn, queue_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec,
                   total_messages, scrape_time
            FROM pgmq.metrics(queue_name => %s::text)
            """,
            (queue_name,),
        )
        return cur.fetchone()


def list_queues(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT queue_name FROM pgmq.list_queues()")
        return [row[0] for row in cur.fetchall()]


def get_archive_count(conn, queue_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pgmq.a_%s" % queue_name  # noqa: S608
        )
        return cur.fetchone()[0]


def purge_queue(conn, queue_name):
    with conn.cursor() as cur:
        cur.execute("SELECT pgmq.purge_queue(queue_name => %s::text)", (queue_name,))
        return cur.fetchone()[0]


def get_pgmq_version(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'pgmq'")
        row = cur.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Pre-upgrade tests: set up state that must survive the upgrade
# ---------------------------------------------------------------------------


class TestPreUpgrade:
    """Create queues and seed data before the extension is upgraded."""

    @pre_only
    def test_phase_is_set(self):
        require_phase()

    @pre_only
    def test_create_queues(self, db_connection):
        """Create the queues that will be tested post-upgrade."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.create(queue_name => %s::text)", (QUEUE_NAME,))
            cur.execute(
                "SELECT pgmq.create_unlogged(queue_name => %s::text)", (QUEUE_UNLOGGED,)
            )
            cur.execute(
                "SELECT pgmq.create_partitioned(queue_name => %s::text, partition_interval => %s::text, retention_interval => %s::text)",
                (QUEUE_PARTITIONED, "10 seconds", "60 seconds"),
            )

        queues = list_queues(db_connection)
        assert QUEUE_NAME in queues
        assert QUEUE_UNLOGGED in queues
        assert QUEUE_PARTITIONED in queues

    @pre_only
    def test_send_messages(self, db_connection):
        """Send messages that should be readable after upgrade."""
        # Message that will stay in the queue (readable post-upgrade)
        msg_id_1 = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "pre", "index": 1, "purpose": "survive_upgrade"},
        )
        assert msg_id_1 is not None

        # Message that will be archived pre-upgrade
        msg_id_2 = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "pre", "index": 2, "purpose": "to_archive"},
        )
        assert archive_message(db_connection, QUEUE_NAME, msg_id_2)

        # Message that will be deleted pre-upgrade
        msg_id_3 = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "pre", "index": 3, "purpose": "to_delete"},
        )
        assert delete_message(db_connection, QUEUE_NAME, msg_id_3)

        # Message on the unlogged queue
        msg_id_4 = send_message(
            db_connection,
            QUEUE_UNLOGGED,
            {"phase": "pre", "index": 4, "purpose": "unlogged_survive"},
        )
        assert msg_id_4 is not None

        # Message on the partitioned queue
        msg_id_5 = send_message(
            db_connection,
            QUEUE_PARTITIONED,
            {"phase": "pre", "index": 5, "purpose": "partitioned_survive"},
        )
        assert msg_id_5 is not None

    @pre_only
    def test_send_batch(self, db_connection):
        """Send a batch of messages that should be readable after upgrade."""
        msgs = [json.dumps({"phase": "pre", "batch": True, "i": i}) for i in range(5)]
        with db_connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM pgmq.send_batch(queue_name => %s::text, msgs => %s::jsonb[])",
                (QUEUE_NAME, msgs),
            )
            ids = [row[0] for row in cur.fetchall()]
        assert len(ids) == 5

    @pre_only
    def test_verify_pre_state(self, db_connection):
        """Verify everything looks correct before upgrade."""
        metrics = get_metrics(db_connection, QUEUE_NAME)
        assert metrics[0] == QUEUE_NAME
        # 1 individual + 5 batch = 6 messages in queue
        assert metrics[1] == 6, f"Expected 6 messages, got {metrics[1]}"
        # 1 archived message
        assert get_archive_count(db_connection, QUEUE_NAME) == 1

        # Unlogged queue has its seeded message
        unlogged_msgs = read_messages_pre(db_connection, QUEUE_UNLOGGED, vt=0, qty=10)
        assert len(unlogged_msgs) == 1, (
            f"Expected 1 message in unlogged queue, got {len(unlogged_msgs)}"
        )

        # Partitioned queue has its seeded message
        # msg_id=0, read_ct=1, enqueued_at=2, vt=3, message=4
        partitioned_msgs = read_messages_pre(
            db_connection, QUEUE_PARTITIONED, vt=0, qty=10
        )
        assert len(partitioned_msgs) == 1, (
            f"Expected 1 message in partitioned queue, got {len(partitioned_msgs)}"
        )
        assert partitioned_msgs[0][4].get("purpose") == "partitioned_survive", (
            "Partitioned queue message has unexpected content"
        )
        partitioned_metrics = get_metrics(db_connection, QUEUE_PARTITIONED)
        assert partitioned_metrics[0] == QUEUE_PARTITIONED
        assert partitioned_metrics[1] == 1, (
            f"Expected 1 message in partitioned metrics, got {partitioned_metrics[1]}"
        )

    @pre_only
    def test_record_version(self, db_connection):
        """Log the pre-upgrade version for debugging."""
        version = get_pgmq_version(db_connection)
        assert version is not None
        print(f"Pre-upgrade pgmq version: {version}")


# ---------------------------------------------------------------------------
# Post-upgrade tests: verify state survived and operations work
# ---------------------------------------------------------------------------


class TestPostUpgradeStateIntact:
    """Verify that pre-upgrade state survived the extension upgrade."""

    @post_only
    def test_phase_is_set(self):
        require_phase()

    @post_only
    def test_version_changed(self, db_connection):
        """Log the post-upgrade version for debugging."""
        version = get_pgmq_version(db_connection)
        assert version is not None
        print(f"Post-upgrade pgmq version: {version}")

    @post_only
    def test_queues_still_exist(self, db_connection):
        """Queues created pre-upgrade must still be listed."""
        queues = list_queues(db_connection)
        assert QUEUE_NAME in queues, f"{QUEUE_NAME} missing after upgrade"
        assert QUEUE_UNLOGGED in queues, f"{QUEUE_UNLOGGED} missing after upgrade"

    @post_only
    def test_messages_survive(self, db_connection):
        """Messages sent pre-upgrade are still in the queue."""
        msgs = read_messages(db_connection, QUEUE_NAME, vt=0, qty=100)
        assert len(msgs) == 6, (
            f"Expected 6 messages to survive upgrade, got {len(msgs)}"
        )

    @post_only
    def test_message_content_intact(self, db_connection):
        """Message bodies are unchanged after upgrade."""
        msgs = read_messages(db_connection, QUEUE_NAME, vt=0, qty=100)

        # Find the message that was sent pre-upgrade to survive the upgrade
        # msg_id=0, read_ct=1, enqueued_at=2, last_read_at=3, vt=4, message=5, headers=6
        survive_msg = None
        for m in msgs:
            if m[5].get("purpose") == "survive_upgrade":
                survive_msg = m
                break

        assert survive_msg is not None, "Pre-upgrade survive message not found"
        assert survive_msg[5]["phase"] == "pre"
        assert survive_msg[5]["index"] == 1

    @post_only
    def test_archive_survives(self, db_connection):
        """Archived messages are still in the archive table."""
        count = get_archive_count(db_connection, QUEUE_NAME)
        assert count == 1, f"Expected 1 archived message after upgrade, got {count}"

    @post_only
    def test_metrics_work(self, db_connection):
        """pgmq.metrics() works on pre-upgrade queues."""
        metrics = get_metrics(db_connection, QUEUE_NAME)
        assert metrics[0] == QUEUE_NAME
        assert metrics[1] == 6  # queue_length

    @post_only
    def test_unlogged_queue_survives(self, db_connection):
        """Unlogged queue and its messages survive upgrade."""
        queues = list_queues(db_connection)
        assert QUEUE_UNLOGGED in queues
        msgs = read_messages(db_connection, QUEUE_UNLOGGED, vt=0, qty=10)
        assert len(msgs) >= 1


class TestPostUpgradeOperations:
    """Verify that all pgmq operations work on pre-upgrade queues."""

    @post_only
    def test_send_new_message(self, db_connection):
        """Can send new messages to a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "new_message"},
        )
        assert msg_id is not None

    @post_only
    def test_send_with_headers(self, db_connection):
        """Can send messages with headers to a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "new_with_headers"},
            headers={"x-added": "post-upgrade"},
        )
        assert msg_id is not None

    @post_only
    def test_read_messages(self, db_connection):
        """Can read messages from a pre-upgrade queue."""
        msgs = read_messages(db_connection, QUEUE_NAME, vt=0, qty=100)
        assert len(msgs) > 0

    @post_only
    def test_delete_message(self, db_connection):
        """Can delete a message from a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "to_delete"},
        )
        assert delete_message(db_connection, QUEUE_NAME, msg_id)

    @post_only
    def test_archive_message(self, db_connection):
        """Can archive a message from a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "to_archive"},
        )
        assert archive_message(db_connection, QUEUE_NAME, msg_id)
        # Now 2 archived (1 pre + 1 post)
        assert get_archive_count(db_connection, QUEUE_NAME) >= 2

    @post_only
    def test_pop_message(self, db_connection):
        """Can pop a message from a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "to_pop"},
        )
        assert msg_id is not None
        result = pop_message(db_connection, QUEUE_NAME)
        assert result is not None

    @post_only
    def test_set_vt(self, db_connection):
        """Can adjust visibility timeout on a pre-upgrade queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "set_vt_test"},
        )
        result = set_vt(db_connection, QUEUE_NAME, msg_id, 60)
        assert result is not None
        assert result[0] == msg_id  # msg_id is column 0
        # Clean up
        delete_message(db_connection, QUEUE_NAME, msg_id)

    @post_only
    def test_send_batch(self, db_connection):
        """Can send a batch of messages to a pre-upgrade queue."""
        msgs = [json.dumps({"phase": "post", "batch": True, "i": i}) for i in range(3)]
        with db_connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM pgmq.send_batch(queue_name => %s::text, msgs => %s::jsonb[])",
                (QUEUE_NAME, msgs),
            )
            ids = [row[0] for row in cur.fetchall()]
        assert len(ids) == 3

    @post_only
    def test_purge_queue(self, db_connection):
        """Can purge a pre-upgrade queue."""
        # Send a message to ensure there's something to purge
        send_message(
            db_connection,
            QUEUE_NAME,
            {"phase": "post", "purpose": "to_purge"},
        )
        count = purge_queue(db_connection, QUEUE_NAME)
        assert count > 0

        msgs = read_messages(db_connection, QUEUE_NAME, vt=0, qty=10)
        assert len(msgs) == 0

    @post_only
    def test_create_new_queue_post_upgrade(self, db_connection):
        """Can create a brand-new queue after upgrade."""
        try:
            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.create(queue_name => %s::text)", (POST_QUEUE_NAME,)
                )

            queues = list_queues(db_connection)
            assert POST_QUEUE_NAME in queues

            msg_id = send_message(
                db_connection,
                POST_QUEUE_NAME,
                {"phase": "post", "purpose": "new_queue_test"},
            )
            msgs = read_messages(db_connection, POST_QUEUE_NAME, vt=0, qty=1)
            assert len(msgs) == 1
            assert msgs[0][5]["purpose"] == "new_queue_test"  # message is column 5
        finally:
            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.drop_queue(queue_name => %s::text)", (POST_QUEUE_NAME,)
                )

    @post_only
    def test_drop_pre_upgrade_queue(self, db_connection):
        """Can drop queues that were created pre-upgrade. Run last."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.drop_queue(queue_name => %s::text)", (QUEUE_NAME,))
            result = cur.fetchone()[0]
            assert result is True

            cur.execute(
                "SELECT pgmq.drop_queue(queue_name => %s::text)", (QUEUE_UNLOGGED,)
            )
            result = cur.fetchone()[0]
            assert result is True

        queues = list_queues(db_connection)
        assert QUEUE_NAME not in queues
        assert QUEUE_UNLOGGED not in queues


class TestPostUpgradePartitionedQueue:
    """Verify partitioned queues created pre-upgrade survive and work after upgrade."""

    @post_only
    def test_partitioned_queue_exists(self, db_connection):
        """Partitioned queue created pre-upgrade is still listed."""
        queues = list_queues(db_connection)
        assert QUEUE_PARTITIONED in queues, f"{QUEUE_PARTITIONED} missing after upgrade"

    @post_only
    def test_partitioned_messages_survive(self, db_connection):
        """Messages sent to partitioned queue pre-upgrade are still readable."""
        msgs = read_messages(db_connection, QUEUE_PARTITIONED, vt=0, qty=10)
        assert len(msgs) >= 1
        survive_msg = next(
            (m for m in msgs if m[5].get("purpose") == "partitioned_survive"), None
        )
        assert survive_msg is not None, "Pre-upgrade partitioned message not found"
        assert survive_msg[5]["phase"] == "pre"

    @post_only
    def test_partitioned_send_and_read(self, db_connection):
        """Can send and read messages on a pre-upgrade partitioned queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_PARTITIONED,
            {"phase": "post", "purpose": "partitioned_new"},
        )
        assert msg_id is not None
        msgs = read_messages(db_connection, QUEUE_PARTITIONED, vt=0, qty=10)
        assert any(m[5].get("purpose") == "partitioned_new" for m in msgs)

    @post_only
    def test_partitioned_delete(self, db_connection):
        """Can delete messages from a pre-upgrade partitioned queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_PARTITIONED,
            {"phase": "post", "purpose": "partitioned_delete"},
        )
        assert delete_message(db_connection, QUEUE_PARTITIONED, msg_id)

    @post_only
    def test_partitioned_archive(self, db_connection):
        """Can archive messages from a pre-upgrade partitioned queue."""
        msg_id = send_message(
            db_connection,
            QUEUE_PARTITIONED,
            {"phase": "post", "purpose": "partitioned_archive"},
        )
        assert archive_message(db_connection, QUEUE_PARTITIONED, msg_id)

    @post_only
    def test_partitioned_metrics(self, db_connection):
        """pgmq.metrics() works on a pre-upgrade partitioned queue."""
        metrics = get_metrics(db_connection, QUEUE_PARTITIONED)
        assert metrics[0] == QUEUE_PARTITIONED

    @post_only
    def test_drop_partitioned_queue(self, db_connection):
        """Can drop the partitioned queue created pre-upgrade. Run last."""
        with db_connection.cursor() as cur:
            cur.execute(
                "SELECT pgmq.drop_queue(queue_name => %s::text, partitioned => %s::boolean)",
                (QUEUE_PARTITIONED, True),
            )
            result = cur.fetchone()[0]
            assert result is True

        queues = list_queues(db_connection)
        assert QUEUE_PARTITIONED not in queues
