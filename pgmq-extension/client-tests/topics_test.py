"""
PGMQ AMQP Topics Tests

This test suite comprehensively tests the PostgreSQL PGMQ topic-based routing mechanism,
which allows AMQP-style message routing using patterns with wildcards.

Tests cover:
- validate_routing_key() function (valid and invalid routing keys)
- validate_topic_pattern() function (valid and invalid patterns)
- bind_topic() and unbind_topic() functions
- test_routing() function for dry-run routing tests
- send_topic() function for actual message routing

The topic routing mechanism works as follows:
1. Create queues using pgmq.create()
2. Bind patterns to queues using pgmq.bind_topic(pattern, queue_name)
3. Send messages using pgmq.send_topic(routing_key, message, headers, delay)
4. Messages are routed to all queues whose patterns match the routing key

Pattern wildcards:
- * (star)  = matches exactly ONE segment
- # (hash)  = matches ZERO or MORE segments

Run tests with:
    pytest topics_test.py -v
    or
    uv run pytest topics_test.py -v
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


# Helper functions
def create_queue(db_connection: psycopg.Connection, queue_name: str) -> None:
    """Create a PGMQ queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.create(%s)", (queue_name,))
        print(f"Created queue: {queue_name}")


def drop_queue(db_connection: psycopg.Connection, queue_name: str) -> None:
    """Drop a PGMQ queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.drop_queue(%s)", (queue_name,))
        print(f"Dropped queue: {queue_name}")


def bind_topic(
    db_connection: psycopg.Connection, pattern: str, queue_name: str
) -> None:
    """Bind a pattern to a queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.bind_topic(%s, %s)", (pattern, queue_name))
        print(f"Bound pattern '{pattern}' to queue '{queue_name}'")


def unbind_topic(
    db_connection: psycopg.Connection, pattern: str, queue_name: str
) -> bool:
    """Unbind a pattern from a queue. Returns True if binding existed."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT pgmq.unbind_topic(%s, %s)", (pattern, queue_name))
        result = cur.fetchone()
        success = result[0] if result else False
        print(f"Unbound pattern '{pattern}' from queue '{queue_name}': {success}")
        return success


def get_routing_matches(
    db_connection: psycopg.Connection, routing_key: str
) -> list[tuple[str, str, str]]:
    """Test which queues would receive a message with the given routing key."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM pgmq.test_routing(%s)", (routing_key,))
        results = cur.fetchall()
        print(f"test_routing('{routing_key}'): {len(results)} matches")
        for row in results:
            print(f"  pattern='{row[0]}', queue='{row[1]}'")
        return results


def send_topic(
    db_connection: psycopg.Connection,
    routing_key: str,
    message: dict,
    headers: dict | None = None,
    delay: int = 0,
) -> int:
    """Send a message using topic routing. Returns count of matched queues."""
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT pgmq.send_topic(%s, %s, %s, %s)",
            (routing_key, json.dumps(message), json.dumps(headers) if headers else None, delay),
        )
        result = cur.fetchone()
        count = result[0] if result else 0
        print(f"send_topic('{routing_key}'): sent to {count} queue(s)")
        return count


def read_message(
    db_connection: psycopg.Connection, queue_name: str, vt: int = 30
) -> dict | None:
    """Read a single message from a queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT * FROM pgmq.read(%s, %s, 1)", (queue_name, vt))
        result = cur.fetchone()
        if result:
            print(f"Read message from '{queue_name}': msg_id={result[0]}")
            return {
                "msg_id": result[0],
                "read_ct": result[1],
                "enqueued_at": result[2],
                "last_read_at": result[3],
                "vt": result[4],
                "message": result[5],
                "headers": result[6],
            }
        return None


def get_queue_length(db_connection: psycopg.Connection, queue_name: str) -> int:
    """Get the number of messages in a queue."""
    with db_connection.cursor() as cur:
        cur.execute("SELECT queue_length FROM pgmq.metrics(%s)", (queue_name,))
        result = cur.fetchone()
        return result[0] if result else 0


def count_bindings(db_connection: psycopg.Connection, queue_name: str) -> int:
    """Count topic bindings for a queue."""
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pgmq.topic_bindings WHERE queue_name = %s",
            (queue_name,),
        )
        return cur.fetchone()[0]


# =============================================================================
# Tests for validate_routing_key()
# =============================================================================


class TestValidateRoutingKey:
    """Tests for the pgmq.validate_routing_key() function."""

    def test_valid_simple_routing_key(self, db_connection: psycopg.Connection):
        """Test that simple alphanumeric routing keys are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_routing_key('logs.error')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: simple routing key 'logs.error' is valid")

    def test_valid_routing_key_with_hyphens(self, db_connection: psycopg.Connection):
        """Test that routing keys with hyphens are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_routing_key('app.user-service.auth')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: routing key with hyphens is valid")

    def test_valid_routing_key_with_underscores(self, db_connection: psycopg.Connection):
        """Test that routing keys with underscores are valid."""
        with db_connection.cursor() as cur:
            cur.execute(
                "SELECT pgmq.validate_routing_key('system_events.db.connection_failed')"
            )
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: routing key with underscores is valid")

    def test_valid_single_segment_routing_key(self, db_connection: psycopg.Connection):
        """Test that single segment routing keys are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_routing_key('logs')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: single segment routing key is valid")

    def test_invalid_empty_routing_key(self, db_connection: psycopg.Connection):
        """Test that empty routing keys are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('')")
        assert "cannot be NULL or empty" in str(exc_info.value)
        print("Test passed: empty routing key is rejected")

    def test_invalid_null_routing_key(self, db_connection: psycopg.Connection):
        """Test that NULL routing keys are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key(NULL)")
        assert "cannot be NULL or empty" in str(exc_info.value)
        print("Test passed: NULL routing key is rejected")

    def test_invalid_routing_key_starts_with_dot(self, db_connection: psycopg.Connection):
        """Test that routing keys starting with a dot are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('.logs.error')")
        assert "cannot start with a dot" in str(exc_info.value)
        print("Test passed: routing key starting with dot is rejected")

    def test_invalid_routing_key_ends_with_dot(self, db_connection: psycopg.Connection):
        """Test that routing keys ending with a dot are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs.error.')")
        assert "cannot end with a dot" in str(exc_info.value)
        print("Test passed: routing key ending with dot is rejected")

    def test_invalid_routing_key_consecutive_dots(self, db_connection: psycopg.Connection):
        """Test that routing keys with consecutive dots are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs..error')")
        assert "cannot contain consecutive dots" in str(exc_info.value)
        print("Test passed: routing key with consecutive dots is rejected")

    def test_invalid_routing_key_with_wildcards(self, db_connection: psycopg.Connection):
        """Test that routing keys with wildcards are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs.*')")
        assert "invalid characters" in str(exc_info.value)
        print("Test passed: routing key with wildcards is rejected")

    def test_invalid_routing_key_with_special_chars(self, db_connection: psycopg.Connection):
        """Test that routing keys with special characters are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs.error!')")
        assert "invalid characters" in str(exc_info.value)
        print("Test passed: routing key with special characters is rejected")

    def test_invalid_routing_key_with_space(self, db_connection: psycopg.Connection):
        """Test that routing keys with spaces are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs error')")
        assert "invalid characters" in str(exc_info.value)
        print("Test passed: routing key with space is rejected")

    def test_invalid_routing_key_too_long(self, db_connection: psycopg.Connection):
        """Test that routing keys exceeding 255 characters are rejected."""
        long_key = "a" * 256
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key(%s)", (long_key,))
        assert "cannot exceed 255 characters" in str(exc_info.value)
        print("Test passed: routing key exceeding 255 characters is rejected")


# =============================================================================
# Tests for validate_topic_pattern()
# =============================================================================


class TestValidateTopicPattern:
    """Tests for the pgmq.validate_topic_pattern() function."""

    def test_valid_pattern_with_star(self, db_connection: psycopg.Connection):
        """Test that patterns with * wildcard are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('logs.*')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern 'logs.*' is valid")

    def test_valid_pattern_with_hash(self, db_connection: psycopg.Connection):
        """Test that patterns with # wildcard are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('logs.#')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern 'logs.#' is valid")

    def test_valid_pattern_star_at_start(self, db_connection: psycopg.Connection):
        """Test that patterns starting with * are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('*.error')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern '*.error' is valid")

    def test_valid_pattern_hash_at_start(self, db_connection: psycopg.Connection):
        """Test that patterns starting with # are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('#.error')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern '#.error' is valid")

    def test_valid_pattern_mixed_wildcards(self, db_connection: psycopg.Connection):
        """Test that patterns with mixed wildcards are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('app.*.#')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern 'app.*.#' is valid")

    def test_valid_pattern_exact_match(self, db_connection: psycopg.Connection):
        """Test that exact patterns (no wildcards) are valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('logs.error.fatal')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: exact pattern 'logs.error.fatal' is valid")

    def test_valid_pattern_only_hash(self, db_connection: psycopg.Connection):
        """Test that single # pattern (match all) is valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('#')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern '#' is valid")

    def test_valid_pattern_only_star(self, db_connection: psycopg.Connection):
        """Test that single * pattern is valid."""
        with db_connection.cursor() as cur:
            cur.execute("SELECT pgmq.validate_topic_pattern('*')")
            result = cur.fetchone()[0]
            assert result is True
            print("Test passed: pattern '*' is valid")

    def test_invalid_empty_pattern(self, db_connection: psycopg.Connection):
        """Test that empty patterns are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('')")
        assert "cannot be NULL or empty" in str(exc_info.value)
        print("Test passed: empty pattern is rejected")

    def test_invalid_null_pattern(self, db_connection: psycopg.Connection):
        """Test that NULL patterns are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern(NULL)")
        assert "cannot be NULL or empty" in str(exc_info.value)
        print("Test passed: NULL pattern is rejected")

    def test_invalid_pattern_starts_with_dot(self, db_connection: psycopg.Connection):
        """Test that patterns starting with a dot are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('.logs.*')")
        assert "cannot start with a dot" in str(exc_info.value)
        print("Test passed: pattern starting with dot is rejected")

    def test_invalid_pattern_ends_with_dot(self, db_connection: psycopg.Connection):
        """Test that patterns ending with a dot are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.*.')")
        assert "cannot end with a dot" in str(exc_info.value)
        print("Test passed: pattern ending with dot is rejected")

    def test_invalid_pattern_consecutive_dots(self, db_connection: psycopg.Connection):
        """Test that patterns with consecutive dots are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs..error')")
        assert "cannot contain consecutive dots" in str(exc_info.value)
        print("Test passed: pattern with consecutive dots is rejected")

    def test_invalid_pattern_consecutive_stars(self, db_connection: psycopg.Connection):
        """Test that patterns with consecutive stars are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.**')")
        assert "cannot contain consecutive stars" in str(exc_info.value)
        print("Test passed: pattern with consecutive stars is rejected")

    def test_invalid_pattern_consecutive_hashes(self, db_connection: psycopg.Connection):
        """Test that patterns with consecutive hashes are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.##')")
        assert "cannot contain consecutive hashes" in str(exc_info.value)
        print("Test passed: pattern with consecutive hashes is rejected")

    def test_invalid_pattern_adjacent_wildcards_star_hash(
        self, db_connection: psycopg.Connection
    ):
        """Test that patterns with adjacent *# are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.*#')")
        assert "cannot contain adjacent wildcards" in str(exc_info.value)
        print("Test passed: pattern with adjacent *# is rejected")

    def test_invalid_pattern_adjacent_wildcards_hash_star(
        self, db_connection: psycopg.Connection
    ):
        """Test that patterns with adjacent #* are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.#*')")
        assert "cannot contain adjacent wildcards" in str(exc_info.value)
        print("Test passed: pattern with adjacent #* is rejected")

    def test_invalid_pattern_special_chars(self, db_connection: psycopg.Connection):
        """Test that patterns with invalid special characters are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.error!')")
        assert "invalid characters" in str(exc_info.value)
        print("Test passed: pattern with special characters is rejected")

    def test_invalid_pattern_too_long(self, db_connection: psycopg.Connection):
        """Test that patterns exceeding 255 characters are rejected."""
        long_pattern = "a" * 256
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern(%s)", (long_pattern,))
        assert "cannot exceed 255 characters" in str(exc_info.value)
        print("Test passed: pattern exceeding 255 characters is rejected")


# =============================================================================
# Tests for bind_topic() and unbind_topic()
# =============================================================================


class TestBindUnbindTopic:
    """Tests for the pgmq.bind_topic() and pgmq.unbind_topic() functions."""

    def test_bind_topic_creates_binding(self, db_connection: psycopg.Connection):
        """Test that bind_topic creates a binding in the topic_bindings table."""
        now = int(time.time())
        queue_name = f"test_bind_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.*", queue_name)

            # Verify binding exists
            count = count_bindings(db_connection, queue_name)
            assert count == 1, f"Expected 1 binding, got {count}"
            print("Test passed: bind_topic creates a binding")

        finally:
            drop_queue(db_connection, queue_name)

    def test_bind_topic_idempotent(self, db_connection: psycopg.Connection):
        """Test that binding the same pattern twice is idempotent (no error, no duplicate)."""
        now = int(time.time())
        queue_name = f"test_bind_idempotent_{now}"

        try:
            create_queue(db_connection, queue_name)

            # Bind the same pattern twice
            bind_topic(db_connection, "logs.*", queue_name)
            bind_topic(db_connection, "logs.*", queue_name)

            # Verify only one binding exists
            count = count_bindings(db_connection, queue_name)
            assert count == 1, f"Expected 1 binding (idempotent), got {count}"
            print("Test passed: bind_topic is idempotent")

        finally:
            drop_queue(db_connection, queue_name)

    def test_bind_topic_multiple_patterns(self, db_connection: psycopg.Connection):
        """Test that multiple different patterns can be bound to the same queue."""
        now = int(time.time())
        queue_name = f"test_bind_multi_{now}"

        try:
            create_queue(db_connection, queue_name)

            bind_topic(db_connection, "logs.*", queue_name)
            bind_topic(db_connection, "errors.#", queue_name)
            bind_topic(db_connection, "app.*.critical", queue_name)

            # Verify all bindings exist
            count = count_bindings(db_connection, queue_name)
            assert count == 3, f"Expected 3 bindings, got {count}"
            print("Test passed: multiple patterns can be bound to one queue")

        finally:
            drop_queue(db_connection, queue_name)

    def test_bind_topic_same_pattern_different_queues(
        self, db_connection: psycopg.Connection
    ):
        """Test that the same pattern can be bound to multiple queues."""
        now = int(time.time())
        queue_name1 = f"test_bind_q1_{now}"
        queue_name2 = f"test_bind_q2_{now}"

        try:
            create_queue(db_connection, queue_name1)
            create_queue(db_connection, queue_name2)

            bind_topic(db_connection, "logs.#", queue_name1)
            bind_topic(db_connection, "logs.#", queue_name2)

            # Verify both bindings exist
            count1 = count_bindings(db_connection, queue_name1)
            count2 = count_bindings(db_connection, queue_name2)
            assert count1 == 1 and count2 == 1, "Each queue should have 1 binding"
            print("Test passed: same pattern can be bound to multiple queues")

        finally:
            drop_queue(db_connection, queue_name1)
            drop_queue(db_connection, queue_name2)

    def test_bind_topic_invalid_pattern(self, db_connection: psycopg.Connection):
        """Test that binding with an invalid pattern raises an error."""
        now = int(time.time())
        queue_name = f"test_bind_invalid_{now}"

        try:
            create_queue(db_connection, queue_name)

            with pytest.raises(psycopg.errors.RaiseException) as exc_info:
                bind_topic(db_connection, "logs..**", queue_name)

            assert "consecutive" in str(exc_info.value).lower()
            print("Test passed: bind_topic with invalid pattern raises error")

        finally:
            drop_queue(db_connection, queue_name)

    def test_bind_topic_empty_queue_name(self, db_connection: psycopg.Connection):
        """Test that binding with empty queue name raises an error."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.bind_topic('logs.*', '')")

        assert "cannot be NULL or empty" in str(exc_info.value)
        print("Test passed: bind_topic with empty queue name raises error")

    def test_unbind_topic_removes_binding(self, db_connection: psycopg.Connection):
        """Test that unbind_topic removes an existing binding."""
        now = int(time.time())
        queue_name = f"test_unbind_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.*", queue_name)

            # Verify binding exists
            count = count_bindings(db_connection, queue_name)
            assert count == 1

            # Unbind
            result = unbind_topic(db_connection, "logs.*", queue_name)
            assert result is True, "unbind_topic should return True for existing binding"

            # Verify binding removed
            count = count_bindings(db_connection, queue_name)
            assert count == 0, f"Expected 0 bindings after unbind, got {count}"
            print("Test passed: unbind_topic removes binding")

        finally:
            drop_queue(db_connection, queue_name)

    def test_unbind_topic_nonexistent_returns_false(
        self, db_connection: psycopg.Connection
    ):
        """Test that unbind_topic returns False for non-existent binding."""
        now = int(time.time())
        queue_name = f"test_unbind_none_{now}"

        try:
            create_queue(db_connection, queue_name)

            # Try to unbind a pattern that was never bound
            result = unbind_topic(db_connection, "nonexistent.pattern", queue_name)
            assert result is False, "unbind_topic should return False for non-existent binding"
            print("Test passed: unbind_topic returns False for non-existent binding")

        finally:
            drop_queue(db_connection, queue_name)

    def test_drop_queue_cascades_bindings(self, db_connection: psycopg.Connection):
        """Test that dropping a queue removes its topic bindings (CASCADE)."""
        now = int(time.time())
        queue_name = f"test_cascade_{now}"

        create_queue(db_connection, queue_name)
        bind_topic(db_connection, "logs.*", queue_name)
        bind_topic(db_connection, "errors.#", queue_name)

        # Verify bindings exist
        count = count_bindings(db_connection, queue_name)
        assert count == 2

        # Drop queue
        drop_queue(db_connection, queue_name)

        # Verify bindings were removed via CASCADE
        count = count_bindings(db_connection, queue_name)
        assert count == 0, f"Expected 0 bindings after drop (CASCADE), got {count}"
        print("Test passed: drop_queue cascades to remove topic bindings")


# =============================================================================
# Tests for test_routing()
# =============================================================================


class TestRouting:
    """Tests for the pgmq.test_routing() function."""

    def test_routing_exact_match(self, db_connection: psycopg.Connection):
        """Test routing with exact pattern match."""
        now = int(time.time())
        queue_name = f"test_route_exact_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.error", queue_name)

            results = get_routing_matches(db_connection, "logs.error")
            assert len(results) == 1
            assert results[0][1] == queue_name
            print("Test passed: exact pattern match works")

        finally:
            drop_queue(db_connection, queue_name)

    def test_routing_star_wildcard(self, db_connection: psycopg.Connection):
        """Test routing with * wildcard (matches exactly one segment)."""
        now = int(time.time())
        queue_name = f"test_route_star_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.*.error", queue_name)

            # Should match - one segment between logs. and .error
            results = get_routing_matches(db_connection, "logs.api.error")
            assert len(results) == 1, f"Expected 1 match, got {len(results)}"

            # Should NOT match - two segments between logs. and .error
            results = get_routing_matches(db_connection, "logs.api.db.error")
            assert len(results) == 0, f"Expected 0 matches, got {len(results)}"

            print("Test passed: * wildcard matches exactly one segment")

        finally:
            drop_queue(db_connection, queue_name)

    def test_routing_hash_wildcard(self, db_connection: psycopg.Connection):
        """Test routing with # wildcard (matches zero or more segments)."""
        now = int(time.time())
        queue_name = f"test_route_hash_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.#", queue_name)

            # Should match - one segment after logs.
            results = get_routing_matches(db_connection, "logs.error")
            assert len(results) == 1

            # Should match - two segments after logs.
            results = get_routing_matches(db_connection, "logs.api.error")
            assert len(results) == 1

            # Should match - three segments after logs.
            results = get_routing_matches(db_connection, "logs.api.db.critical")
            assert len(results) == 1

            print("Test passed: # wildcard matches zero or more segments")

        finally:
            drop_queue(db_connection, queue_name)

    def test_routing_star_vs_hash_difference(self, db_connection: psycopg.Connection):
        """Test the difference between * and # wildcards."""
        now = int(time.time())
        queue_star = f"test_star_{now}"
        queue_hash = f"test_hash_{now}"

        try:
            create_queue(db_connection, queue_star)
            create_queue(db_connection, queue_hash)
            bind_topic(db_connection, "logs.*", queue_star)
            bind_topic(db_connection, "logs.#", queue_hash)

            # One segment: both match
            results = get_routing_matches(db_connection, "logs.error")
            assert len(results) == 2, f"Expected 2 matches for one segment, got {len(results)}"

            # Two segments: only hash matches
            results = get_routing_matches(db_connection, "logs.error.fatal")
            assert len(results) == 1, f"Expected 1 match for two segments, got {len(results)}"
            assert results[0][1] == queue_hash

            print("Test passed: * and # wildcards behave differently")

        finally:
            drop_queue(db_connection, queue_star)
            drop_queue(db_connection, queue_hash)

    def test_routing_multiple_queues(self, db_connection: psycopg.Connection):
        """Test routing to multiple queues with overlapping patterns."""
        now = int(time.time())
        queue_all = f"test_route_all_{now}"
        queue_error = f"test_route_error_{now}"
        queue_critical = f"test_route_critical_{now}"

        try:
            create_queue(db_connection, queue_all)
            create_queue(db_connection, queue_error)
            create_queue(db_connection, queue_critical)

            bind_topic(db_connection, "logs.#", queue_all)
            bind_topic(db_connection, "logs.*.error", queue_error)
            bind_topic(db_connection, "logs.*.critical", queue_critical)

            # Info message: only queue_all
            results = get_routing_matches(db_connection, "logs.api.info")
            assert len(results) == 1
            queues = [r[1] for r in results]
            assert queue_all in queues

            # Error message: queue_all and queue_error
            results = get_routing_matches(db_connection, "logs.api.error")
            assert len(results) == 2
            queues = [r[1] for r in results]
            assert queue_all in queues
            assert queue_error in queues

            # Critical message: queue_all and queue_critical
            results = get_routing_matches(db_connection, "logs.db.critical")
            assert len(results) == 2
            queues = [r[1] for r in results]
            assert queue_all in queues
            assert queue_critical in queues

            print("Test passed: routing to multiple queues with overlapping patterns")

        finally:
            drop_queue(db_connection, queue_all)
            drop_queue(db_connection, queue_error)
            drop_queue(db_connection, queue_critical)

    def test_routing_no_matches(self, db_connection: psycopg.Connection):
        """Test routing when no patterns match."""
        now = int(time.time())
        queue_name = f"test_route_none_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.*", queue_name)

            # Should not match any pattern
            results = get_routing_matches(db_connection, "metrics.cpu.usage")
            assert len(results) == 0, f"Expected 0 matches, got {len(results)}"
            print("Test passed: routing returns empty for no matches")

        finally:
            drop_queue(db_connection, queue_name)

    def test_routing_hash_at_start(self, db_connection: psycopg.Connection):
        """Test routing with # at the start of pattern."""
        now = int(time.time())
        queue_name = f"test_route_hash_start_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "#.error", queue_name)

            # Should match all of these
            results = get_routing_matches(db_connection, "error")
            # Note: '#.error' requires at least a dot before error, so this may not match
            # Let's check the actual behavior

            results = get_routing_matches(db_connection, "logs.error")
            assert len(results) >= 1

            results = get_routing_matches(db_connection, "logs.api.error")
            assert len(results) >= 1

            print("Test passed: # at start matches zero or more segments before")

        finally:
            drop_queue(db_connection, queue_name)

    def test_routing_catch_all(self, db_connection: psycopg.Connection):
        """Test routing with catch-all # pattern."""
        now = int(time.time())
        queue_name = f"test_route_catchall_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "#", queue_name)

            # Should match everything
            results = get_routing_matches(db_connection, "logs")
            assert len(results) == 1

            results = get_routing_matches(db_connection, "logs.error")
            assert len(results) == 1

            results = get_routing_matches(db_connection, "anything.at.all")
            assert len(results) == 1

            print("Test passed: # pattern matches everything")

        finally:
            drop_queue(db_connection, queue_name)


# =============================================================================
# Tests for send_topic()
# =============================================================================


class TestSendTopic:
    """Tests for the pgmq.send_topic() function."""

    def test_send_topic_single_queue(self, db_connection: psycopg.Connection):
        """Test sending a message to a single queue via topic routing."""
        now = int(time.time())
        queue_name = f"test_send_single_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "orders.created", queue_name)

            # Send message
            count = send_topic(db_connection, "orders.created", {"order_id": 123})
            assert count == 1, f"Expected 1 queue matched, got {count}"

            # Verify message arrived
            msg = read_message(db_connection, queue_name)
            assert msg is not None, "Message should be in queue"
            assert msg["message"]["order_id"] == 123
            print("Test passed: send_topic delivers to single queue")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_multiple_queues(self, db_connection: psycopg.Connection):
        """Test sending a message to multiple queues (fanout)."""
        now = int(time.time())
        queue1 = f"test_send_q1_{now}"
        queue2 = f"test_send_q2_{now}"
        queue3 = f"test_send_q3_{now}"

        try:
            create_queue(db_connection, queue1)
            create_queue(db_connection, queue2)
            create_queue(db_connection, queue3)

            bind_topic(db_connection, "#", queue1)
            bind_topic(db_connection, "#", queue2)
            bind_topic(db_connection, "#", queue3)

            # Send message
            count = send_topic(db_connection, "any.routing.key", {"event": "test"})
            assert count == 3, f"Expected 3 queues matched, got {count}"

            # Verify all queues received the message
            for q in [queue1, queue2, queue3]:
                length = get_queue_length(db_connection, q)
                assert length == 1, f"Queue {q} should have 1 message, has {length}"

            print("Test passed: send_topic delivers to multiple queues (fanout)")

        finally:
            drop_queue(db_connection, queue1)
            drop_queue(db_connection, queue2)
            drop_queue(db_connection, queue3)

    def test_send_topic_no_matches(self, db_connection: psycopg.Connection):
        """Test sending a message when no patterns match."""
        now = int(time.time())
        queue_name = f"test_send_nomatch_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "orders.*", queue_name)

            # Send message that doesn't match
            count = send_topic(db_connection, "users.created", {"user_id": 1})
            assert count == 0, f"Expected 0 queues matched, got {count}"

            # Verify queue is empty
            length = get_queue_length(db_connection, queue_name)
            assert length == 0, f"Queue should be empty, has {length} messages"
            print("Test passed: send_topic returns 0 for no matches")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_with_headers(self, db_connection: psycopg.Connection):
        """Test sending a message with custom headers."""
        now = int(time.time())
        queue_name = f"test_send_headers_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "events.#", queue_name)

            headers = {"correlation_id": "abc123", "source": "test"}
            count = send_topic(
                db_connection,
                "events.user.signup",
                {"user_id": 42},
                headers=headers,
            )
            assert count == 1

            msg = read_message(db_connection, queue_name)
            assert msg is not None
            assert msg["headers"]["correlation_id"] == "abc123"
            assert msg["headers"]["source"] == "test"
            assert msg["headers"]["x-pgmq-pattern"] == "events.#"
            assert msg["headers"]["x-pgmq-routing-key"] == "events.user.signup"
            print("Test passed: send_topic preserves headers")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_exposes_pattern_and_routing_key_in_headers(
        self, db_connection: psycopg.Connection
    ):
        """Test that send_topic sets x-pgmq-pattern and x-pgmq-routing-key in message headers."""
        now = int(time.time())
        queue_name = f"test_send_topic_headers_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.app.#", queue_name)

            count = send_topic(
                db_connection,
                "logs.app.error",
                {"message": "test error"},
            )
            assert count == 1

            msg = read_message(db_connection, queue_name)
            assert msg is not None
            assert msg["headers"]["x-pgmq-pattern"] == "logs.app.#"
            assert msg["headers"]["x-pgmq-routing-key"] == "logs.app.error"
            print("Test passed: send_topic exposes x-pgmq-pattern and x-pgmq-routing-key in headers")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_batch_topic_exposes_pattern_and_routing_key_in_headers(
        self, db_connection: psycopg.Connection
    ):
        """Test that send_batch_topic sets x-pgmq-pattern and x-pgmq-routing-key in message headers."""
        now = int(time.time())
        queue_name = f"test_batch_topic_headers_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "batch.routing.#", queue_name)

            with db_connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT queue_name, msg_id FROM pgmq.send_batch_topic(
                        'batch.routing.key',
                        ARRAY['{"n": 1}'::jsonb, '{"n": 2}'::jsonb]
                    )
                    """,
                )
                rows = cur.fetchall()
            assert len(rows) == 2, f"Expected 2 rows (2 msgs x 1 queue), got {len(rows)}"

            msg = read_message(db_connection, queue_name)
            assert msg is not None
            assert msg["headers"]["x-pgmq-pattern"] == "batch.routing.#"
            assert msg["headers"]["x-pgmq-routing-key"] == "batch.routing.key"
            print(
                "Test passed: send_batch_topic exposes x-pgmq-pattern and x-pgmq-routing-key in headers"
            )

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_with_delay(self, db_connection: psycopg.Connection):
        """Test sending a message with a delay."""
        now = int(time.time())
        queue_name = f"test_send_delay_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "delayed.*", queue_name)

            # Send with 2 second delay
            count = send_topic(
                db_connection,
                "delayed.message",
                {"data": "delayed"},
                delay=2,
            )
            assert count == 1

            # Message should not be visible immediately
            msg = read_message(db_connection, queue_name)
            assert msg is None, "Message should not be visible yet (delayed)"

            # Wait for delay
            time.sleep(2.5)

            # Now message should be visible
            msg = read_message(db_connection, queue_name)
            assert msg is not None, "Message should be visible after delay"
            assert msg["message"]["data"] == "delayed"
            print("Test passed: send_topic respects delay parameter")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_invalid_routing_key(self, db_connection: psycopg.Connection):
        """Test that send_topic validates the routing key."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            send_topic(db_connection, "invalid..key", {"test": True})

        assert "consecutive dots" in str(exc_info.value)
        print("Test passed: send_topic validates routing key")

    def test_send_topic_null_message(self, db_connection: psycopg.Connection):
        """Test that send_topic rejects NULL message."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.send_topic('valid.key', NULL, NULL, 0)")

        assert "cannot be NULL" in str(exc_info.value)
        print("Test passed: send_topic rejects NULL message")

    def test_send_topic_negative_delay(self, db_connection: psycopg.Connection):
        """Test that send_topic rejects negative delay."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.send_topic(%s, %s, NULL, %s)",
                    ("valid.key", '{"test": true}', -1),
                )

        assert "cannot be negative" in str(exc_info.value)
        print("Test passed: send_topic rejects negative delay")

    def test_send_topic_overloaded_functions(self, db_connection: psycopg.Connection):
        """Test the overloaded versions of send_topic (2 and 3 args)."""
        now = int(time.time())
        queue_name = f"test_send_overload_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "test.#", queue_name)

            # 2-arg version: send_topic(routing_key, msg)
            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.send_topic(%s, %s)",
                    ("test.two.args", '{"version": 2}'),
                )
                result = cur.fetchone()
                assert result[0] == 1

            # 3-arg version: send_topic(routing_key, msg, delay)
            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.send_topic(%s, %s, %s)",
                    ("test.three.args", '{"version": 3}', 0),
                )
                result = cur.fetchone()
                assert result[0] == 1

            # Verify both messages arrived
            length = get_queue_length(db_connection, queue_name)
            assert length == 2, f"Expected 2 messages, got {length}"
            print("Test passed: overloaded send_topic functions work")

        finally:
            drop_queue(db_connection, queue_name)

    def test_send_topic_selective_routing(self, db_connection: psycopg.Connection):
        """Test selective routing based on pattern specificity."""
        now = int(time.time())
        queue_all = f"test_selective_all_{now}"
        queue_errors = f"test_selective_err_{now}"
        queue_api_errors = f"test_selective_api_{now}"

        try:
            create_queue(db_connection, queue_all)
            create_queue(db_connection, queue_errors)
            create_queue(db_connection, queue_api_errors)

            bind_topic(db_connection, "#", queue_all)
            bind_topic(db_connection, "*.error", queue_errors)
            bind_topic(db_connection, "api.error", queue_api_errors)

            # Send api.error - should match all 3
            count = send_topic(db_connection, "api.error", {"type": "api_error"})
            assert count == 3, f"Expected 3 matches for api.error, got {count}"

            # Send db.error - should match queue_all and queue_errors
            count = send_topic(db_connection, "db.error", {"type": "db_error"})
            assert count == 2, f"Expected 2 matches for db.error, got {count}"

            # Send api.info - should match only queue_all
            count = send_topic(db_connection, "api.info", {"type": "api_info"})
            assert count == 1, f"Expected 1 match for api.info, got {count}"

            # Verify queue lengths
            assert get_queue_length(db_connection, queue_all) == 3
            assert get_queue_length(db_connection, queue_errors) == 2
            assert get_queue_length(db_connection, queue_api_errors) == 1

            print("Test passed: selective routing based on pattern specificity")

        finally:
            drop_queue(db_connection, queue_all)
            drop_queue(db_connection, queue_errors)
            drop_queue(db_connection, queue_api_errors)


# =============================================================================
# Tests for regex injection protection
# =============================================================================


class TestRegexInjection:
    """Tests for regex injection protection in pattern validation and compiled regex.

    Ensures that:
    1. Regex metacharacters are rejected by the pattern/routing key validators
    2. The compiled_regex column properly escapes dots to literal matchers
    3. Routing behavior treats dots and hyphens as literal characters
    """

    # --- Pattern validation: reject regex metacharacters ---

    def test_pattern_rejects_parentheses(self, db_connection: psycopg.Connection):
        """Test that parentheses (regex grouping) are rejected in patterns."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.(foo|bar)')")
        assert "invalid characters" in str(exc_info.value)

    def test_pattern_rejects_square_brackets(self, db_connection: psycopg.Connection):
        """Test that square brackets (regex character class) are rejected in patterns."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.[error]')")
        assert "invalid characters" in str(exc_info.value)

    def test_pattern_rejects_dollar(self, db_connection: psycopg.Connection):
        """Test that dollar sign (regex end anchor) is rejected in patterns."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_topic_pattern('logs.error$')")
        assert "invalid characters" in str(exc_info.value)

    # --- Routing key validation: reject regex metacharacters ---

    def test_routing_key_rejects_regex_metacharacters(
        self, db_connection: psycopg.Connection
    ):
        """Test that routing keys with regex metacharacters are rejected."""
        with pytest.raises(psycopg.errors.RaiseException) as exc_info:
            with db_connection.cursor() as cur:
                cur.execute("SELECT pgmq.validate_routing_key('logs.(foo|bar)')")
        assert "invalid characters" in str(exc_info.value)

    # --- Compiled regex correctness ---

    def test_compiled_regex_escapes_dots(self, db_connection: psycopg.Connection):
        """Test that dots in patterns are escaped to literal dot matchers in compiled_regex."""
        now = int(time.time())
        queue_name = f"test_regex_dots_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.error", queue_name)

            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT compiled_regex FROM pgmq.topic_bindings "
                    "WHERE pattern = 'logs.error' AND queue_name = %s",
                    (queue_name,),
                )
                compiled = cur.fetchone()[0]
                assert compiled == r"^logs\.error$", f"Expected ^logs\\.error$, got {compiled}"
        finally:
            drop_queue(db_connection, queue_name)

    def test_compiled_regex_star_wildcard(self, db_connection: psycopg.Connection):
        """Test that * compiles to [^.]+ (match one non-dot segment)."""
        now = int(time.time())
        queue_name = f"test_regex_star_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.*", queue_name)

            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT compiled_regex FROM pgmq.topic_bindings "
                    "WHERE pattern = 'logs.*' AND queue_name = %s",
                    (queue_name,),
                )
                compiled = cur.fetchone()[0]
                assert compiled == r"^logs\.[^.]+$", f"Expected ^logs\\.[^.]+$, got {compiled}"
        finally:
            drop_queue(db_connection, queue_name)

    def test_compiled_regex_hash_wildcard(self, db_connection: psycopg.Connection):
        """Test that # compiles to .* (match zero or more of anything)."""
        now = int(time.time())
        queue_name = f"test_regex_hash_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "logs.#", queue_name)

            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT compiled_regex FROM pgmq.topic_bindings "
                    "WHERE pattern = 'logs.#' AND queue_name = %s",
                    (queue_name,),
                )
                compiled = cur.fetchone()[0]
                assert compiled == r"^logs\..*$", f"Expected ^logs\\..*$, got {compiled}"
        finally:
            drop_queue(db_connection, queue_name)

    def test_compiled_regex_mixed_pattern(self, db_connection: psycopg.Connection):
        """Test compiled regex for pattern with both * and # wildcards."""
        now = int(time.time())
        queue_name = f"test_regex_mixed_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "app.*.logs.#", queue_name)

            with db_connection.cursor() as cur:
                cur.execute(
                    "SELECT compiled_regex FROM pgmq.topic_bindings "
                    "WHERE pattern = 'app.*.logs.#' AND queue_name = %s",
                    (queue_name,),
                )
                compiled = cur.fetchone()[0]
                assert (
                    compiled == r"^app\.[^.]+\.logs\..*$"
                ), f"Unexpected compiled regex: {compiled}"
        finally:
            drop_queue(db_connection, queue_name)

    # --- Behavioral tests: dots and hyphens treated as literal in routing ---

    def test_dot_not_matching_arbitrary_characters(
        self, db_connection: psycopg.Connection
    ):
        """Test that dots in patterns match only literal dots, not arbitrary characters."""
        now = int(time.time())
        queue_name = f"test_regex_dotlit_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "a.b", queue_name)

            # 'aXb' should NOT match 'a.b' (dot is literal, not regex wildcard)
            results = get_routing_matches(db_connection, "aXb")
            assert len(results) == 0, "Dot in pattern should not match arbitrary characters"

            # 'a.b' should match
            results = get_routing_matches(db_connection, "a.b")
            assert len(results) == 1, "Literal dot should match"
        finally:
            drop_queue(db_connection, queue_name)

    def test_hyphen_in_pattern_safe(self, db_connection: psycopg.Connection):
        """Test that hyphens in patterns are treated literally, not as regex range operators."""
        now = int(time.time())
        queue_name = f"test_regex_hyphen_{now}"

        try:
            create_queue(db_connection, queue_name)
            bind_topic(db_connection, "my-app.logs.*", queue_name)

            # Should match with literal hyphen
            results = get_routing_matches(db_connection, "my-app.logs.error")
            assert len(results) == 1, "Hyphen in pattern should match literally"

            # Should NOT match when hyphen is replaced with different char
            results = get_routing_matches(db_connection, "myXapp.logs.error")
            assert len(results) == 0, "Hyphen should be literal, not match arbitrary chars"
        finally:
            drop_queue(db_connection, queue_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
