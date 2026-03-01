import json
import os
import time

import psycopg
import pytest


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)


def create_queue(conn: psycopg.Connection, queue_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pgmq.create(%s)", (queue_name,))
        result = cur.fetchone()
        cur.execute('COMMIT')
        print(f"Queue creation result: {result}")


def send_messages_by_group(
    conn: psycopg.Connection,
    queue_name: str,
    group_keys: list[str],
    messages_per_group: int,
) -> list[int]:
    msg_ids = []
    for group_key in group_keys:
        for i in range(1, messages_per_group + 1):
            msg = {"group": group_key, "seq": i}
            headers = {"x-pgmq-group": group_key}
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pgmq.send(%s::text, %s::jsonb, %s::jsonb)",
                    (queue_name, json.dumps(msg), json.dumps(headers)),
                )
                result = cur.fetchone()
                msg_id = result[0] if result else None
                assert msg_id is not None
                msg_ids.append(msg_id)
                print(f"Sent msg_id={msg_id} group={group_key} seq={i}")
                cur.execute('COMMIT')
    print(f"Total messages sent: {len(msg_ids)}, IDs: {msg_ids}")
    return msg_ids


def read_grouped_head(
    conn: psycopg.Connection, queue_name: str, vt: int, qty: int
) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT msg_id, message, headers FROM pgmq.read_grouped_head(%s::text, %s::int, %s::int)",
            (queue_name, vt, qty),
        )
        rows = cur.fetchall()
    return [{"msg_id": r[0], "message": r[1], "headers": r[2]} for r in rows]


@pytest.fixture
def conn1():
    """First database connection."""
    conn = psycopg.Connection.connect(DATABASE_URL, autocommit=False)
    yield conn
    conn.close()


@pytest.fixture
def conn2():
    """Second database connection."""
    conn = psycopg.Connection.connect(DATABASE_URL, autocommit=False)
    yield conn
    conn.close()


def test_read_grouped_head(conn1: psycopg.Connection, conn2: psycopg.Connection):
    """Test read_grouped_head with two concurrent transactions proving SKIP LOCKED."""
    now = int(time.time())
    queue_name = f"test_read_grouped_head_{now}"

    create_queue(conn1, queue_name)

    group_keys = ["group_A", "group_B", "group_C", "group_D", "group_E", "group_F"]
    send_messages_by_group(conn1, queue_name, group_keys, messages_per_group=3)

    vt = 0 # do not change visibility, only verify the FOR UPDATE / SKIP LOCKED is working well

    # open conn1 transaction explicitly — rows locked until COMMIT
    with conn1.cursor() as cur:
        cur.execute("BEGIN TRANSACTION")
        cur.execute(
            "SELECT msg_id, message, headers FROM pgmq.read_grouped_head(%s::text, vt => %s::int, qty => %s::int)",
            (queue_name, vt, 3),
        )
        rows = cur.fetchall()
        conn1_messages = [{"msg_id": r[0], "message": r[1], "headers": r[2]} for r in rows]
        assert len(conn1_messages) == 3
        conn1_groups = [m["headers"]["x-pgmq-group"] for m in conn1_messages]
        assert conn1_groups == ["group_A", "group_B", "group_C"]

        with conn2.cursor() as cur2:
            cur2.execute("BEGIN TRANSACTION") # start new transaction with conn2
            cur2.execute(
                "SELECT msg_id, message, headers FROM pgmq.read_grouped_head(%s::text, vt => %s::int, qty => %s::int)",
                (queue_name, vt, 3),
            )
            rows = cur2.fetchall()
            conn2_messages = [{"msg_id": r[0], "message": r[1], "headers": r[2]} for r in rows]
            assert len(conn2_messages) == 3
            conn2_groups = [m["headers"]["x-pgmq-group"] for m in conn2_messages]
            assert conn2_groups == ["group_D", "group_E", "group_F"]

            # no overlap between the two connections
            conn1_ids = {m["msg_id"] for m in conn1_messages}
            conn2_ids = {m["msg_id"] for m in conn2_messages}
            assert conn1_ids.isdisjoint(conn2_ids)

    with conn1.cursor() as cur:
        cur.execute("COMMIT")
    with conn2.cursor() as cur:
        cur.execute("COMMIT")

    with conn2.cursor() as cur:
        cur.execute("SELECT pgmq.drop_queue(%s)", (queue_name,))
        cur.execute("COMMIT")

