"""A tiny dedupe ledger shared by every fake downstream stub.

Real payment and dispatch providers de-duplicate on an idempotency key you
pass them; that de-dupe store is what actually makes an activity safe to
retry. This is a minimal stand-in for that store, backed by sqlite so state
survives a killed worker process -- which is the whole point in Phase 5,
where a worker is restarted mid-workflow and must not double-charge or
double-reserve anything.
"""

import json
import sqlite3
import threading
from pathlib import Path

_DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent.parent.parent / "deploy" / "stub_state.db"


class Ledger:
    def __init__(self, db_path: Path | str = _DEFAULT_DB_PATH):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger (
                    idempotency_key TEXT PRIMARY KEY,
                    activity_name TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=10)

    def record_once(self, idempotency_key: str, activity_name: str, result: dict) -> tuple[dict, bool]:
        """Insert a result for this key if absent.

        Returns (result, was_already_recorded). The insert either succeeds
        (this is the first time this idempotency key has ever been seen) or
        collides on the primary key (a duplicate call), in which case the
        previously stored result is returned instead of computing a new one.
        """
        payload = json.dumps(result)
        with self._lock, self._connect() as conn:
            try:
                conn.execute(
                    "INSERT INTO ledger (idempotency_key, activity_name, result_json) VALUES (?, ?, ?)",
                    (idempotency_key, activity_name, payload),
                )
                conn.commit()
                return result, False
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT result_json FROM ledger WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                return json.loads(row[0]), True

    @staticmethod
    def _count_for_key(conn: sqlite3.Connection, key: str) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE idempotency_key = ?", (key,)
        ).fetchone()[0]

    def get(self, idempotency_key: str) -> dict | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT result_json FROM ledger WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            return json.loads(row[0]) if row else None

    def call_count(self, idempotency_key: str) -> int:
        """Number of ledger rows for this key -- always 0 or 1 by construction.

        Exposed for tests; real duplicate-call counting happens one level up,
        in the in-memory `calls` counters the stubs keep for observability.
        """
        with self._lock, self._connect() as conn:
            return self._count_for_key(conn, idempotency_key)

    def count_by_activity(self, activity_name: str) -> int:
        """How many distinct idempotency keys recorded a side effect for this
        activity. Used by tests to assert things like "exactly one capture
        happened" without needing to know the generated key in advance."""
        with self._lock, self._connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE activity_name = ?", (activity_name,)
            ).fetchone()[0]

    def reset(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM ledger")
