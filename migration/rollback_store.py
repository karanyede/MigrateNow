"""
Rollback Store — SQLite-backed persistence for migration pre-states.

Schema
------
rollback_jobs:
    id, label, status, migration_type, target_platform,
    target_table, target_instance, created_at,
    captured_inserts, captured_updates

rollback_records:
    id, job_id, action ('insert'|'update'),
    target_key (sys_id / SF Id on target),
    pre_state  (JSON blob — field values before migration; NULL for inserts)

Status lifecycle:  'pending' → 'captured' → 'rolling_back' → 'rolled_back'
                                                            ↘ 'failed'
                              → 'discarded'
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger("sn_migration")

# DDL — idempotent
_DDL = """
CREATE TABLE IF NOT EXISTS rollback_jobs (
    id                  TEXT PRIMARY KEY,
    label               TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    migration_type      TEXT NOT NULL DEFAULT 'sn_sn',
    target_platform     TEXT NOT NULL DEFAULT 'sn',
    target_table        TEXT NOT NULL DEFAULT '',
    target_instance     TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    captured_inserts    INTEGER NOT NULL DEFAULT 0,
    captured_updates    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rollback_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT    NOT NULL REFERENCES rollback_jobs(id) ON DELETE CASCADE,
    action      TEXT    NOT NULL,
    target_key  TEXT    NOT NULL DEFAULT '',
    pre_state   TEXT    -- JSON or NULL for inserts
);

CREATE INDEX IF NOT EXISTS idx_rollback_records_job
    ON rollback_records (job_id, action);
"""


class RollbackStore:
    """
    Thread-safe SQLite wrapper for rollback job persistence.

    Parameters
    ----------
    db_path : Path
        Path to the SQLite database file.  Created on first use.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_DDL)
        logger.debug("RollbackStore initialised at %s", db_path)

    # ─────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────

    @contextmanager
    def _connect(self):
        """Yield a connection with WAL mode + foreign keys enabled."""
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ─────────────────────────────────────────────────────────────────
    # Job management
    # ─────────────────────────────────────────────────────────────────

    def create_job(
        self,
        job_id: str,
        label: str,
        migration_type: str,
        target_platform: str,
        target_table: str,
        target_instance: str,
        created_at: str,
    ) -> None:
        """Insert a new rollback job record with status='pending'."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO rollback_jobs
                    (id, label, status, migration_type, target_platform,
                     target_table, target_instance, created_at)
                VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
                """,
                (job_id, label, migration_type, target_platform,
                 target_table, target_instance, created_at),
            )
        logger.info("Rollback job created: %s (%s)", job_id, label)

    def mark_status(self, job_id: str, status: str) -> None:
        """Update the status of a rollback job."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE rollback_jobs SET status=? WHERE id=?",
                (status, job_id),
            )
        logger.info("Rollback job %s → status=%s", job_id, status)

    def get_job(self, job_id: str) -> dict | None:
        """Return a single job dict or None if not found."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM rollback_jobs WHERE id=?", (job_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_jobs(self) -> list[dict]:
        """Return all jobs, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM rollback_jobs ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_job(self, job_id: str) -> None:
        """
        Delete a job and all its records (discard).

        Requires foreign_keys=ON + CASCADE — handled in _connect().
        """
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM rollback_records WHERE job_id=?", (job_id,))
            conn.execute("DELETE FROM rollback_jobs WHERE id=?", (job_id,))
        logger.info("Rollback job %s discarded.", job_id)

    # ─────────────────────────────────────────────────────────────────
    # Record capture
    # ─────────────────────────────────────────────────────────────────

    def add_updates(self, job_id: str, records: list[dict]) -> None:
        """
        Persist pre-migration state for UPDATE operations.

        Parameters
        ----------
        job_id : str
        records : list[dict]
            Each dict must have:
                target_key  – sys_id / SF Id on target
                pre_state   – full record dict as it existed before migration
        """
        if not records:
            return
        rows = [
            (job_id, "update", r["target_key"], json.dumps(r["pre_state"], ensure_ascii=False))
            for r in records
            if r.get("target_key")
        ]
        with self._lock, self._connect() as conn:
            conn.executemany(
                "INSERT INTO rollback_records (job_id, action, target_key, pre_state) VALUES (?,?,?,?)",
                rows,
            )
            conn.execute(
                "UPDATE rollback_jobs SET captured_updates = captured_updates + ? WHERE id=?",
                (len(rows), job_id),
            )
        logger.info("Rollback: stored %d update pre-states for job %s.", len(rows), job_id)

    def add_inserts(self, job_id: str, target_keys: list[str]) -> None:
        """
        Persist target IDs for INSERT operations (pre_state is NULL —
        the record did not exist before migration).

        Parameters
        ----------
        job_id : str
        target_keys : list[str]
            sys_id / SF Id values assigned by the target after insert.
        """
        if not target_keys:
            return
        clean = [k for k in target_keys if k]
        rows = [(job_id, "insert", k) for k in clean]
        with self._lock, self._connect() as conn:
            conn.executemany(
                "INSERT INTO rollback_records (job_id, action, target_key) VALUES (?,?,?)",
                rows,
            )
            conn.execute(
                "UPDATE rollback_jobs SET captured_inserts = captured_inserts + ? WHERE id=?",
                (len(clean), job_id),
            )
        logger.info("Rollback: stored %d insert keys for job %s.", len(clean), job_id)

    def get_records(
        self, job_id: str, action: str | None = None
    ) -> list[dict]:
        """
        Fetch rollback records for a job.

        Parameters
        ----------
        job_id : str
        action : str | None
            Filter to ``'insert'`` or ``'update'``.  None = all.
        """
        with self._connect() as conn:
            if action:
                rows = conn.execute(
                    "SELECT * FROM rollback_records WHERE job_id=? AND action=?",
                    (job_id, action),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rollback_records WHERE job_id=?",
                    (job_id,),
                ).fetchall()

        result = []
        for row in rows:
            d = dict(row)
            if d.get("pre_state"):
                try:
                    d["pre_state"] = json.loads(d["pre_state"])
                except Exception:
                    d["pre_state"] = {}
            result.append(d)
        return result
