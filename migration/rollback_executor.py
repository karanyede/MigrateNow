"""
Rollback Executor — undoes a migration job.

For every record that was INSERTED during migration → DELETE from target.
For every record that was UPDATED during migration → PATCH target back to pre-state.

Supports both ServiceNow and Salesforce targets via duck-typing on the client.
Progress is reported via an optional callback (phase, processed, total, detail).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable

from migration.rollback_store import RollbackStore

logger = logging.getLogger("sn_migration")

# Batch sizes for rollback operations
_SN_BATCH = 100   # SN Batch API supports up to 100 sub-requests
_SF_BATCH = 200   # SF SObject Collections supports up to 200


@dataclass
class RollbackResult:
    """Outcome of a rollback run."""
    deleted: int = 0       # inserts that were deleted
    restored: int = 0      # updates whose pre-state was restored
    failed: int = 0        # operations that errored
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


class RollbackExecutor:
    """
    Executes rollback operations against a target instance.

    Parameters
    ----------
    store : RollbackStore
        The rollback database.
    target_client : ServiceNowClient | SalesforceClient
        Authenticated client for the target.
    target_table : str
        Target table / SObject name.
    target_platform : str
        ``'sn'`` or ``'sf'``.
    progress_callback : Callable | None
        ``fn(phase, processed, total, detail)`` for real-time progress.
    """

    def __init__(
        self,
        store: RollbackStore,
        target_client,
        target_table: str,
        target_platform: str,
        progress_callback: Callable | None = None,
    ) -> None:
        self.store = store
        self.tgt = target_client
        self.table = target_table
        self.platform = target_platform
        self._progress = progress_callback

    # ─────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────

    def run(self, job_id: str) -> RollbackResult:
        """Execute the full rollback for *job_id*."""
        result = RollbackResult()
        t0 = time.perf_counter()

        self.store.mark_status(job_id, "rolling_back")

        insert_recs = self.store.get_records(job_id, action="insert")
        update_recs = self.store.get_records(job_id, action="update")

        total = len(insert_recs) + len(update_recs)
        processed = 0

        def emit(phase, detail=""):
            if self._progress:
                self._progress(phase, processed, total, detail)

        # ── Phase 1: Delete inserted records ─────────────────────────
        emit("delete", f"Deleting {len(insert_recs):,} inserted records…")
        insert_keys = [r["target_key"] for r in insert_recs if r.get("target_key")]

        if insert_keys:
            if self.platform == "sf":
                d, f_count, errs = self._sf_bulk_delete(insert_keys)
            else:
                d, f_count, errs = self._sn_batch_delete(insert_keys)

            result.deleted += d
            result.failed += f_count
            result.errors.extend(errs)
            processed += len(insert_keys)
            emit("delete", f"Deleted {d:,} records ({f_count} failed).")

        # ── Phase 2: Restore updated records ─────────────────────────
        emit("restore", f"Restoring {len(update_recs):,} updated records…")

        if update_recs:
            if self.platform == "sf":
                res, f_count, errs = self._sf_batch_restore(update_recs)
            else:
                res, f_count, errs = self._sn_batch_restore(update_recs)

            result.restored += res
            result.failed += f_count
            result.errors.extend(errs)
            processed += len(update_recs)
            emit("restore", f"Restored {res:,} records ({f_count} failed).")

        result.elapsed_seconds = time.perf_counter() - t0
        processed = total
        emit("done", (
            f"Rollback complete: {result.deleted:,} deleted, "
            f"{result.restored:,} restored, {result.failed} failed."
        ))

        final_status = "failed" if result.failed == total and total > 0 else "rolled_back"
        self.store.mark_status(job_id, final_status)
        return result

    # ─────────────────────────────────────────────────────────────────
    # ServiceNow rollback operations
    # ─────────────────────────────────────────────────────────────────

    def _sn_batch_delete(
        self, keys: list[str]
    ) -> tuple[int, int, list[str]]:
        """Delete SN records in batches using the Batch API or fallback."""
        ok, fail = 0, 0
        errors: list[str] = []

        for i in range(0, len(keys), _SN_BATCH):
            chunk = keys[i: i + _SN_BATCH]
            ops = [
                {
                    "id": f"del_{sid}",
                    "method": "DELETE",
                    "url": f"/api/now/table/{self.table}/{sid}",
                    "headers": [],
                }
                for sid in chunk
            ]
            try:
                responses = self.tgt.batch_request(ops)
                for j, resp in enumerate(responses):
                    status = resp.get("status_code", 0)
                    if status in (200, 204, 404):  # 404 = already gone, treat as OK
                        ok += 1
                    else:
                        fail += 1
                        errors.append(
                            f"DELETE {chunk[j] if j < len(chunk) else '?'}: HTTP {status}"
                        )
            except Exception as exc:
                fail += len(chunk)
                errors.append(f"Batch delete chunk failed: {exc}")
                logger.warning("SN batch delete chunk failed: %s", exc)

        logger.info("SN rollback delete: %d deleted, %d failed.", ok, fail)
        return ok, fail, errors

    def _sn_batch_restore(
        self, records: list[dict]
    ) -> tuple[int, int, list[str]]:
        """Restore SN records using Batch API PATCH."""
        ok, fail = 0, 0
        errors: list[str] = []

        for i in range(0, len(records), _SN_BATCH):
            chunk = records[i: i + _SN_BATCH]
            ops = []
            for rec in chunk:
                sid = rec["target_key"]
                pre = rec.get("pre_state") or {}
                # Strip sys_id from patch body — it's in the URL
                body = {k: v for k, v in pre.items() if k != "sys_id"}
                import json
                ops.append({
                    "id": f"rst_{sid}",
                    "method": "PATCH",
                    "url": f"/api/now/table/{self.table}/{sid}",
                    "headers": [{"name": "Content-Type", "value": "application/json"}],
                    "body": json.dumps(body),
                })
            try:
                responses = self.tgt.batch_request(ops)
                for j, resp in enumerate(responses):
                    status = resp.get("status_code", 0)
                    if 200 <= status < 300:
                        ok += 1
                    else:
                        fail += 1
                        sid_ref = chunk[j]["target_key"] if j < len(chunk) else "?"
                        errors.append(f"PATCH {sid_ref}: HTTP {status}")
            except Exception as exc:
                fail += len(chunk)
                errors.append(f"Batch restore chunk failed: {exc}")
                logger.warning("SN batch restore chunk failed: %s", exc)

        logger.info("SN rollback restore: %d restored, %d failed.", ok, fail)
        return ok, fail, errors

    # ─────────────────────────────────────────────────────────────────
    # Salesforce rollback operations
    # ─────────────────────────────────────────────────────────────────

    def _sf_bulk_delete(
        self, keys: list[str]
    ) -> tuple[int, int, list[str]]:
        """Delete SF records using SObject Collections DELETE (200/call)."""
        ok, fail = 0, 0
        errors: list[str] = []

        for i in range(0, len(keys), _SF_BATCH):
            chunk = keys[i: i + _SF_BATCH]
            try:
                results = self.tgt.sobject_collections_delete(chunk)
                for j, r in enumerate(results):
                    if r.get("success") or r.get("statusCode") in (204, 404):
                        ok += 1
                    else:
                        fail += 1
                        errs = r.get("errors", [])
                        msg = errs[0].get("message", str(r)) if errs else str(r)
                        sf_id = chunk[j] if j < len(chunk) else "?"
                        errors.append(f"DELETE {sf_id}: {msg}")
            except Exception as exc:
                fail += len(chunk)
                errors.append(f"SF bulk delete chunk failed: {exc}")
                logger.warning("SF bulk delete chunk failed: %s", exc)

        logger.info("SF rollback delete: %d deleted, %d failed.", ok, fail)
        return ok, fail, errors

    def _sf_batch_restore(
        self, records: list[dict]
    ) -> tuple[int, int, list[str]]:
        """Restore SF records using SObject Collections PATCH."""
        ok, fail = 0, 0
        errors: list[str] = []

        for i in range(0, len(records), _SF_BATCH):
            chunk = records[i: i + _SF_BATCH]
            payload = []
            for rec in chunk:
                pre = rec.get("pre_state") or {}
                entry = {k: v for k, v in pre.items() if k not in ("attributes",)}
                entry["Id"] = rec["target_key"]
                payload.append(entry)

            try:
                results = self.tgt.sobject_collections_update(self.table, payload)
                for j, r in enumerate(results):
                    if r.get("success"):
                        ok += 1
                    else:
                        fail += 1
                        errs = r.get("errors", [])
                        msg = errs[0].get("message", str(r)) if errs else str(r)
                        sf_id = chunk[j]["target_key"] if j < len(chunk) else "?"
                        errors.append(f"PATCH {sf_id}: {msg}")
            except Exception as exc:
                fail += len(chunk)
                errors.append(f"SF batch restore chunk failed: {exc}")
                logger.warning("SF batch restore chunk failed: %s", exc)

        logger.info("SF rollback restore: %d restored, %d failed.", ok, fail)
        return ok, fail, errors
