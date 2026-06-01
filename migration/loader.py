"""
Bulk loader — sends INSERT / UPDATE operations to the target
ServiceNow instance using the most efficient available method.

Strategy (cascading fallback):
  1. **Parallel JSONv2 insertMultiple** — fastest; splits records into chunks
     and sends them concurrently.  Uses ~5 API calls for 75k records.
  2. **JSONv2 insertMultiple** (serial) — available on ALL instances.
  3. **Batch API** (``/api/now/batch``) — 100 sub-requests per call.
  4. **Parallel Table API** — Individual POST/PATCH calls in a thread pool.

The loader auto-detects which strategy works on the first call and
sticks with it for the remainder of the migration.

Failed records are captured and returned for downstream logging / retry.
"""

from __future__ import annotations

import json
import logging
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

from config import BATCH_SIZE, MAX_WORKERS_LOAD, JSONV2_CHUNK_SIZE
from migration.client import ServiceNowClient

logger = logging.getLogger("sn_migration")

# Max parallel threads for single-record fallback.
MAX_WORKERS = 25


@dataclass
class LoadResult:
    """Aggregate outcome of a bulk-load run."""

    inserted: int = 0
    updated: int = 0
    failed: int = 0
    failed_records: list[dict] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    batch_count: int = 0


class BulkLoader:
    """
    Loads records into a target ServiceNow table.

    Parameters
    ----------
    client : ServiceNowClient
        Authenticated client for the **target** instance.
    table_name : str
        Target table API name.
    field_mapping : dict[str, str]
        ``{source_field: target_field}`` mapping produced by the UI.
    batch_size : int
        Operations per batch (default from config).
    progress_callback : Callable | None
        Optional ``fn(processed, total, phase)`` called after each batch
        so the UI can update a progress bar.
    """

    def __init__(
        self,
        client: ServiceNowClient,
        table_name: str,
        field_mapping: dict[str, str],
        batch_size: int = BATCH_SIZE,
        progress_callback: Callable | None = None,
    ) -> None:
        self.client = client
        self.table = table_name
        self.mapping = field_mapping
        self.batch_size = batch_size
        self._progress = progress_callback
        # Strategy detection: None = untested, True = available
        self._jsonv2_available: bool | None = None
        self._batch_api_available: bool | None = None

    # ─────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────

    def load(
        self,
        inserts: list[dict],
        updates: list[dict],
    ) -> LoadResult:
        """
        Execute all INSERT and UPDATE operations using the fastest
        available strategy.

        Returns a ``LoadResult`` with counts and any failed records.
        """
        result = LoadResult()
        total = len(inserts) + len(updates)
        processed = 0
        t0 = time.perf_counter()

        # ── Phase 1: Inserts ─────────────────────────────────────────
        # Use parallel JSONv2 as primary strategy
        if inserts:
            ok, fail, failed_recs = self._parallel_jsonv2_insert(inserts)
            result.inserted += ok
            result.failed += fail
            result.failed_records.extend(failed_recs)
            jsonv2_chunk_size = self._jsonv2_chunk_size(len(inserts))
            result.batch_count += (len(inserts) + jsonv2_chunk_size - 1) // jsonv2_chunk_size
            processed += len(inserts)
            if self._progress:
                self._progress(processed, total, "insert")

        # ── Phase 2: Updates ─────────────────────────────────────────
        # Updates use Batch API (max ~100 ops/call) or parallel PATCH,
        # NOT JSONv2 (which has no updateMultiple). Use BATCH_SIZE.
        for batch in self._chunks(updates, self.batch_size):
            ok, fail, failed_recs = self._execute_update_batch(batch)
            result.updated += ok
            result.failed += fail
            result.failed_records.extend(failed_recs)
            result.batch_count += 1
            processed += len(batch)
            if self._progress:
                self._progress(processed, total, "update")

        result.elapsed_seconds = time.perf_counter() - t0
        logger.info(
            "Load complete: %d inserted, %d updated, %d failed "
            "across %d batch calls in %.1f s.",
            result.inserted,
            result.updated,
            result.failed,
            result.batch_count,
            result.elapsed_seconds,
        )
        return result

    # ─────────────────────────────────────────────────────────────────
    # INSERT strategies
    # ─────────────────────────────────────────────────────────────────

    def _execute_insert_batch(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert a batch using the best available strategy."""
        if not records:
            return 0, 0, []

        # Strategy 1: Parallel JSONv2 insertMultiple (fastest)
        if self._jsonv2_available is not False:
            try:
                return self._parallel_jsonv2_insert(records)
            except Exception as exc:
                err = str(exc)
                if "404" in err or "403" in err or "405" in err:
                    logger.warning(
                        "JSONv2 insertMultiple not available. Trying Batch API..."
                    )
                    self._jsonv2_available = False
                else:
                    logger.exception("JSONv2 insertMultiple failed unexpectedly.")
                    self._jsonv2_available = False

        # Strategy 2: Batch API
        if self._batch_api_available is not False:
            try:
                return self._batch_api_insert(records)
            except Exception as exc:
                err = str(exc)
                if any(code in err for code in ("400", "404", "413")):
                    logger.warning(
                        "Batch API not available (err=%s). "
                        "Falling back to parallel Table API.",
                        err[:200],
                    )
                    self._batch_api_available = False
                else:
                    logger.exception("Batch API failed.")
                    self._batch_api_available = False

        # Strategy 3: Parallel single-record Table API
        return self._parallel_single_insert(records)

    def _parallel_jsonv2_insert(self, records: List[dict]) -> Tuple[int, int, List[dict]]:
        """
        Split records into chunks and run JSONv2 insertMultiple concurrently.
        Uses `MAX_WORKERS_LOAD` from config (default 5).
        """
        if not records:
            return 0, 0, []

        chunk_size = self._jsonv2_chunk_size(len(records))
        chunks = [records[i:i+chunk_size] for i in range(0, len(records), chunk_size)]

        logger.info(
            "Splitting %d inserts into %d JSONv2 chunks (size max %d).",
            len(records), len(chunks), chunk_size,
        )

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_LOAD) as executor:
            futures = {
                executor.submit(self._jsonv2_insert_multiple, chunk): idx
                for idx, chunk in enumerate(chunks)
            }
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    ok, fail, failed_recs = future.result()
                    results.append((ok, fail, failed_recs))
                    logger.info("Chunk %d/%d complete: %d OK, %d failed.", idx + 1, len(chunks), ok, fail)
                except Exception as exc:
                    logger.error("Chunk %d/%d failed with exception: %s", idx + 1, len(chunks), exc)
                    # Treat entire chunk as failed
                    results.append((0, len(chunks[idx]), chunks[idx]))

        total_ok = sum(r[0] for r in results)
        total_fail = sum(r[1] for r in results)
        failed_records = []
        for _, _, fail_list in results:
            failed_records.extend(fail_list)

        logger.info(
            "Parallel JSONv2 insert: %d OK, %d failed across %d chunks.",
            total_ok, total_fail, len(chunks),
        )
        return total_ok, total_fail, failed_records

    # ─────────────────────────────────────────────────────────────────
    # UPDATE strategies
    # ─────────────────────────────────────────────────────────────────

    def _execute_update_batch(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update a batch using the best available strategy."""
        if not records:
            return 0, 0, []

        # Strategy 1: JSONv2 update (one per record, but parallel)
        # JSONv2 doesn't have a native updateMultiple via REST that
        # matches on sys_id, so we use parallel single updates.
        # Strategy 2: Batch API
        if self._batch_api_available is not False:
            try:
                return self._batch_api_update(records)
            except Exception as exc:
                err = str(exc)
                if any(code in err for code in ("400", "404", "413")):
                    logger.warning(
                        "Batch API not available for updates (err=%s). "
                        "Falling back to parallel Table API.",
                        err[:200],
                    )
                    self._batch_api_available = False
                else:
                    logger.exception("Batch API update failed.")
                    self._batch_api_available = False

        # Strategy 3: Parallel single-record PATCH
        return self._parallel_single_update(records)

    # ═════════════════════════════════════════════════════════════════
    # Strategy implementations
    # ═════════════════════════════════════════════════════════════════

    def _apply_mapping(self, record: dict) -> dict:
        """Rename source fields to target fields using the mapping."""
        mapped: dict = {}
        for src_field, tgt_field in self.mapping.items():
            if src_field in record:
                mapped[tgt_field] = record[src_field]
        return mapped

    # ── JSONv2 insertMultiple ────────────────────────────────────────

    def _jsonv2_insert_multiple(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """
        Insert records via ``/{table}.do?JSONv2&sysparm_action=insertMultiple``.

        Sends ALL records in the batch in a single HTTP call.
        Returns ``(ok, fail, failed_records)``.
        """
        mapped_records = []
        for rec in records:
            mapped = self._apply_mapping(rec)
            # Preserve sys_id from source for future diffing.
            if "sys_id" in rec and "sys_id" not in mapped:
                mapped["sys_id"] = rec["sys_id"]
            mapped_records.append(mapped)

        resp = self.client.jsonv2_insert_multiple(self.table, mapped_records)
        self._jsonv2_available = True

        results = resp.get("records", [])
        ok, fail, failed = 0, 0, []

        for i, r in enumerate(results):
            status = r.get("__status", "")
            if status == "success":
                ok += 1
            else:
                fail += 1
                error_msg = r.get("__error", r.get("error_message", "unknown error"))
                if isinstance(error_msg, dict):
                    error_msg = error_msg.get("message", str(error_msg))
                
                orig = dict(records[i]) if i < len(records) else {}
                if fail <= 5 or fail % 10000 == 0:
                    logger.error(
                        "insertMultiple item %d failed: %s (sys_id=%s)",
                        i, error_msg, orig.get("sys_id", "?"),
                    )
                elif fail == 6:
                    logger.error("... (suppressing further individual JSONv2 insert errors)")
                
                orig["__error"] = str(error_msg)
                failed.append(orig)

        # If SN returned fewer results than records sent, mark the rest
        # as failed.
        if len(results) < len(records):
            for i in range(len(results), len(records)):
                fail += 1
                failed.append(records[i])

        logger.info(
            "Inserted %d records (%d failed) via JSONv2 — 1 API call.",
            ok, fail,
        )
        return ok, fail, failed

    # ── Batch API (if available) ─────────────────────────────────────

    def _batch_api_insert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert via /api/now/batch."""
        ops = []
        for rec in records:
            mapped = self._apply_mapping(rec)
            if "sys_id" in rec and "sys_id" not in mapped:
                mapped["sys_id"] = rec["sys_id"]
            ops.append(
                {
                    "id": rec.get("sys_id", ""),
                    "method": "POST",
                    "url": f"/api/now/table/{self.table}",
                    "headers": [
                        {"name": "Content-Type", "value": "application/json"}
                    ],
                    "body": json.dumps(mapped),
                }
            )
        responses = self.client.batch_request(ops)
        self._batch_api_available = True
        return self._parse_batch_responses(responses, records)

    def _batch_api_update(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update via /api/now/batch."""
        ops = []
        for rec in records:
            # Use _target_sys_id (from coalesce diff) or fall back to sys_id
            patch_sid = rec.get("_target_sys_id", rec.get("sys_id", ""))
            mapped = self._apply_mapping(rec)
            mapped.pop("sys_id", None)
            mapped.pop("_target_sys_id", None)
            ops.append(
                {
                    "id": patch_sid,
                    "method": "PATCH",
                    "url": f"/api/now/table/{self.table}/{patch_sid}",
                    "headers": [
                        {"name": "Content-Type", "value": "application/json"}
                    ],
                    "body": json.dumps(mapped),
                }
            )
        responses = self.client.batch_request(ops)
        self._batch_api_available = True
        return self._parse_batch_responses(responses, records)

    def _parse_batch_responses(
        self, responses: list[dict], records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        ok, fail, failed = 0, 0, []
        for i, resp in enumerate(responses):
            status = resp.get("status_code", 0)
            if 200 <= status < 300:
                ok += 1
            else:
                fail += 1
                rec = records[i] if i < len(records) else {}
                
                body_str = resp.get("body", "")
                if resp.get("body_encoding") == "base64" and body_str:
                    import base64
                    try:
                        body_str = base64.b64decode(body_str).decode("utf-8")
                    except Exception:
                        pass
                        
                logger.error(
                    "Batch item %d failed (HTTP %s): %s",
                    i, status, str(body_str)[:500],
                )
                failed.append(rec)
        return ok, fail, failed

    # ── Parallel single-record Table API ─────────────────────────────

    def _parallel_single_insert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert one-by-one using a thread pool."""
        return self._parallel_table_api(records, mode="insert")

    def _parallel_single_update(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update one-by-one using a thread pool."""
        return self._parallel_table_api(records, mode="update")

    def _parallel_table_api(
        self, records: list[dict], mode: str
    ) -> tuple[int, int, list[dict]]:
        ok, fail = 0, 0
        failed: list[dict] = []

        def _do_one(rec: dict) -> tuple[bool, dict]:
            mapped = self._apply_mapping(rec)
            mapped.pop("_target_sys_id", None)
            try:
                if mode == "insert":
                    if "sys_id" in rec and "sys_id" not in mapped:
                        mapped["sys_id"] = rec["sys_id"]
                    self.client.insert_record(self.table, mapped)
                else:
                    # Use _target_sys_id (from coalesce diff) or fall back
                    patch_sid = rec.get("_target_sys_id", rec.get("sys_id", ""))
                    mapped.pop("sys_id", None)
                    self.client.update_record(self.table, patch_sid, mapped)
                return True, rec
            except Exception as exc:
                logger.error(
                    "Single-record %s failed (sys_id=%s): %s",
                    mode, rec.get("sys_id", "?"), str(exc)[:300],
                )
                return False, rec

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_do_one, rec): rec for rec in records}
            for future in as_completed(futures):
                success, rec = future.result()
                if success:
                    ok += 1
                else:
                    fail += 1
                    failed.append(rec)

        logger.info(
            "%s %d records (%d failed) via parallel Table API.",
            "Inserted" if mode == "insert" else "Updated",
            ok, fail,
        )
        return ok, fail, failed

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _chunks(lst: list, size: int):
        """Yield successive chunks of *size* from *lst*."""
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    @staticmethod
    def _jsonv2_chunk_size(record_count: int) -> int:
        """Return the effective JSONv2 chunk size for a record set."""
        if JSONV2_CHUNK_SIZE > 0:
            return JSONV2_CHUNK_SIZE

        chunk_size = max(1, record_count // MAX_WORKERS_LOAD)
        return min(chunk_size, 50_000)