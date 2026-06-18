"""
Salesforce bulk loader — sends INSERT / UPDATE / UPSERT operations using
the most efficient available method.

Strategy (cascading fallback):
  1. **Bulk API 2.0 Ingest** — fastest; uploads CSV, server processes async.
  2. **SObject Collections** — 200 records per call, synchronous.
  3. **Single-record REST** — Individual POST/PATCH calls (last resort).

The loader mirrors ``BulkLoader`` interface for ServiceNow, so the
orchestrator can treat both identically.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, List

from config import SF_COLLECTIONS_BATCH_SIZE
from migration.sf_client import SalesforceClient

logger = logging.getLogger("sn_migration")


@dataclass
class SFLoadResult:
    """Aggregate outcome of a Salesforce bulk-load run."""

    inserted: int = 0
    updated: int = 0
    failed: int = 0
    failed_records: list[dict] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    batch_count: int = 0


class SalesforceLoader:
    """
    Loads records into a target Salesforce SObject.

    Parameters
    ----------
    client : SalesforceClient
        Authenticated client for the **target** org.
    object_name : str
        Target SObject API name.
    field_mapping : dict[str, str]
        ``{source_field: target_field}`` mapping from the UI.
    external_id_field : str | None
        External ID field for upsert dedup (e.g. ``"SN_Legacy_Id__c"``).
    batch_size : int
        Records per SObject Collections call (max 200).
    progress_callback : Callable | None
        ``fn(processed, total, phase)`` for progress updates.
    """

    def __init__(
        self,
        client: SalesforceClient,
        object_name: str,
        field_mapping: dict[str, str],
        external_id_field: str | None = None,
        batch_size: int = SF_COLLECTIONS_BATCH_SIZE,
        progress_callback: Callable | None = None,
    ) -> None:
        self.client = client
        self.object_name = object_name
        self.mapping = field_mapping
        self.external_id_field = external_id_field
        self.batch_size = min(batch_size, 200)  # SF max is 200
        self._progress = progress_callback
        self._processed = 0
        self._total = 0

    def _report_progress(self, count: int, phase: str) -> None:
        self._processed += count
        if self._progress:
            self._progress(self._processed, self._total, phase)

    # ─────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────

    def load(
        self,
        inserts: list[dict],
        updates: list[dict],
    ) -> SFLoadResult:
        """
        Execute all INSERT and UPDATE operations using the fastest
        available strategy.

        Returns an ``SFLoadResult`` with counts and any failed records.
        """
        result = SFLoadResult()
        self._total = len(inserts) + len(updates)
        self._processed = 0
        t0 = time.perf_counter()

        # ── Phase 1: Inserts ─────────────────────────────────────────
        if inserts:
            ok, fail, failed_recs = self._execute_inserts(inserts)
            result.inserted += ok
            result.failed += fail
            result.failed_records.extend(failed_recs)
            result.batch_count += 1

        # ── Phase 2: Updates (upsert if we have external ID) ─────────
        if updates:
            ok, fail, failed_recs = self._execute_updates(updates)
            result.updated += ok
            result.failed += fail
            result.failed_records.extend(failed_recs)
            result.batch_count += 1

        result.elapsed_seconds = time.perf_counter() - t0
        logger.info(
            "SF Load complete: %d inserted, %d updated, %d failed "
            "in %.1f s.",
            result.inserted,
            result.updated,
            result.failed,
            result.elapsed_seconds,
        )
        return result

    # ─────────────────────────────────────────────────────────────────
    # INSERT strategies
    # ─────────────────────────────────────────────────────────────────

    def _execute_inserts(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert records using cascading strategy."""
        if not records:
            return 0, 0, []

        # Strategy 1: Bulk API 2.0 Ingest (for batches exceeding SObject Collections limit)
        if len(records) > self.batch_size:
            try:
                return self._bulk_insert(records)
            except Exception as exc:
                logger.warning(
                    "Bulk API 2.0 insert failed: %s. Trying SObject Collections…",
                    exc,
                )

        # Strategy 2: SObject Collections
        try:
            return self._collections_insert(records)
        except Exception as exc:
            logger.warning(
                "SObject Collections insert failed: %s. "
                "Falling back to single-record.",
                exc,
            )

        # Strategy 3: Single-record REST
        return self._single_insert(records)

    # ─────────────────────────────────────────────────────────────────
    # UPDATE strategies
    # ─────────────────────────────────────────────────────────────────

    def _execute_updates(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update records using cascading strategy."""
        if not records:
            return 0, 0, []

        # Strategy 1: Bulk API 2.0 upsert (if we have external ID and batch exceeds collections limit)
        if self.external_id_field and len(records) > self.batch_size:
            try:
                return self._bulk_upsert(records)
            except Exception as exc:
                logger.warning(
                    "Bulk API 2.0 upsert failed: %s. "
                    "Trying SObject Collections…",
                    exc,
                )

        # Strategy 2: SObject Collections update
        try:
            return self._collections_update(records)
        except Exception as exc:
            logger.warning(
                "SObject Collections update failed: %s. "
                "Falling back to single-record.",
                exc,
            )

        # Strategy 3: Single-record PATCH
        return self._single_update(records)

    # ═════════════════════════════════════════════════════════════════
    # Strategy implementations
    # ═════════════════════════════════════════════════════════════════

    def _apply_mapping(self, record: dict) -> dict:
        """Rename source fields to target fields using the mapping.

        Also performs type coercion:
        - Strips 'Id' (target-org Ids are auto-generated).
        - Converts whole-number floats (e.g. 60426.0) to int, because
          Salesforce REST returns Number fields as floats but target
          Integer fields reject '60426.0' in CSV uploads.
        """
        mapped: dict = {}
        for src_field, tgt_field in self.mapping.items():
            if src_field in record:
                val = record[src_field]
                # Coerce whole-number floats → int (60426.0 → 60426)
                if isinstance(val, float) and val.is_integer():
                    val = int(val)
                mapped[tgt_field] = val
        # Never send the source Id — it's invalid in the target org
        mapped.pop("Id", None)
        return mapped

    def _records_to_csv(self, records: list[dict], fields: list[str]) -> str:
        """Convert list[dict] to CSV string for Bulk API upload."""
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue()

    # ── Bulk API 2.0 Insert ──────────────────────────────────────────

    def _bulk_insert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert via Bulk API 2.0 Ingest."""
        mapped_records = [self._apply_mapping(rec) for rec in records]

        # Determine CSV fields from the first record
        if not mapped_records:
            return 0, 0, []
        csv_fields = list(mapped_records[0].keys())

        # Create job
        job_id = self.client.bulk_ingest_create(
            self.object_name, "insert"
        )

        # Upload CSV
        csv_data = self._records_to_csv(mapped_records, csv_fields)
        self.client.bulk_ingest_upload(job_id, csv_data)

        # Close and poll
        self.client.bulk_ingest_close(job_id)
        self.client.bulk_ingest_poll(job_id)

        # Get results
        results = self.client.bulk_ingest_results(job_id)

        ok = len(results.get("successfulResults", []))
        failed_list = results.get("failedResults", [])
        fail = len(failed_list)

        failed_records = []
        for i, fr in enumerate(failed_list):
            error_msg = fr.get("sf__Error", "unknown")
            if i < 5 or (i + 1) % 10000 == 0:
                logger.error(
                    "Bulk insert failed record %d: %s", i + 1, error_msg
                )
            elif i == 5:
                logger.error("... (suppressing further individual insert errors)")
            if i < len(records):
                failed_records.append(records[i])

        logger.info(
            "Bulk API 2.0 insert: %d OK, %d failed — 1 job.",
            ok, fail,
        )
        self._report_progress(len(records), "insert")
        return ok, fail, failed_records

    # ── Bulk API 2.0 Upsert ──────────────────────────────────────────

    def _bulk_upsert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Upsert via Bulk API 2.0 Ingest with External ID field."""
        mapped_records = [self._apply_mapping(rec) for rec in records]

        if not mapped_records:
            return 0, 0, []
        csv_fields = list(mapped_records[0].keys())

        # Ensure external ID field is in CSV fields
        if self.external_id_field not in csv_fields:
            csv_fields.append(self.external_id_field)

        job_id = self.client.bulk_ingest_create(
            self.object_name,
            "upsert",
            external_id_field=self.external_id_field,
        )

        csv_data = self._records_to_csv(mapped_records, csv_fields)
        self.client.bulk_ingest_upload(job_id, csv_data)

        self.client.bulk_ingest_close(job_id)
        self.client.bulk_ingest_poll(job_id)

        results = self.client.bulk_ingest_results(job_id)

        ok = len(results.get("successfulResults", []))
        failed_list = results.get("failedResults", [])
        fail = len(failed_list)

        failed_records = []
        for i, fr in enumerate(failed_list):
            error_msg = fr.get("sf__Error", "unknown")
            if i < 5 or (i + 1) % 10000 == 0:
                logger.error(
                    "Bulk upsert failed record %d: %s", i + 1, error_msg
                )
            elif i == 5:
                logger.error("... (suppressing further individual upsert errors)")
            if i < len(records):
                failed_records.append(records[i])

        logger.info(
            "Bulk API 2.0 upsert: %d OK, %d failed — 1 job.",
            ok, fail,
        )
        self._report_progress(len(records), "update")
        return ok, fail, failed_records

    # ── SObject Collections Insert ───────────────────────────────────

    def _collections_insert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert via SObject Collections (200 per call)."""
        ok, fail = 0, 0
        failed_records: list[dict] = []

        for chunk in self._chunks(records, self.batch_size):
            mapped_chunk = [self._apply_mapping(rec) for rec in chunk]

            try:
                results = self.client.sobject_collections_create(
                    self.object_name, mapped_chunk
                )
                for i, r in enumerate(results):
                    if r.get("success"):
                        ok += 1
                    else:
                        fail += 1
                        error_msg = r.get("errors", [{}])[0].get("message", "unknown")
                        if fail <= 5 or fail % 10000 == 0:
                            logger.error(
                                "Collections insert failed record %d: %s", fail, error_msg
                            )
                        elif fail == 6:
                            logger.error("... (suppressing further individual insert errors)")
                        if i < len(chunk):
                            failed_records.append(chunk[i])
            except Exception as exc:
                logger.error("Collections insert chunk failed: %s", exc)
                fail += len(chunk)
                failed_records.extend(chunk)
            self._report_progress(len(chunk), "insert")

        logger.info(
            "SObject Collections insert: %d OK, %d failed.", ok, fail
        )
        return ok, fail, failed_records

    # ── SObject Collections Update ───────────────────────────────────

    def _collections_update(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update via SObject Collections (200 per call)."""
        ok, fail = 0, 0
        failed_records: list[dict] = []

        for chunk in self._chunks(records, self.batch_size):
            mapped_chunk = []
            for rec in chunk:
                mapped = self._apply_mapping(rec)
                # Need the SF Id for update
                if "Id" in rec:
                    mapped["Id"] = rec["Id"]
                elif "_target_sf_id" in rec:
                    mapped["Id"] = rec["_target_sf_id"]
                mapped_chunk.append(mapped)

            try:
                results = self.client.sobject_collections_update(
                    self.object_name, mapped_chunk
                )
                for i, r in enumerate(results):
                    if r.get("success"):
                        ok += 1
                    else:
                        fail += 1
                        errors = r.get("errors", [])
                        error_msg = errors[0].get("message", "?") if errors else "?"
                        logger.error(
                            "Collections update failed: %s", error_msg
                        )
                        if i < len(chunk):
                            failed_records.append(chunk[i])
            except Exception as exc:
                logger.error("Collections update chunk failed: %s", exc)
                fail += len(chunk)
                failed_records.extend(chunk)
            self._report_progress(len(chunk), "update")

        logger.info(
            "SObject Collections update: %d OK, %d failed.", ok, fail
        )
        return ok, fail, failed_records

    # ── Single-record fallback ───────────────────────────────────────

    def _single_insert(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Insert one-by-one (last resort)."""
        ok, fail = 0, 0
        failed_records: list[dict] = []

        for rec in records:
            mapped = self._apply_mapping(rec)
            try:
                results = self.client.sobject_collections_create(
                    self.object_name, [mapped]
                )
                if results and results[0].get("success"):
                    ok += 1
                else:
                    fail += 1
                    failed_records.append(rec)
            except Exception as exc:
                logger.error(
                    "Single-record insert failed: %s", str(exc)[:300]
                )
                fail += 1
                failed_records.append(rec)
            self._report_progress(1, "insert")

        return ok, fail, failed_records

    def _single_update(
        self, records: list[dict]
    ) -> tuple[int, int, list[dict]]:
        """Update one-by-one (last resort)."""
        ok, fail = 0, 0
        failed_records: list[dict] = []

        for rec in records:
            mapped = self._apply_mapping(rec)
            if "Id" in rec:
                mapped["Id"] = rec["Id"]
            elif "_target_sf_id" in rec:
                mapped["Id"] = rec["_target_sf_id"]
            try:
                results = self.client.sobject_collections_update(
                    self.object_name, [mapped]
                )
                if results and results[0].get("success"):
                    ok += 1
                else:
                    fail += 1
                    failed_records.append(rec)
            except Exception as exc:
                logger.error(
                    "Single-record update failed: %s", str(exc)[:300]
                )
                fail += 1
                failed_records.append(rec)
            self._report_progress(1, "update")

        return ok, fail, failed_records

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _chunks(lst: list, size: int):
        """Yield successive chunks of *size* from *lst*."""
        for i in range(0, len(lst), size):
            yield lst[i: i + size]
