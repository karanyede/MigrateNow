"""
Migration orchestrator — the top-level pipeline.

Pipeline stages:
  1. Fetch ALL source records   (CSV Export Processor or keyset pagination)
  2. Fetch target sys_ids       (keyset pagination, sys_id only)
  3. Compute diff               (O(n) set-based)
  4. Bulk-load inserts + updates (Batch API, 100 ops/call)
  5. Build summary report       (timing, counts, API calls)

The orchestrator is the only module the Flask app needs to interact
with.  It wires together client → fetcher → differ → loader.
"""

from __future__ import annotations

import sys
if sys.platform.startswith("win"):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import json
import logging
import time
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from config import BATCH_SIZE, FETCH_PAGE_SIZE, LOG_DIR, SF_COLLECTIONS_BATCH_SIZE
from migration.client import ServiceNowClient
from migration.csv_fetcher import CsvExportFetcher
from migration.differ import DiffEngine, TargetMatcher
from migration.fetcher import BulkFetcher
from migration.loader import BulkLoader, LoadResult
from migration.rate_tracker import RateTracker
from migration.rollback_store import RollbackStore
from migration.sf_client import SalesforceClient
from migration.sf_bulk_fetcher import SalesforceBulkFetcher
from migration.sf_loader import SalesforceLoader, SFLoadResult

logger = logging.getLogger("sn_migration")


@dataclass
class TimingBreakdown:
    """Wall-clock seconds for each pipeline stage."""

    fetch_source: float = 0.0
    fetch_target: float = 0.0
    diff: float = 0.0
    load: float = 0.0
    total: float = 0.0


@dataclass
class MigrationReport:
    """
    Everything the UI / CLI needs to display the final result.
    """

    source_instance: str = ""
    target_instance: str = ""
    source_table: str = ""
    target_table: str = ""
    total_source_records: int = 0
    inserts: int = 0
    updates: int = 0
    skipped: int = 0
    failed: int = 0
    fetch_mode_used: str = ""  # "csv", "rest", "bulk_api", "rest_query"
    migration_type: str = "sn_sn"  # "sn_sn", "sn_sf", "sf_sf", "sf_sn"
    timing: TimingBreakdown = field(default_factory=TimingBreakdown)
    api_calls: dict = field(default_factory=dict)
    failed_records_file: str = ""

    def as_console_banner(self) -> str:
        """Pretty-print for CLI / log output."""
        sep = "═" * 55
        dash = "─" * 55
        t = self.timing

        def _fmt_time(s: float) -> str:
            if s >= 60:
                return f"{int(s // 60)}m {s % 60:.0f}s"
            return f"{s:.1f}s"

        src_api = self.api_calls.get("source", {})
        tgt_api = self.api_calls.get("target", {})

        lines = [
            "",
            sep,
            "  MIGRATION COMPLETE",
            sep,
            f"  Source:  {self.source_instance} / {self.source_table}",
            f"  Target:  {self.target_instance} / {self.target_table}",
            f"  Fetch Mode: {self.fetch_mode_used.upper()}",
            dash,
            f"  Records Fetched:   {self.total_source_records:>8,}",
            f"  Records Inserted:  {self.inserts:>8,}",
            f"  Records Updated:   {self.updates:>8,}",
            f"  Records Skipped:   {self.skipped:>8,}",
            f"  Records Failed:    {self.failed:>8,}",
            dash,
            f"  Total Time:        {_fmt_time(t.total):>8}",
            f"    Fetch (Source):  {_fmt_time(t.fetch_source):>8}",
            f"    Fetch (Target): {_fmt_time(t.fetch_target):>8}",
            f"    Diff Compute:   {_fmt_time(t.diff):>8}",
            f"    Load (Insert+Update): {_fmt_time(t.load):>8}",
            dash,
            "  API Calls Made:",
            f"    Source: {src_api.get('calls_made', 0):>6}"
            f"  (Limit: {src_api.get('rate_limit_per_hour', '?')}/hr,"
            f" run ~{src_api.get('estimated_remaining', '?')} left,"
            f" org ~{src_api.get('org_remaining', '?')} left)",
            f"    Target: {tgt_api.get('calls_made', 0):>6}"
            f"  (Limit: {tgt_api.get('rate_limit_per_hour', '?')}/hr,"
            f" run ~{tgt_api.get('estimated_remaining', '?')} left,"
            f" org ~{tgt_api.get('org_remaining', '?')} left)",
            dash,
            f"  Total API Calls:  {self.api_calls.get('total_calls', 0):>8,}",
            sep,
            "",
        ]
        return "\n".join(lines)

    def to_dict(self) -> dict:
        """JSON-serialisable dict for the UI."""
        return {
            "source_instance": self.source_instance,
            "target_instance": self.target_instance,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "total_source_records": self.total_source_records,
            "inserts": self.inserts,
            "updates": self.updates,
            "skipped": self.skipped,
            "failed": self.failed,
            "fetch_mode_used": self.fetch_mode_used,
            "migration_type": self.migration_type,
            "timing": {
                "fetch_source": round(self.timing.fetch_source, 2),
                "fetch_target": round(self.timing.fetch_target, 2),
                "diff": round(self.timing.diff, 2),
                "load": round(self.timing.load, 2),
                "total": round(self.timing.total, 2),
            },
            "api_calls": self.api_calls,
            "failed_records_file": self.failed_records_file,
        }


class MigrationOrchestrator:
    """
    Drives the full migration pipeline.

    Parameters
    ----------
    source_client : ServiceNowClient | SalesforceClient
        Authenticated client for the source instance/org.
    target_client : ServiceNowClient | SalesforceClient
        Authenticated client for the target instance/org.
    source_table, target_table : str
        API table/object names.
    field_mapping : dict[str, str]
        ``{source_field: target_field}`` from the field-mapping UI.
    tracker : RateTracker
        Shared API-call tracker.
    progress_callback : Callable | None
        ``fn(phase, processed, total, detail)`` for real-time UI updates.
    source_fields_meta : list[dict] | None
        Field metadata — used to identify reference fields.
    fetch_mode : str
        ``"auto"`` — try CSV/Bulk first, fall back to REST.
        ``"csv"``  — force CSV Export Processor (SN only).
        ``"rest"`` — use REST API.
        ``"bulk_api"`` — force SF Bulk API 2.0.
    migration_type : str
        ``"sn_sn"``, ``"sn_sf"``, ``"sf_sf"``, or ``"sf_sn"``.
    sf_external_id_field : str | None
        External ID field name for SF target dedup.
    """

    def __init__(
        self,
        source_client,
        target_client,
        source_table: str,
        target_table: str,
        field_mapping: dict[str, str],
        tracker: RateTracker,
        progress_callback: Callable | None = None,
        source_fields_meta: list[dict] | None = None,
        fetch_mode: str = "auto",
        migration_type: str = "sn_sn",
        sf_external_id_field: str | None = None,
        filter_conditions: list | None = None,
        pause_event: threading.Event | None = None,
        limit: int | None = None,
        rollback_store: RollbackStore | None = None,
        rollback_job_id: str | None = None,
        coalesce_config: dict | None = None,
    ) -> None:
        self.src = source_client
        self.tgt = target_client
        self.src_table = source_table
        self.tgt_table = target_table
        self.mapping = field_mapping
        self.tracker = tracker
        self._progress = progress_callback
        self.fetch_mode = fetch_mode
        self.migration_type = migration_type
        self.sf_external_id_field = sf_external_id_field
        self.filter_conditions = filter_conditions or []
        self.pause_event = pause_event
        self.limit = limit
        self.rollback_store = rollback_store
        self.rollback_job_id = rollback_job_id
        self.coalesce_config = coalesce_config or {}
        # Build {field_name: ref_table} for reference fields in the mapping
        self._ref_fields: dict[str, str] = {}
        if source_fields_meta and self._is_source_sn:
            ref_lookup = {
                f["name"]: f["reference"]
                for f in source_fields_meta
                if f.get("reference")
            }
            for src_field in field_mapping:
                if src_field in ref_lookup:
                    self._ref_fields[src_field] = ref_lookup[src_field]

    # ── Platform helpers ─────────────────────────────────────────────

    @property
    def _is_source_sn(self) -> bool:
        return self.migration_type in ("sn_sn", "sn_sf")

    @property
    def _is_source_sf(self) -> bool:
        return self.migration_type in ("sf_sf", "sf_sn")

    @property
    def _is_target_sn(self) -> bool:
        return self.migration_type in ("sn_sn", "sf_sn")

    @property
    def _is_target_sf(self) -> bool:
        return self.migration_type in ("sn_sf", "sf_sf")

    def _emit(self, phase: str, processed: int, total: int, detail: str = ""):
        if self._progress:
            self._progress(phase, processed, total, detail)

    # ─────────────────────────────────────────────────────────────────
    # Reference resolution
    # ─────────────────────────────────────────────────────────────────

    def _resolve_references(self, records: list[dict]) -> None:
        """
        Resolve reference field sys_ids from source instance to target
        instance **in-place**.

        For each reference field in the mapping:
          1. Collect all unique source sys_ids
          2. Batch-lookup display values on source (sys_id → name)
          3. Batch-lookup sys_ids by name on target (name → target_sys_id)
          4. Replace source sys_ids with target sys_ids in records

        This adds ~2 API calls per reference table instead of making
        every fetch_page 40x slower with display_value=true.
        """
        if not self._ref_fields or not records:
            return

        for src_field, ref_display_table in self._ref_fields.items():
            # 1. Collect unique non-empty sys_ids from records
            unique_ids = {
                r[src_field] for r in records
                if r.get(src_field) and len(r[src_field]) == 32
            }
            if not unique_ids:
                continue

            # 2. Batch-lookup display values on source
            # Query: sys_idIN<id1>,<id2>,... on the referenced table
            # We need to find the actual table name (ref_display_table
            # is the display label from sys_dictionary, e.g. "User")
            # For simplicity, use the source table API with display_value
            # to resolve just these sys_ids.
            id_list = ",".join(unique_ids)

            # Lookup display names from the source
            # Use sys_dictionary reference value which is the table label
            # We need the actual API table name — look it up
            ref_table = self._resolve_ref_table_name(ref_display_table)
            if not ref_table:
                logger.warning(
                    "Could not resolve reference table '%s' for field '%s'. Skipping.",
                    ref_display_table, src_field,
                )
                continue

            # Fetch source display names: {sys_id: display_name}
            src_display = self._batch_lookup_display(
                self.src, ref_table, unique_ids,
            )
            if not src_display:
                continue

            # 3. Batch-lookup target sys_ids by display name
            unique_names = set(src_display.values())
            tgt_sysids = self._batch_lookup_by_name(
                self.tgt, ref_table, unique_names,
            )

            # 4. Build source_sys_id → target_sys_id map
            id_map: dict[str, str] = {}
            for src_id, name in src_display.items():
                if name in tgt_sysids:
                    id_map[src_id] = tgt_sysids[name]

            # 5. Replace in records
            replaced = 0
            for rec in records:
                old_val = rec.get(src_field, "")
                if old_val in id_map:
                    rec[src_field] = id_map[old_val]
                    replaced += 1

            logger.info(
                "Resolved %d/%d '%s' references (%s) cross-instance.",
                replaced, len(unique_ids), src_field, ref_table,
            )

    def _resolve_ref_table_name(self, display_label: str) -> str:
        """Convert a reference table display label to API table name."""
        # Common mappings
        known = {
            "User": "sys_user",
            "Group": "sys_user_group",
            "Company": "core_company",
            "Location": "cmn_location",
            "Configuration Item": "cmdb_ci",
            "Service": "cmdb_ci_service",
            "Offering": "service_offering",
            "Contract": "ast_contract",
        }
        if display_label in known:
            return known[display_label]

        # Try API lookup
        try:
            resp = self.src._request(
                "GET",
                "/api/now/table/sys_db_object",
                params={
                    "sysparm_query": f"label={display_label}",
                    "sysparm_fields": "name",
                    "sysparm_limit": 1,
                },
            )
            resp.raise_for_status()
            rows = resp.json().get("result", [])
            if rows:
                return rows[0].get("name", "")
        except Exception:
            pass
        return ""

    def _batch_lookup_display(
        self, client: ServiceNowClient, table: str, sys_ids: set[str],
    ) -> dict[str, str]:
        """Fetch {sys_id: display_value} for a set of sys_ids."""
        result: dict[str, str] = {}
        id_list = list(sys_ids)
        # Process in chunks of 100 to avoid query length limits
        for i in range(0, len(id_list), 100):
            chunk = id_list[i:i+100]
            query = "sys_idIN" + ",".join(chunk)
            try:
                page = client.fetch_page(
                    table_name=table,
                    limit=len(chunk),
                    fields=["sys_id", "name"],
                    extra_query=query,
                )
                for rec in page:
                    result[rec["sys_id"]] = rec.get("name", "")
            except Exception as e:
                logger.warning("Display lookup failed for %s: %s", table, e)
        return result

    def _batch_lookup_by_name(
        self, client: ServiceNowClient, table: str, names: set[str],
    ) -> dict[str, str]:
        """Fetch {name: sys_id} from target by name."""
        result: dict[str, str] = {}
        name_list = [n for n in names if n]
        for i in range(0, len(name_list), 50):
            chunk = name_list[i:i+50]
            query = "nameIN" + ",".join(chunk)
            try:
                page = client.fetch_page(
                    table_name=table,
                    limit=len(chunk),
                    fields=["sys_id", "name"],
                    extra_query=query,
                )
                for rec in page:
                    result[rec.get("name", "")] = rec["sys_id"]
            except Exception as e:
                logger.warning("Name lookup failed for %s: %s", table, e)
        return result

    def _build_sn_query(self, filters: list) -> str:
        """Converts filter conditions JSON → SN encoded query string."""
        if not filters:
            return ""
        
        group_queries = []
        for group in filters:
            if not group:
                continue
            cond_queries = []
            for cond in group:
                field = cond.get("field")
                op = cond.get("op")
                val = cond.get("value", "")
                if not field or not op:
                    continue
                if op == "EMPTY":
                    cond_queries.append(f"{field}ISEMPTY")
                elif op == "NOTEMPTY":
                    cond_queries.append(f"{field}ISNOTEMPTY")
                else:
                    cond_queries.append(f"{field}{op}{val}")
            if cond_queries:
                # Group conditions are OR-ed in ServiceNow using ^OR
                group_queries.append("^OR".join(cond_queries))
        
        if not group_queries:
            return ""
        
        # Groups are AND-ed using ^
        return "^".join(group_queries)

    def _build_sf_where(self, filters: list) -> str:
        """Converts filter conditions JSON → SOQL WHERE clause."""
        if not filters:
            return ""
        
        def escape_string(s: str) -> str:
            return s.replace("'", "\\'")

        def format_soql_val(val: str, op: str) -> str:
            val = val.strip()
            if op.upper() == "IN":
                parts = [p.strip() for p in val.split(",") if p.strip()]
                formatted_parts = []
                for p in parts:
                    if p.lower() in ("true", "false"):
                        formatted_parts.append(p.lower())
                    else:
                        try:
                            float(p)
                            formatted_parts.append(p)
                        except ValueError:
                            formatted_parts.append(f"'{escape_string(p)}'")
                return "(" + ", ".join(formatted_parts) + ")"
            
            if val.lower() in ("true", "false"):
                return val.lower()
            try:
                float(val)
                return val
            except ValueError:
                escaped = escape_string(val)
                if op.upper() == "LIKE":
                    if not escaped.startswith("%") and not escaped.endswith("%"):
                        return f"'%{escaped}%'"
                return f"'{escaped}'"

        group_queries = []
        for group in filters:
            if not group:
                continue
            cond_queries = []
            for cond in group:
                field = cond.get("field")
                op = cond.get("op")
                val = cond.get("value", "")
                if not field or not op:
                    continue
                
                op_upper = op.upper()
                if op_upper in ("IS NULL", "IS NOT NULL"):
                    cond_queries.append(f"{field} {op_upper}")
                else:
                    formatted_val = format_soql_val(val, op)
                    cond_queries.append(f"{field} {op} {formatted_val}")
            
            if cond_queries:
                if len(cond_queries) > 1:
                    group_queries.append("(" + " OR ".join(cond_queries) + ")")
                else:
                    group_queries.append(cond_queries[0])
        
        if not group_queries:
            return ""
        
        # Groups are AND-ed in SOQL
        return " AND ".join(group_queries)

    @staticmethod
    def _escape_sf_string(value: str) -> str:
        """Escape single quotes for SOQL string literals."""
        return str(value).replace("\\", "\\\\").replace("'", "\\'")

    @staticmethod
    def _chunk(values: list[str], size: int) -> list[list[str]]:
        """Split a list into fixed-size chunks."""
        return [values[i:i + size] for i in range(0, len(values), size)]

    def _fetch_sf_target_by_key_values(
        self,
        object_name: str,
        fields: list[str],
        key_field: str,
        key_values: set[str],
        chunk_size: int = 100,
    ) -> list[dict]:
        """
        Fetch only candidate target records by key values using SOQL IN.

        This avoids full-object scans when source payloads are small.
        """
        clean_values = sorted({str(v).strip() for v in key_values if str(v).strip()})
        if not clean_values:
            return []

        select_fields = list(dict.fromkeys(fields + [key_field]))
        field_list = ",".join(select_fields)
        all_records: list[dict] = []

        for vals in self._chunk(clean_values, chunk_size):
            quoted = ",".join(f"'{self._escape_sf_string(v)}'" for v in vals)
            soql = (
                f"SELECT {field_list} FROM {object_name} "
                f"WHERE {key_field} IN ({quoted}) ORDER BY Id"
            )
            all_records.extend(self.tgt.query(soql))

        return all_records


    # ─────────────────────────────────────────────────────────────────
    # Source fetch strategies
    # ─────────────────────────────────────────────────────────────────

    def _fetch_source_csv(self, source_fields: list[str]) -> list[dict] | None:
        """
        Attempt to fetch source records using CSV Export Processor.

        Returns the records list on success, or ``None`` if CSV export
        is unavailable / failed (so the caller can fall back to REST).
        """
        try:
            self._emit(
                "fetch_source", 0, 0,
                "Fetching source via CSV Export Processor (high-volume mode)…",
            )
            csv_fetcher = CsvExportFetcher(
                client=self.src,
                table_name=self.src_table,
                fields=source_fields,
                extra_query=self._build_sn_query(self.filter_conditions),
            )
            records = csv_fetcher.fetch_all(limit=self.limit)

            if records:
                logger.info(
                    "CSV Export Processor fetched %d records from '%s'.",
                    len(records), self.src_table,
                )
                return records

            # CSV returned 0 records — might be disabled or empty table.
            # Fall back to REST to be sure.
            logger.warning(
                "CSV export returned 0 records. Falling back to REST API."
            )
            return None

        except Exception as exc:
            logger.warning(
                "CSV Export Processor failed: %s. Falling back to REST API.",
                exc,
            )
            self._emit(
                "fetch_source", 0, 0,
                f"CSV export unavailable ({exc}). Switching to REST API…",
            )
            return None

    def _fetch_source_rest(self, source_fields: list[str]) -> list[dict]:
        """
        Fetch source records using the standard keyset-paginated REST API.
        This is the reliable fallback.
        """
        self._emit("fetch_source", 0, 0, "Fetching source via REST API…")
        fetcher = BulkFetcher(
            client=self.src,
            table_name=self.src_table,
            fields=source_fields,
            page_size=FETCH_PAGE_SIZE,
            extra_query=self._build_sn_query(self.filter_conditions),
        )
        return fetcher.fetch_all(limit=self.limit)

    # ─────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────

    # ── SF source fetch ───────────────────────────────────────────────

    def _fetch_source_sf_bulk(self, source_fields: list[str]) -> list[dict] | None:
        """Fetch source records from Salesforce using Bulk API 2.0."""
        try:
            self._emit(
                "fetch_source", 0, 0,
                "Fetching source via SF Bulk API 2.0 Query…",
            )
            fetcher = SalesforceBulkFetcher(
                client=self.src,
                object_name=self.src_table,
                fields=source_fields,
                extra_where=self._build_sf_where(self.filter_conditions),
                limit=self.limit,
            )
            records = fetcher.fetch_all()
            if records:
                logger.info(
                    "SF Bulk API fetched %d records from '%s'.",
                    len(records), self.src_table,
                )
                return records
            logger.warning("SF Bulk API returned 0 records.")
            return None
        except Exception as exc:
            logger.warning("SF Bulk API fetch failed: %s", exc)
            return None

    def _fetch_source_sf_rest(self, source_fields: list[str]) -> list[dict]:
        """Fetch source records from SF using REST /query."""
        self._emit("fetch_source", 0, 0, "Fetching source via SF REST /query…")
        field_list = ",".join(dict.fromkeys(["Id"] + source_fields))
        soql = f"SELECT {field_list} FROM {self.src_table}"
        extra_where = self._build_sf_where(self.filter_conditions)
        if extra_where:
            soql += f" WHERE {extra_where}"
        soql += " ORDER BY Id"
        if self.limit is not None:
            soql += f" LIMIT {self.limit}"
        return self.src.query(soql)

    def run(self) -> MigrationReport:
        """Execute the full migration and return the report."""
        report = MigrationReport(
            source_instance=self.src.instance,
            target_instance=self.tgt.instance,
            source_table=self.src_table,
            target_table=self.tgt_table,
            migration_type=self.migration_type,
        )
        t_total = time.perf_counter()

        # ── 1. Fetch source records ──────────────────────────────────
        self._emit("fetch_source", 0, 0, "Starting source fetch…")
        t0 = time.perf_counter()

        source_fields = list(self.mapping.keys())
        source_records = None
        fetch_mode_used = "rest"  # default

        if self._is_source_sf:
            # Salesforce source
            if self.fetch_mode in ("bulk_api", "auto"):
                source_records = self._fetch_source_sf_bulk(source_fields)
                if source_records is not None:
                    fetch_mode_used = "bulk_api"
            if source_records is None:
                source_records = self._fetch_source_sf_rest(source_fields)
                fetch_mode_used = "rest_query"
        else:
            # ServiceNow source (existing logic unchanged)
            if self.fetch_mode in ("csv", "auto"):
                source_records = self._fetch_source_csv(source_fields)
                if source_records is not None:
                    fetch_mode_used = "csv"
            if source_records is None:
                if self.fetch_mode == "csv":
                    raise RuntimeError(
                        "CSV Export Processor failed and fetch_mode is set to "
                        "'csv' (no fallback). Check that the CSV export "
                        "endpoint is accessible on the source instance."
                    )
                source_records = self._fetch_source_rest(source_fields)
                fetch_mode_used = "rest"

        report.fetch_mode_used = fetch_mode_used
        report.total_source_records = len(source_records)
        report.timing.fetch_source = time.perf_counter() - t0
        self._emit(
            "fetch_source",
            len(source_records),
            len(source_records),
            f"Fetched {len(source_records):,} source records via {fetch_mode_used.upper()}.",
        )

        # Edge case: no source records after applying filters — finish early.
        if not source_records:
            logger.info(
                "No source records found for %s.%s — marking migration complete.",
                self.src.instance, self.src_table,
            )
            report.timing.total = time.perf_counter() - t_total
            report.api_calls = self.tracker.summary()
            self._emit("done", 1, 1, "Migration complete (no source records).")
            return report

        # ── 1b. Resolve reference fields cross-instance ──────────────
        if self._ref_fields:
            self._emit(
                "resolve_refs", 0, 0,
                f"Resolving {len(self._ref_fields)} reference fields…",
            )
            self._resolve_references(source_records)
            self._emit(
                "resolve_refs", 1, 1,
                "Reference fields resolved.",
            )

        # ── 2. Fetch target records for diff ─────────────────────────
        self._emit("fetch_target", 0, 0, "Fetching target records…")
        t0 = time.perf_counter()

        target_fields = list(set(self.mapping.values()))

        # Determine target primary key and validation
        target_valid_fields = None
        external_id_available = False
        external_id_mapped = False

        if self._is_target_sf:
            if "Id" not in target_fields:
                target_fields.append("Id")
            if self.sf_external_id_field and self.sf_external_id_field not in target_fields:
                target_fields.append(self.sf_external_id_field)

            try:
                target_describe = self.tgt.get_object_fields(self.tgt_table)
                target_valid_fields = {f["name"] for f in target_describe}
                external_id_available = bool(
                    self.sf_external_id_field
                    and self.sf_external_id_field in target_valid_fields
                )
                external_id_mapped = bool(
                    self.sf_external_id_field
                    and self.sf_external_id_field in self.mapping.values()
                )

                invalid_fields = [f for f in target_fields if f not in target_valid_fields]
                if invalid_fields:
                    logger.warning(
                        "Removing %d fields not found on target '%s': %s",
                        len(invalid_fields), self.tgt_table, invalid_fields,
                    )
                    self._emit(
                        "fetch_target", 0, 0,
                        f"Skipping {len(invalid_fields)} fields not on target: "
                        f"{', '.join(invalid_fields[:5])}",
                    )
                    target_fields = [f for f in target_fields if f in target_valid_fields]
            except Exception as exc:
                logger.warning("Could not validate target fields: %s", exc)

        # Resolve coalesce configuration
        coalesce_config = self.coalesce_config or {}
        if not coalesce_config or not coalesce_config.get("enabled"):
            # Fall back to auto-detected default single-field coalesce logic
            if self._is_target_sf:
                if self.sf_external_id_field and external_id_available and external_id_mapped:
                    coalesce_config = {"enabled": True, "logic": "AND", "fields": [self.sf_external_id_field]}
                elif "Name" in self.mapping.values() and ((target_valid_fields is None) or ("Name" in target_valid_fields)):
                    coalesce_config = {"enabled": True, "logic": "AND", "fields": ["Name"]}
                else:
                    coalesce_config = {"enabled": False, "logic": "AND", "fields": []}
            else:
                src_pk = "Id" if self._is_source_sf else "sys_id"
                coalesce_target_field = self.mapping.get(src_pk, src_pk)
                if coalesce_target_field != src_pk:
                    coalesce_config = {"enabled": True, "logic": "AND", "fields": [coalesce_target_field]}
                else:
                    coalesce_config = {"enabled": False, "logic": "AND", "fields": []}

        # Make sure all coalesce fields are in target_fields list
        if coalesce_config.get("enabled"):
            for f in coalesce_config.get("fields", []):
                if f not in target_fields:
                    if self._is_target_sf:
                        if target_valid_fields is None or f in target_valid_fields:
                            target_fields.append(f)
                    else:
                        target_fields.append(f)

        # Perform target records fetch
        target_records_list = []
        if self._is_target_sf:
            # Check if we can fetch selectively (only if exactly 1 coalesce field)
            is_selective = False
            if coalesce_config.get("enabled") and len(coalesce_config.get("fields", [])) == 1:
                key_field = coalesce_config["fields"][0]
                reverse_mapping = {tgt: src for src, tgt in self.mapping.items()}
                src_field = reverse_mapping.get(key_field)
                if src_field:
                    key_values = {
                        str(r.get(src_field, "")).strip()
                        for r in source_records
                        if str(r.get(src_field, "")).strip()
                    }
                    target_records_list = self._fetch_sf_target_by_key_values(
                        object_name=self.tgt_table,
                        fields=target_fields,
                        key_field=key_field,
                        key_values=key_values,
                    )
                    is_selective = True
                    logger.info(
                        "Target fetch strategy: selective by %s (%d source keys).",
                        key_field,
                        len(key_values),
                    )

            if not is_selective:
                field_list = ",".join(target_fields)
                soql = f"SELECT {field_list} FROM {self.tgt_table} ORDER BY Id"
                target_records_list = self.tgt.query(soql)
                logger.info(
                    "Target fetch strategy: full target scan."
                )
        else:
            if "sys_id" not in target_fields:
                target_fields.append("sys_id")

            # Try CSV Export first (much faster)
            try:
                target_csv_fetcher = CsvExportFetcher(
                    client=self.tgt,
                    table_name=self.tgt_table,
                    fields=target_fields,
                )
                target_records_list = target_csv_fetcher.fetch_all()
            except Exception as e:
                logger.warning("Target CSV fetch failed (%s), falling back to REST API.", e)
                target_fetcher = BulkFetcher(
                    self.tgt, self.tgt_table, target_fields, FETCH_PAGE_SIZE
                )
                target_records_list = target_fetcher.fetch_all()

        # Build TargetMatcher for O(1) matching
        target_matcher = TargetMatcher(
            target_records=target_records_list,
            field_mapping=self.mapping,
            coalesce_config=coalesce_config,
        )

        self._emit(
            "fetch_target",
            len(target_records_list),
            len(target_records_list),
            f"Found {len(target_records_list):,} existing records in target.",
        )

        report.timing.fetch_target = time.perf_counter() - t0

        # ── 3. Compute diff ──────────────────────────────────────────
        self._emit("diff", 0, 0, "Computing diff…")
        t0 = time.perf_counter()

        differ = DiffEngine(
            field_mapping=self.mapping,
            target_matcher=target_matcher,
            coalesce_mode=coalesce_config.get("enabled", False),
        )
        diff = differ.compute(source_records)
        report.skipped = diff.skipped
        report.timing.diff = time.perf_counter() - t0

        # Sanity check: if target isn't empty but we're about to insert
        # a large fraction of source records, warning
        if (target_records_list
                and diff.inserts
                and len(diff.inserts) > len(source_records) * 0.5):
            logger.warning(
                "⚠ SAFETY CHECK: %d inserts queued but target already "
                "has %d records. This may create duplicates! "
                "Verify that the target fetch was complete.",
                len(diff.inserts), len(target_records_list),
            )

        self._emit(
            "diff",
            diff.total,
            diff.total,
            f"{len(diff.inserts):,} to insert, {len(diff.updates):,} to update.",
        )

        # ── 3b. Capture update pre-states for rollback (zero API cost) ──
        if self.rollback_store and self.rollback_job_id and diff.updates:
            pre_states = []
            for rec in diff.updates:
                tgt_rec = target_matcher.match(rec)
                if tgt_rec:
                    tgt_key = tgt_rec.get("Id", "") or tgt_rec.get("sys_id", "")
                    if tgt_key:
                        pre_states.append({"target_key": tgt_key, "pre_state": tgt_rec})

            if pre_states:
                try:
                    self.rollback_store.add_updates(self.rollback_job_id, pre_states)
                    logger.info(
                        "Rollback: captured pre-state for %d updates.",
                        len(pre_states),
                    )
                except Exception as rb_exc:
                    logger.warning("Rollback pre-state capture failed: %s", rb_exc)

        # ── 4. Bulk load ─────────────────────────────────────────────
        self._emit("load", 0, len(diff.inserts) + len(diff.updates), "Loading…")
        t0 = time.perf_counter()

        def _load_progress(processed, total, phase):
            if self.pause_event:
                if not self.pause_event.is_set():
                    self._emit("paused", processed, total, "Migration paused by user. Click Resume to continue.")
                    self.pause_event.wait()
                    self._emit("resuming", processed, total, "Resuming migration...")
            self._emit("load", processed, total, f"{phase}: {processed:,}/{total:,}")

        if self._is_target_sf:
            # Salesforce target loader
            sf_loader = SalesforceLoader(
                self.tgt,
                self.tgt_table,
                self.mapping,
                external_id_field=self.sf_external_id_field,
                batch_size=SF_COLLECTIONS_BATCH_SIZE,
                progress_callback=_load_progress,
            )
            sf_result: SFLoadResult = sf_loader.load(diff.inserts, diff.updates)
            report.inserts = sf_result.inserted
            report.updates = sf_result.updated
            report.failed = sf_result.failed
            failed_records = sf_result.failed_records
        else:
            # ServiceNow target loader (existing, unchanged)
            loader = BulkLoader(
                self.tgt,
                self.tgt_table,
                self.mapping,
                BATCH_SIZE,
                progress_callback=_load_progress,
            )
            load_result: LoadResult = loader.load(diff.inserts, diff.updates)
            report.inserts = load_result.inserted
            report.updates = load_result.updated
            report.failed = load_result.failed
            failed_records = load_result.failed_records
        report.timing.load = time.perf_counter() - t0

        # ── 4b. Save inserted IDs to rollback store ──────────────────
        if self.rollback_store and self.rollback_job_id:
            # Collect inserted_ids from whichever loader was used
            if self._is_target_sf:
                ins_ids = sf_result.inserted_ids  # type: ignore[union-attr]
            else:
                ins_ids = load_result.inserted_ids  # type: ignore[union-attr]

            try:
                if ins_ids:
                    self.rollback_store.add_inserts(self.rollback_job_id, ins_ids)
                    logger.info(
                        "Rollback: captured %d inserted IDs.", len(ins_ids)
                    )
                self.rollback_store.mark_status(self.rollback_job_id, "captured")
            except Exception as rb_exc:
                logger.warning("Rollback insert-ID capture failed: %s", rb_exc)

        # ── 5. Save failed records ───────────────────────────────────
        if failed_records:
            fail_path = LOG_DIR / f"failed_records_{int(time.time())}.json"
            fail_path.write_text(
                json.dumps(failed_records, indent=2), encoding="utf-8"
            )
            report.failed_records_file = str(fail_path)
            logger.warning("Failed records saved to %s", fail_path)

        # ── 6. Finalise ──────────────────────────────────────────────
        report.timing.total = time.perf_counter() - t_total
        report.api_calls = self.tracker.summary()

        banner = report.as_console_banner()
        logger.info(banner)
        print(banner)

        # ── One-liner summary for quick scanning ─────────────────────
        total_migrated = report.inserts + report.updates
        total_calls = report.api_calls.get("total_calls", 0)
        _fmt_t = (
            f"{int(report.timing.total // 60)}m {report.timing.total % 60:.0f}s"
            if report.timing.total >= 60
            else f"{report.timing.total:.1f}s"
        )
        summary_line = (
            f"✓ Migrated {total_migrated:,} records "
            f"({report.inserts:,} inserts, {report.updates:,} updates) "
            f"in {_fmt_t} using {total_calls:,} API calls  "
            f"[{self.src.instance}/{self.src_table} → "
            f"{self.tgt.instance}/{self.tgt_table}] "
            f"(fetch: {report.fetch_mode_used.upper()})"
        )
        logger.info(summary_line)
        print(f"\n  {summary_line}\n")

        # Log final API call count to the dedicated api_calls logger
        _api_logger = logging.getLogger("api_calls")
        _api_logger.info("=" * 60)
        _api_logger.info("MIGRATION COMPLETE")
        _api_logger.info(
            "Source API calls: %d │ Target API calls: %d │ Total: %d",
            report.api_calls.get("source", {}).get("calls_made", 0),
            report.api_calls.get("target", {}).get("calls_made", 0),
            total_calls,
        )
        _api_logger.info(summary_line)
        _api_logger.info("=" * 60)

        self._emit("done", 1, 1, "Migration complete.")
        return report
