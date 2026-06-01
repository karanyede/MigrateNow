"""
CSV Export Processor — high-volume bulk data fetcher.

Uses ServiceNow's CSV Export Processor endpoint::

    GET /{table}_list.do?CSV&sysparm_query=...&sysparm_fields=...

Instead of the REST Table API, this bypasses the 10 k per-page hard cap
and ACL-per-cell overhead, allowing 50 k–100 k+ records per HTTP call.

The response is a streaming CSV that we parse row-by-row with
``csv.DictReader``, keeping memory at O(1) per row regardless of total
dataset size.

For instances that enforce a CSV export row limit
(``glide.ui.export.limit``), the fetcher automatically chains requests
using keyset pagination (``sys_id > LAST_ID``), giving us
"CSV keyset pagination" — each chunk is a full CSV stream that picks up
exactly where the last one stopped.

If the CSV endpoint is unavailable (403, plugin disabled, etc.), the
caller should fall back to the existing ``BulkFetcher``.
"""

from __future__ import annotations

import concurrent.futures
import csv
import io
import logging
import time
from typing import List, Tuple

from config import CSV_PARTITIONS, MAX_WORKERS_CSV
from migration.client import ServiceNowClient

logger = logging.getLogger("sn_migration")


class CsvExportFetcher:
    """
    Stream all records from a ServiceNow table via CSV Export Processor.

    Parameters
    ----------
    client : ServiceNowClient
        Authenticated client for the instance to read from.
    table_name : str
        API name of the table (e.g. ``"u_migration_dump"``).
    fields : list[str]
        Columns to retrieve.  Always includes ``sys_id``.
    extra_query : str
        Additional encoded query to append (e.g. date filters).
    """

    def __init__(
        self,
        client: ServiceNowClient,
        table_name: str,
        fields: list[str],
        extra_query: str = "",
    ) -> None:
        self.client = client
        self.table = table_name
        # Always include sys_id — needed for keyset pagination cursor
        # and as the primary key for the diff engine.
        self.fields = list(dict.fromkeys(["sys_id"] + fields))
        self.extra_query = extra_query

    # ─────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────

    def fetch_all(self, parallel: bool = True, max_workers: int = MAX_WORKERS_CSV) -> List[dict]:
        """Fetch all records, optionally using parallel partitioned export."""
        if parallel and self.client.instance:
            try:
                return self._fetch_all_parallel(max_workers)
            except Exception as exc:
                logger.warning("Parallel CSV fetch failed (%s). Falling back to serial fetch.", exc)
        return self._fetch_all_serial()

    def _fetch_all_parallel(self, max_workers: int = 4) -> List[dict]:
        """
        Fetch all records using parallel CSV exports partitioned by groups of hex digits.
        Number of partitions = CSV_PARTITIONS (default 4). Each partition gets ~1/N of the data.
        """
        # Define partition groups of hex digits (0-9a-f)
        hex_digits = [f"{x:01x}" for x in range(16)]
        # Split into CSV_PARTITIONS groups
        partition_size = max(1, len(hex_digits) // CSV_PARTITIONS)
        partitions = [
            hex_digits[i:i+partition_size] for i in range(0, len(hex_digits), partition_size)
        ]
        # Ensure we have exactly CSV_PARTITIONS (adjust last group)
        if len(partitions) > CSV_PARTITIONS:
            # Merge last few groups
            while len(partitions) > CSV_PARTITIONS:
                partitions[-2].extend(partitions[-1])
                partitions.pop()
        elif len(partitions) < CSV_PARTITIONS:
            # Duplicate some prefixes? Should not happen.
            pass

        def fetch_partition(prefixes: List[str], idx: int) -> Tuple[int, List[dict]]:
            """Fetch records whose sys_id starts with any of the given prefixes."""
            # Build query: sys_idSTARTSWITHp1^ORsys_idSTARTSWITHp2...
            subqueries = [f"sys_idSTARTSWITH{p}" for p in prefixes]
            query = "^OR".join(subqueries)
            if self.extra_query:
                query = f"({query})^{self.extra_query}"
            query = f"{query}^ORDERBYsys_id"

            try:
                resp = self.client.csv_export_stream(
                    table_name=self.table,
                    fields=self.fields,
                    query=query,
                )
                records = self._parse_csv_response(resp)
                logger.debug("Partition %d (%s): %d records", idx, ",".join(prefixes), len(records))
                return idx, records
            except Exception as exc:
                logger.error("Failed to fetch partition %d: %s", idx, exc)
                return idx, []

        all_records = []
        t0 = time.perf_counter()

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(partitions))) as executor:
            future_to_part = {
                executor.submit(fetch_partition, part, i): i
                for i, part in enumerate(partitions)
            }
            for future in concurrent.futures.as_completed(future_to_part):
                _, records = future.result()
                all_records.extend(records)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Parallel CSV fetch complete: %d total records from '%s' "
            "in %.1fs using %d workers and %d partitions.",
            len(all_records), self.table, elapsed, max_workers, len(partitions),
        )
        return all_records
    

    # ─────────────────────────────────────────────────────────────────
    # Serial keyset‑paginated CSV (original implementation)
    # ─────────────────────────────────────────────────────────────────

    def _fetch_all_serial(self) -> List[dict]:
        """
        Fetch every record using serial keyset-paginated CSV.
        Original implementation – reliable fallback.
        """
        all_records: list[dict] = []
        last_id = ""
        chunk_num = 0
        t_total = time.perf_counter()

        while True:
            chunk_num += 1
            t0 = time.perf_counter()

            # Build query with keyset cursor
            query_parts: list[str] = []
            if last_id:
                query_parts.append(f"sys_id>{last_id}")
            if self.extra_query:
                query_parts.append(self.extra_query)
            query_parts.append("ORDERBYsys_id")
            query = "^".join(query_parts)

            # Stream the CSV from ServiceNow
            try:
                resp = self.client.csv_export_stream(
                    table_name=self.table,
                    fields=self.fields,
                    query=query,
                )
            except Exception as exc:
                if chunk_num == 1:
                    # First attempt failed — let caller handle fallback
                    raise
                logger.error(
                    "CSV export chunk %d failed: %s. "
                    "Returning %d records fetched so far.",
                    chunk_num, exc, len(all_records),
                )
                break

            # Parse the streaming CSV response
            chunk_records = self._parse_csv_response(resp)
            elapsed = time.perf_counter() - t0

            if not chunk_records:
                if chunk_num == 1:
                    logger.warning(
                        "CSV export returned 0 records for '%s'. "
                        "The table may be empty or the CSV endpoint "
                        "may be disabled.",
                        self.table,
                    )
                break

            all_records.extend(chunk_records)
            last_id = chunk_records[-1]["sys_id"]

            logger.info(
                "CSV chunk %d: %d records in %.1fs "
                "(running total: %d, last_sys_id: %s…) from '%s'.",
                chunk_num,
                len(chunk_records),
                elapsed,
                len(all_records),
                last_id[:12],
                self.table,
            )

            # If we got fewer records than a typical SN export limit,
            # we've reached the end of the dataset.
            if len(chunk_records) < 9900:
                logger.info(
                    "CSV chunk %d returned %d records (< 9900 threshold). "
                    "Assuming end of dataset.",
                    chunk_num, len(chunk_records),
                )
                break

        total_elapsed = time.perf_counter() - t_total
        logger.info(
            "Serial CSV fetch complete for '%s': %d records across %d chunk(s) "
            "in %.1fs (%.0f records/sec).",
            self.table,
            len(all_records),
            chunk_num,
            total_elapsed,
            len(all_records) / total_elapsed if total_elapsed > 0 else 0,
        )

        return all_records

    def fetch_sys_ids(self) -> set[str]:
        """
        Fetch only ``sys_id`` values via CSV export.

        Much faster than fetching all fields — useful for the diff phase.
        """
        # Temporarily override fields to just sys_id
        original_fields = self.fields
        self.fields = ["sys_id"]
        try:
            records = self.fetch_all()
            return {r["sys_id"] for r in records if r.get("sys_id")}
        finally:
            self.fields = original_fields

    def fetch_coalesce_map(self, coalesce_field: str) -> dict[str, str]:
        """
        Fetch ``{coalesce_value: target_sys_id}`` via CSV export.

        Parameters
        ----------
        coalesce_field : str
            The target field that holds the source record's identity
            (e.g. ``u_legacy_sysid``).

        Returns
        -------
        dict mapping coalesce field values → target sys_id strings.
        """
        original_fields = self.fields
        self.fields = ["sys_id", coalesce_field]
        try:
            records = self.fetch_all()
            coalesce_map: dict[str, str] = {}
            for rec in records:
                val = rec.get(coalesce_field, "")
                if val:
                    coalesce_map[val] = rec["sys_id"]
            logger.info(
                "CSV coalesce map: %d entries for '%s' from '%s'.",
                len(coalesce_map), coalesce_field, self.table,
            )
            return coalesce_map
        finally:
            self.fields = original_fields

    # ─────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────

    def _parse_csv_response(self, resp) -> list[dict]:
        """
        Parse a streaming CSV response into a list of dicts.

        Handles:
          - BOM markers (ServiceNow sometimes prepends UTF-8 BOM)
          - Windows-1252 encoding (SN default for CSV exports)
          - Empty or malformed rows
          - Fields that don't match our requested field list
        """
        records: list[dict] = []

        try:
            # Stream lines from the response.
            # SN CSV exports may use windows-1252 or utf-8.
            # We try utf-8 first, fall back to windows-1252.
            lines = self._iter_decoded_lines(resp)

            reader = csv.DictReader(lines)

            if reader.fieldnames is None:
                logger.warning("CSV response has no header row.")
                return []

            # Log the header for debugging
            logger.debug(
                "CSV header from '%s': %s",
                self.table, reader.fieldnames,
            )

            # Validate that sys_id is in the header
            if "sys_id" not in reader.fieldnames:
                logger.error(
                    "CSV header missing 'sys_id'. Got: %s. "
                    "This may mean the CSV export returned display "
                    "values or the field list is incorrect.",
                    reader.fieldnames[:10],
                )
                return []

            row_count = 0
            skip_count = 0
            for row in reader:
                row_count += 1

                # Skip rows with empty sys_id (malformed data)
                sid = row.get("sys_id", "").strip()
                if not sid:
                    skip_count += 1
                    continue

                # Build a clean record with only our requested fields
                clean: dict[str, str] = {}
                for field in self.fields:
                    val = row.get(field, "")
                    # Strip whitespace and handle None
                    clean[field] = str(val).strip() if val is not None else ""

                records.append(clean)

            if skip_count > 0:
                logger.warning(
                    "CSV parse: skipped %d rows with empty sys_id "
                    "out of %d total rows.",
                    skip_count, row_count,
                )

        except Exception as exc:
            logger.error(
                "CSV parse error for '%s': %s. "
                "Returning %d records parsed so far.",
                self.table, exc, len(records),
            )

        return records

    @staticmethod
    def _iter_decoded_lines(resp) -> Iterator[str]:
        """
        Yield decoded text lines from a streaming ``requests.Response``.

        Handles encoding detection and BOM stripping.
        """
        first_line = True
        for raw_line in resp.iter_lines(decode_unicode=False):
            if not raw_line:
                continue

            # Try UTF-8 first, fall back to windows-1252
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                line = raw_line.decode("windows-1252", errors="replace")

            # Strip BOM from first line
            if first_line:
                line = line.lstrip("\ufeff")
                first_line = False

            yield line