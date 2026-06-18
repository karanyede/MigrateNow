"""
Salesforce Bulk API 2.0 Query fetcher — high-volume data export.

Uses the Bulk API 2.0 Query endpoint for server-side processing at
~10,000 records/second.  Falls back to REST /query for smaller datasets
or if the Bulk API is unavailable.

Output format matches the SN fetcher (list[dict]) so the diff engine
works identically for both platforms.
"""

from __future__ import annotations

import logging
import time
from typing import List

from migration.sf_client import SalesforceClient

logger = logging.getLogger("sn_migration")


class SalesforceBulkFetcher:
    """
    Streams all records from a Salesforce SObject using Bulk API 2.0 Query.

    Parameters
    ----------
    client : SalesforceClient
        Authenticated client for the instance to read from.
    object_name : str
        SObject API name (e.g. ``"Account"``, ``"Contact"``).
    fields : list[str]
        Columns to retrieve.  Always includes ``Id``.
    extra_where : str
        Additional WHERE clause (e.g. ``"CreatedDate > 2024-01-01"``).
    """

    def __init__(
        self,
        client: SalesforceClient,
        object_name: str,
        fields: list[str],
        extra_where: str = "",
        limit: int | None = None,
    ) -> None:
        self.client = client
        self.object_name = object_name
        # Always include Id — it's the SF primary key
        self.fields = list(dict.fromkeys(["Id"] + fields))
        self.extra_where = extra_where
        self.limit = limit

    def fetch_all(self) -> List[dict]:
        """
        Fetch all records using Bulk API 2.0 Query.

        Falls back to REST /query if Bulk API fails.

        Returns all records as list[dict].
        """
        try:
            records = self._fetch_bulk()
        except Exception as exc:
            logger.warning(
                "Bulk API 2.0 Query failed for '%s': %s. "
                "Falling back to REST /query.",
                self.object_name,
                exc,
            )
            records = self._fetch_rest()

        if self.limit is not None:
            records = records[:self.limit]
        return records

    def _fetch_bulk(self) -> List[dict]:
        """Fetch using Bulk API 2.0 Query."""
        t0 = time.perf_counter()

        # 1. Create job
        job_id = self.client.bulk_query_create(
            self.object_name,
            self.fields,
            self.extra_where,
            limit=self.limit,
        )

        # 2. Poll until complete
        self.client.bulk_query_poll(job_id)

        # 3. Download results
        records = self.client.bulk_query_results(job_id)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Bulk API 2.0 Query fetched %d records from '%s' in %.1fs "
            "(%.0f records/sec).",
            len(records),
            self.object_name,
            elapsed,
            len(records) / elapsed if elapsed > 0 else 0,
        )
        return records

    def _fetch_rest(self) -> List[dict]:
        """
        Fallback: fetch using REST /query with nextRecordsUrl pagination.
        """
        t0 = time.perf_counter()

        field_list = ",".join(self.fields)
        soql = f"SELECT {field_list} FROM {self.object_name}"
        if self.extra_where:
            soql += f" WHERE {self.extra_where}"
        soql += " ORDER BY Id"
        if self.limit is not None:
            soql += f" LIMIT {self.limit}"

        records = self.client.query(soql)

        elapsed = time.perf_counter() - t0
        logger.info(
            "REST /query fetched %d records from '%s' in %.1fs.",
            len(records),
            self.object_name,
            elapsed,
        )
        return records
