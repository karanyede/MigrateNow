"""
Keyset-paginated bulk data fetcher.

Uses ``sys_id > LAST_ID`` ordering which is O(1) per page regardless of
how deep into the dataset we are — unlike ``sysparm_offset`` which
degrades to O(n) at scale.

Records are yielded page-by-page as a generator so memory stays constant
even for 100 k+ record tables.
"""

from __future__ import annotations

import logging
from typing import Generator

from migration.client import ServiceNowClient

logger = logging.getLogger("sn_migration")


class BulkFetcher:
    """
    Streams all records from a ServiceNow table using keyset pagination.

    Parameters
    ----------
    client : ServiceNowClient
        Authenticated client for the instance to read from.
    table_name : str
        API name of the table (e.g. ``"incident"``).
    fields : list[str]
        Columns to retrieve.  Always includes ``sys_id``.
    page_size : int
        Records per page (default 2 000).
    """

    def __init__(
        self,
        client: ServiceNowClient,
        table_name: str,
        fields: list[str],
        page_size: int = 2000,
        extra_query: str = "",
    ) -> None:
        self.client = client
        self.table = table_name
        # Always fetch sys_id — it's the keyset cursor and the primary key.
        self.fields = list(dict.fromkeys(["sys_id"] + fields))
        self.page_size = page_size
        self.extra_query = extra_query

    # ─────────────────────────────────────────────────────────────────
    # Generator API
    # ─────────────────────────────────────────────────────────────────

    def pages(self, limit: int | None = None) -> Generator[list[dict], None, None]:
        """
        Yield one page of records at a time.

        Stops when ServiceNow returns an empty page or the limit is reached.
        """
        last_id = ""
        page_num = 0
        total_rows = 0

        while True:
            current_limit = self.page_size
            if limit is not None:
                remaining = limit - total_rows
                if remaining <= 0:
                    break
                current_limit = min(self.page_size, remaining)

            page = self.client.fetch_page(
                table_name=self.table,
                last_sys_id=last_id,
                limit=current_limit,
                fields=self.fields,
                extra_query=self.extra_query,
            )
            if not page:
                break

            page_num += 1
            total_rows += len(page)

            # Defensive: ensure every item is a dict.  ServiceNow can
            # occasionally return unexpected shapes (single dict, string,
            # or a list of strings) depending on table/version quirks.
            if page and not isinstance(page[0], dict):
                logger.error(
                    "Unexpected page item type on page %d: %s — sample: %r",
                    page_num,
                    type(page[0]).__name__,
                    str(page[0])[:200],
                )
                if isinstance(page, dict):
                    page = [page]
                else:
                    logger.error("Cannot parse page data. Stopping fetch.")
                    break

            last_id = page[-1]["sys_id"]

            logger.info(
                "Fetched page %d (%d rows, running total %d) from '%s'.",
                page_num,
                len(page),
                total_rows,
                self.table,
            )
            yield page

        logger.info(
            "Fetch complete for '%s': %d rows across %d pages.",
            self.table,
            total_rows,
            page_num,
        )

    def fetch_all(self, limit: int | None = None) -> list[dict]:
        """
        Convenience: collect every page into a single list.

        Use ``pages()`` instead if memory is a concern.
        """
        all_records: list[dict] = []
        for page in self.pages(limit=limit):
            all_records.extend(page)
        return all_records

    def fetch_sys_ids(self) -> set[str]:
        """
        Fetch **only** ``sys_id`` values from the table.

        This is used for the diff phase to determine which source
        records already exist in the target — minimises payload
        because we transfer only a single column.
        """
        ids: set[str] = set()
        last_id = ""
        page_num = 0

        while True:
            page = self.client.fetch_page(
                table_name=self.table,
                last_sys_id=last_id,
                limit=self.page_size,
                fields=["sys_id"],
            )
            if not page:
                break

            page_num += 1
            for row in page:
                if isinstance(row, dict):
                    ids.add(row["sys_id"])
            last_id = page[-1]["sys_id"]

            logger.debug(
                "sys_id page %d (%d IDs, running total %d).",
                page_num,
                len(page),
                len(ids),
            )

        logger.info(
            "Fetched %d sys_ids from '%s' across %d pages.",
            len(ids),
            self.table,
            page_num,
        )
        return ids

    def fetch_coalesce_map(self, coalesce_field: str) -> dict[str, str]:
        """
        Fetch ``{coalesce_value: target_sys_id}`` from the target table.

        This is used when the target generates its own ``sys_id`` values
        (e.g. via JSONv2 insertMultiple) and we need an alternate field
        to match source records against existing target records.

        Parameters
        ----------
        coalesce_field : str
            The target field that holds the source record's identity
            (e.g. ``u_legacy_sysid``).

        Returns
        -------
        dict mapping coalesce field values → target sys_id strings.
        """
        coalesce_map: dict[str, str] = {}
        last_id = ""
        page_num = 0

        while True:
            page = self.client.fetch_page(
                table_name=self.table,
                last_sys_id=last_id,
                limit=self.page_size,
                fields=["sys_id", coalesce_field],
            )
            if not page:
                break

            page_num += 1
            for row in page:
                if isinstance(row, dict):
                    val = row.get(coalesce_field, "")
                    if val:  # skip blanks
                        coalesce_map[val] = row["sys_id"]
            last_id = page[-1]["sys_id"]

        logger.info(
            "Fetched %d coalesce entries ('%s') from '%s' across %d pages.",
            len(coalesce_map),
            coalesce_field,
            self.table,
            page_num,
        )
        return coalesce_map
