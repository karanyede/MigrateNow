"""
Low-level ServiceNow REST API client.

Wraps ``requests.Session`` with:
* Connection pooling and keep-alive
* Automatic retry on 429 / 5xx with exponential back-off
* Rate-limit header parsing via ``RateTracker``
* Structured DEBUG logging for every request/response
"""

from __future__ import annotations

import json
import time
import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    BATCH_SIZE,
    CSV_EXPORT_TIMEOUT,
    FETCH_PAGE_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_FACTOR,
)
from migration.rate_tracker import RateTracker

logger = logging.getLogger("sn_migration")


class ServiceNowClient:
    """
    Reusable HTTP client for a **single** ServiceNow instance.

    Parameters
    ----------
    instance : str
        Hostname, e.g. ``"dev12345.service-now.com"``.
    username, password : str
        Basic-auth credentials.
    role : str
        ``"source"`` or ``"target"`` – passed to ``RateTracker``.
    tracker : RateTracker
        Shared tracker across source/target clients.
    """

    def __init__(
        self,
        instance: str,
        username: str,
        password: str,
        role: str,
        tracker: RateTracker,
    ) -> None:
        self.instance = instance.rstrip("/")
        self.base_url = f"https://{self.instance}"
        self.auth = (username, password)
        self.role = role
        self.tracker = tracker

        # ── session with retry adapter ───────────────────────────────
        self.session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    # ─────────────────────────────────────────────────────────────────
    # Core HTTP helpers
    # ─────────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: Any = None,
        timeout: int = REQUEST_TIMEOUT,
    ) -> requests.Response:
        """
        Execute an HTTP request, log it, and record the API call.
        Handles 429 with ``Retry-After`` back-off on top of urllib3 retries.
        """
        url = f"{self.base_url}{path}"
        t0 = time.perf_counter()

        response = self.session.request(
            method=method,
            url=url,
            auth=self.auth,
            params=params,
            json=json_body,
            timeout=timeout,
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.tracker.record_call(self.role, response)

        logger.debug(
            "API %s %s → %s (%.0f ms, %s bytes)",
            method,
            path,
            response.status_code,
            elapsed_ms,
            len(response.content),
        )

        # If still 429 after urllib3 retries, honour Retry-After header.
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(
                "Rate-limited (429). Sleeping %d s before retry.", retry_after
            )
            time.sleep(retry_after)
            return self._request(method, path, params, json_body, timeout)

        return response

    # ─────────────────────────────────────────────────────────────────
    # Table metadata
    # ─────────────────────────────────────────────────────────────────

    def get_tables(self) -> list[dict]:
        """
        Fetch the list of accessible tables from ``sys_db_object``.

        Returns a list of dicts with keys ``name`` and ``label``.
        """
        tables: list[dict] = []
        last_id = ""
        while True:
            query = f"sys_id>{last_id}^ORDERBYsys_id"
            resp = self._request(
                "GET",
                "/api/now/table/sys_db_object",
                params={
                    "sysparm_query": query,
                    "sysparm_fields": "sys_id,name,label",
                    "sysparm_limit": FETCH_PAGE_SIZE,
                    "sysparm_no_count": "true",
                    "sysparm_exclude_reference_link": "true",
                },
            )
            resp.raise_for_status()
            page = resp.json().get("result", [])
            if not page:
                break
            tables.extend(
                {"name": t["name"], "label": t.get("label", t["name"])}
                for t in page
            )
            last_id = page[-1]["sys_id"]
        logger.info("Fetched %d table definitions from %s.", len(tables), self.role)
        return tables

    def search_tables(self, search_term: str, limit: int = 50) -> list[dict]:
        """
        Search for tables whose ``name`` contains *search_term*.

        Uses ``nameLIKE`` (case-insensitive contains) on ``sys_db_object``
        and also checks ``label``.  Returns up to *limit* results sorted
        alphabetically by name.

        This is much cheaper than ``get_tables()`` because it filters
        server-side and returns a small result set.
        """
        query = (
            f"nameLIKE{search_term}"
            f"^ORlabelLIKE{search_term}"
            f"^ORDERBYname"
        )
        resp = self._request(
            "GET",
            "/api/now/table/sys_db_object",
            params={
                "sysparm_query": query,
                "sysparm_fields": "name,label",
                "sysparm_limit": limit,
                "sysparm_no_count": "true",
                "sysparm_exclude_reference_link": "true",
            },
        )
        resp.raise_for_status()
        results = resp.json().get("result", [])
        tables = [
            {"name": t["name"], "label": t.get("label", t["name"])}
            for t in results
            if t.get("name")
        ]
        logger.debug(
            "Table search '%s' on %s → %d results.",
            search_term, self.role, len(tables),
        )
        return tables

    # ── Inheritance chain helpers ──────────────────────────────────

    def _get_inheritance_chain(self, table_name: str) -> list[str]:
        """
        Walk the ``super_class`` chain for *table_name* via
        ``sys_db_object`` and return the list of table names from
        child → root (e.g. ``['incident', 'task']``).

        Uses raw sys_id lookups (not labels) for cross-instance
        reliability.
        """
        chain: list[str] = [table_name]
        current = table_name
        visited: set[str] = {table_name}

        for _ in range(10):  # safety limit — no schema > 10 levels deep
            # Get the raw super_class sys_id (not display value)
            resp = self._request(
                "GET",
                "/api/now/table/sys_db_object",
                params={
                    "sysparm_query": f"name={current}",
                    "sysparm_fields": "name,super_class",
                    "sysparm_limit": 1,
                    "sysparm_display_value": "false",
                    "sysparm_exclude_reference_link": "true",
                },
            )
            resp.raise_for_status()
            rows = resp.json().get("result", [])
            if not rows:
                break

            sc = rows[0].get("super_class", "")
            # May be a dict {value, link} or a plain string
            if isinstance(sc, dict):
                parent_sysid = sc.get("value", "").strip()
            else:
                parent_sysid = str(sc).strip()
            if not parent_sysid:
                break

            # Look up the parent table name directly by sys_id
            resp2 = self._request(
                "GET",
                f"/api/now/table/sys_db_object/{parent_sysid}",
                params={"sysparm_fields": "name"},
            )
            if resp2.status_code == 404:
                break
            resp2.raise_for_status()
            parent_name = resp2.json().get("result", {}).get("name", "")
            if not parent_name or parent_name in visited:
                break

            chain.append(parent_name)
            visited.add(parent_name)
            current = parent_name

        logger.info(
            "Inheritance chain for '%s': %s",
            table_name,
            " → ".join(chain),
        )
        return chain

    def get_table_fields(self, table_name: str) -> list[dict]:
        """
        Fetch column metadata for *table_name* from ``sys_dictionary``,
        **including inherited fields** from parent tables.

        Walks the inheritance chain (e.g. incident → task) and queries
        ``sys_dictionary`` for every table in the chain.  Child fields
        override parent fields with the same name.

        Returns a list of dicts::

            {name, label, type, max_length, reference}

        ``reference`` is the table name for reference-type fields
        (e.g. ``"sys_user"`` for ``caller_id``), or ``""`` otherwise.
        """
        chain = self._get_inheritance_chain(table_name)

        # Build a single query for all tables in the chain
        name_filters = "^".join(f"ORname={t}" for t in chain)
        # Remove leading ^OR
        name_filters = name_filters.replace("ORname=", "name=", 1)
        query = f"{name_filters}^elementISNOTEMPTY^ORDERBYelement"

        resp = self._request(
            "GET",
            "/api/now/table/sys_dictionary",
            params={
                "sysparm_query": query,
                "sysparm_fields": (
                    "name,element,column_label,"
                    "internal_type,max_length,reference"
                ),
                "sysparm_limit": 2000,
                "sysparm_no_count": "true",
                "sysparm_exclude_reference_link": "true",
                "sysparm_display_value": "true",
            },
        )
        resp.raise_for_status()
        page = resp.json().get("result", [])

        # Deduplicate: child overrides parent.  Process parent-first
        # so child entries win.
        seen: dict[str, dict] = {}  # element_name → field dict
        chain_priority = {t: i for i, t in enumerate(chain)}  # 0 = child

        for f in page:
            element = f.get("element", "")
            if not element:
                continue

            itype = f.get("internal_type", "")
            if isinstance(itype, dict):
                itype = itype.get("display_value", itype.get("value", ""))
            if str(itype).lower() == "collection":
                continue

            ref = f.get("reference", "")
            if isinstance(ref, dict):
                ref = ref.get("display_value", ref.get("value", ""))

            # Determine priority — lower = higher priority (child wins)
            tbl = f.get("name", "")
            if isinstance(tbl, dict):
                tbl = tbl.get("value", "")
            prio = chain_priority.get(tbl, 999)

            if element not in seen or prio < seen[element]["_prio"]:
                seen[element] = {
                    "name": element,
                    "label": f.get("column_label", element),
                    "type": itype,
                    "max_length": f.get("max_length", ""),
                    "reference": ref,
                    "_prio": prio,
                }

        # Strip the internal _prio key and sort
        fields = []
        for fd in sorted(seen.values(), key=lambda x: x["name"]):
            fd.pop("_prio", None)
            fields.append(fd)

        logger.info(
            "Fetched %d fields for table '%s' (chain: %s) from %s.",
            len(fields),
            table_name,
            " → ".join(chain),
            self.role,
        )
        return fields

    def get_choice_values(self, table_name: str) -> dict[str, list[dict]]:
        """
        Fetch choice values from sys_choice for all fields of a table.
        Returns a dict mapping field name -> list of {"value": str, "label": str}.
        """
        chain = self._get_inheritance_chain(table_name)
        # Build a single query for all tables in the chain
        name_filters = "^".join(f"ORname={t}" for t in chain)
        name_filters = name_filters.replace("ORname=", "name=", 1)
        query = f"{name_filters}^inactive=false^ORDERBYsequence"

        resp = self._request(
            "GET",
            "/api/now/table/sys_choice",
            params={
                "sysparm_query": query,
                "sysparm_fields": "element,value,label",
                "sysparm_limit": 5000,
                "sysparm_no_count": "true",
                "sysparm_exclude_reference_link": "true",
            },
        )
        if resp.status_code == 404:
            # Some orgs might restrict sys_choice, fail gracefully
            return {}
        resp.raise_for_status()

        choices: dict[str, list[dict]] = {}
        for row in resp.json().get("result", []):
            field = row.get("element", "")
            if not field:
                continue
            if field not in choices:
                choices[field] = []
            choices[field].append({
                "value": row.get("value", ""),
                "label": row.get("label", "")
            })

        logger.info(
            "Fetched choices for %d fields on table '%s' from %s.",
            len(choices), table_name, self.role
        )
        return choices

    def get_record_count(self, table_name: str) -> int:
        """
        Return the total record count for *table_name* using the
        aggregate API (single lightweight call).
        """
        resp = self._request(
            "GET",
            f"/api/now/stats/{table_name}",
            params={
                "sysparm_count": "true",
            },
        )
        resp.raise_for_status()
        data = resp.json().get("result", {})
        count = int(data.get("stats", {}).get("count", 0))
        logger.info(
            "Record count for '%s' on %s: %d", table_name, self.role, count
        )
        return count

    # ─────────────────────────────────────────────────────────────────
    # Keyset-paginated fetch
    # ─────────────────────────────────────────────────────────────────

    def fetch_page(
        self,
        table_name: str,
        last_sys_id: str = "",
        limit: int = FETCH_PAGE_SIZE,
        fields: list[str] | None = None,
        extra_query: str = "",
    ) -> list[dict]:
        """
        Fetch a single page of records via keyset pagination.

        Records are ordered by ``sys_id``.  Pass the last ``sys_id``
        from the previous page to continue.
        """
        parts: list[str] = []
        if last_sys_id:
            parts.append(f"sys_id>{last_sys_id}")
        if extra_query:
            parts.append(extra_query)
        parts.append("ORDERBYsys_id")
        query = "^".join(parts)

        params: dict[str, Any] = {
            "sysparm_query": query,
            "sysparm_limit": limit,
            "sysparm_no_count": "true",
            "sysparm_exclude_reference_link": "true",
            "sysparm_display_value": "false",
        }
        if fields:
            params["sysparm_fields"] = ",".join(fields)

        resp = self._request("GET", f"/api/now/table/{table_name}", params=params)
        resp.raise_for_status()

        # Parse JSON — handle truncated / corrupted responses.
        try:
            result = resp.json().get("result", [])
        except Exception as exc:
            logger.error(
                "JSON decode failed for %s (limit=%s, %d bytes): %s",
                table_name, limit, len(resp.content), exc,
            )
            return []

        # ServiceNow may return unexpected types for 'result':
        #   - list[dict]  → normal (multiple records)
        #   - dict        → single record (no wrapping list)
        #   - str         → error message or empty string
        #   - None        → no results
        if result is None:
            return []
        if isinstance(result, str):
            logger.warning(
                "fetch_page got string result instead of list: %r",
                result[:300],
            )
            return []
        if isinstance(result, dict):
            return [result]

        # Filter out non-dict items (SN PDI chunked+gzip truncation
        # can produce valid JSON with corrupted trailing items).
        if result:
            clean = [r for r in result if isinstance(r, dict)]
            if len(clean) < len(result):
                logger.warning(
                    "fetch_page %s: filtered %d non-dict items from %d "
                    "results (truncation detected).",
                    table_name, len(result) - len(clean), len(result),
                )
            return clean

        return result

    # ─────────────────────────────────────────────────────────────────
    # CSV Export Processor
    # ─────────────────────────────────────────────────────────────────

    def csv_export_stream(
        self,
        table_name: str,
        fields: list[str],
        query: str = "",
        timeout: int | None = None,
    ) -> requests.Response:
        """
        Stream a CSV export from ServiceNow's Export Processor.

        Endpoint::

            GET /{table_name}_list.do?CSV
                &sysparm_fields=field1,field2,...
                &sysparm_query=ORDERBYsys_id
                &sysparm_display_value=false

        Returns a streaming ``requests.Response`` object.  The caller
        is responsible for iterating over ``.iter_lines()`` and closing
        the response.

        Parameters
        ----------
        table_name : str
            API name of the table.
        fields : list[str]
            Columns to export.
        query : str
            Encoded query string (e.g. ``"sys_id>abc^ORDERBYsys_id"``).
        timeout : int | None
            Override timeout (default: ``CSV_EXPORT_TIMEOUT``).
        """
        if timeout is None:
            timeout = CSV_EXPORT_TIMEOUT

        url = f"{self.base_url}/{table_name}_list.do"

        params = {
            "CSV": "",
            "sysparm_fields": ",".join(fields),
            "sysparm_query": query,
            "sysparm_display_value": "false",
        }

        t0 = time.perf_counter()
        logger.info(
            "CSV export stream: GET %s_list.do?CSV "
            "(fields=%d, query=%s…)",
            table_name,
            len(fields),
            query[:80],
        )

        response = self.session.get(
            url,
            auth=self.auth,
            params=params,
            stream=True,
            timeout=timeout,
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.tracker.record_call(self.role, response)

        logger.debug(
            "CSV export response: %s (%.0f ms, Content-Length: %s)",
            response.status_code,
            elapsed_ms,
            response.headers.get("Content-Length", "streaming"),
        )

        if not response.ok:
            logger.error(
                "CSV export failed: HTTP %s — %s",
                response.status_code,
                response.text[:500] if not response.headers.get(
                    "Transfer-Encoding"
                ) else "(streaming response)",
            )
            response.raise_for_status()

        return response

    # ─────────────────────────────────────────────────────────────────
    # Batch API
    # ─────────────────────────────────────────────────────────────────

    def batch_request(self, operations: list[dict]) -> list[dict]:
        """
        Send a batch of REST operations in a single HTTP call.

        Each element of *operations* is a dict::

            {"method": "POST", "url": "/api/now/table/...", "body": "..."}

        Returns the list of per-operation response dicts from ServiceNow.
        """
        import base64
        
        # ServiceNow requires the body of POST/PUT/PATCH batch requests
        # to be Base64 encoded.
        encoded_ops = []
        for op in operations:
            op_copy = dict(op)
            if "body" in op_copy and isinstance(op_copy["body"], str):
                op_copy["body"] = base64.b64encode(
                    op_copy["body"].encode("utf-8")
                ).decode("utf-8")
            encoded_ops.append(op_copy)

        payload = {
            "batch_request_id": f"migration_{int(time.time())}",
            "rest_requests": encoded_ops,
        }
        resp = self._request(
            "POST",
            "/api/now/v1/batch",
            json_body=payload,
            timeout=max(REQUEST_TIMEOUT, 300),  # generous for big batches
        )
        if not resp.ok:
            logger.error(
                "Batch API error %d: %s", resp.status_code, resp.text[:2000]
            )
            resp.raise_for_status()
        return resp.json().get("serviced_requests", [])

    # ─────────────────────────────────────────────────────────────────
    # Convenience: single-record insert (for small jobs / fallback)
    # ─────────────────────────────────────────────────────────────────

    def insert_record(self, table_name: str, record: dict) -> dict:
        resp = self._request(
            "POST", f"/api/now/table/{table_name}", json_body=record
        )
        resp.raise_for_status()
        return resp.json().get("result", {}) if resp.text.strip() else {}

    def update_record(self, table_name: str, sys_id: str, fields: dict) -> dict:
        resp = self._request(
            "PATCH", f"/api/now/table/{table_name}/{sys_id}", json_body=fields
        )
        resp.raise_for_status()
        return resp.json().get("result", {}) if resp.text.strip() else {}

    # ─────────────────────────────────────────────────────────────────
    # JSONv2 bulk operations (legacy but available on ALL instances)
    # ─────────────────────────────────────────────────────────────────

    def jsonv2_insert_multiple(
        self, table_name: str, records: list[dict]
    ) -> dict:
        """
        Insert multiple records in a **single** API call via the
        legacy JSONv2 endpoint.

        Endpoint::

            POST /{table}.do?JSONv2&sysparm_action=insertMultiple

        Body::

            {"records": [{...}, {...}, ...]}

        Returns the parsed JSON response containing a ``records`` list,
        each with ``__status`` == ``"success"`` or an error message.
        """
        resp = self._request(
            "POST",
            f"/{table_name}.do?JSONv2&sysparm_action=insertMultiple",
            json_body={"records": records},
            timeout=max(REQUEST_TIMEOUT, 600),  # generous for large batches
        )
        if not resp.ok:
            logger.error(
                "JSONv2 insertMultiple error %d: %s",
                resp.status_code, resp.text[:2000],
            )
            resp.raise_for_status()
        try:
            return resp.json()
        except Exception as e:
            logger.error("JSONv2 insertMultiple failed to parse response: %s. Response text: %s", e, resp.text[:500])
            # Return a synthetic response so loader.py marks all records as failed
            return {
                "records": [
                    {"__status": "error", "__error": f"SN returned invalid JSON (likely overloaded). {e}"}
                    for _ in records
                ]
            }

    # ─────────────────────────────────────────────────────────────────
    # Schema Modification
    # ─────────────────────────────────────────────────────────────────

    def create_legacy_sysid_field(self, table_name: str) -> dict:
        """
        Creates the `u_legacy_sysid` field on the given target table.
        It uses the sys_dictionary table to insert the field definition.
        """
        payload = {
            "name": table_name,
            "element": "u_legacy_sysid",
            "internal_type": "string",
            "column_label": "Legacy Sys ID",
            "max_length": "40"
        }
        resp = self._request(
            "POST",
            "/api/now/table/sys_dictionary",
            json_body=payload,
        )
        if not resp.ok:
            logger.error("Failed to create u_legacy_sysid field on %s: %s", table_name, resp.text)
            resp.raise_for_status()
        return resp.json().get("result", {})
