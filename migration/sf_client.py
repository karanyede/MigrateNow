"""
Salesforce REST API client.

Wraps ``requests.Session`` with:
* OAuth2 Username-Password authentication
* Automatic retry on 429 / 5xx with exponential back-off
* Sforce-Limit-Info header parsing via ``RateTracker``
* Bulk API 2.0 (Query + Ingest) support
* SObject Collections (composite) support
* Structured DEBUG logging for every request/response
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_FACTOR,
    SF_API_VERSION,
    SF_BULK_POLL_INTERVAL,
    SF_COLLECTIONS_BATCH_SIZE,
)
from migration.rate_tracker import RateTracker

logger = logging.getLogger("sn_migration")


class SalesforceClient:
    """
    Reusable HTTP client for a **single** Salesforce org.

    Parameters
    ----------
    login_url : str
        ``"https://login.salesforce.com"`` or ``"https://test.salesforce.com"``.
    client_id, client_secret : str
        Connected App OAuth credentials.
    username, password : str
        Salesforce user credentials.
    security_token : str
        User's security token (appended to password for auth).
    role : str
        ``"source"`` or ``"target"`` – passed to ``RateTracker``.
    tracker : RateTracker
        Shared tracker across source/target clients.
    """

    def __init__(
        self,
        login_url: str,
        username: str,
        password: str,
        security_token: str,
        role: str,
        tracker: RateTracker,
        client_id: str = "",
        client_secret: str = "",
    ) -> None:
        self.login_url = login_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.security_token = security_token or ""
        self.role = role
        self.tracker = tracker

        self.access_token: str = ""
        self.instance_url: str = ""
        self._api_base: str = ""
        self._objects_cache: list[dict] | None = None

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

        # Authenticate on init
        self._authenticate()

    # ─── Properties ──────────────────────────────────────────────────

    @property
    def instance(self) -> str:
        """Return just the hostname from instance_url for display."""
        if self.instance_url:
            return urlparse(self.instance_url).hostname or self.instance_url
        return self.login_url

    # ─────────────────────────────────────────────────────────────────
    # Authentication
    # ─────────────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        """Pick the best auth method and authenticate."""
        if self.client_id and self.client_secret:
            self._authenticate_oauth()
        else:
            self._authenticate_soap()

    def _authenticate_soap(self) -> None:
        """
        SOAP Partner API login — no Connected App required.

        POST {login_url}/services/Soap/u/{api_version}
        Only needs username + password + security_token.
        """
        from xml.sax.saxutils import escape as xml_escape
        import xml.etree.ElementTree as ET

        url = f"{self.login_url}/services/Soap/u/{SF_API_VERSION}"
        combined_password = xml_escape(
            f"{self.password}{self.security_token}"
        )
        username_escaped = xml_escape(self.username)

        soap_envelope = (
            '<?xml version="1.0" encoding="utf-8" ?>'
            '<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
            ' xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
            '<env:Body>'
            '<n1:login xmlns:n1="urn:partner.soap.sforce.com">'
            f'<n1:username>{username_escaped}</n1:username>'
            f'<n1:password>{combined_password}</n1:password>'
            '</n1:login>'
            '</env:Body>'
            '</env:Envelope>'
        )

        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "login",
        }

        t0 = time.perf_counter()
        # Disable auto-redirect: Salesforce may 302 to the org domain,
        # and requests silently converts POST→GET on 302, causing 405.
        resp = self.session.post(
            url, data=soap_envelope, headers=headers, timeout=REQUEST_TIMEOUT,
            allow_redirects=False,
        )
        # Manually follow redirect with POST
        if resp.status_code in (301, 302, 303, 307, 308):
            redirect_url = resp.headers.get("Location", "")
            if redirect_url:
                logger.debug("SOAP login redirected to %s", redirect_url)
                resp = self.session.post(
                    redirect_url, data=soap_envelope, headers=headers,
                    timeout=REQUEST_TIMEOUT, allow_redirects=False,
                )
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if not resp.ok:
            error_detail = resp.text[:500]
            logger.error(
                "SF SOAP login failed (HTTP %s): %s",
                resp.status_code, error_detail,
            )
            raise ConnectionError(
                f"Salesforce login failed: HTTP {resp.status_code} — {error_detail}"
            )

        # Parse SOAP XML response
        root = ET.fromstring(resp.text)

        # Check for SOAP fault
        fault_el = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")
        if fault_el is not None:
            faultstring = fault_el.findtext("faultstring", "Unknown fault")
            logger.error("SF SOAP fault: %s", faultstring)
            raise ConnectionError(f"Salesforce login failed: {faultstring}")

        ns = "urn:partner.soap.sforce.com"
        session_id = root.findtext(f".//{{{ns}}}sessionId")
        server_url = root.findtext(f".//{{{ns}}}serverUrl")

        if not session_id or not server_url:
            raise ConnectionError(
                "Salesforce SOAP login: missing sessionId or serverUrl."
            )

        self.access_token = session_id
        # serverUrl: https://instance.my.salesforce.com/services/Soap/u/62.0/00D…
        parsed = urlparse(server_url)
        self.instance_url = f"{parsed.scheme}://{parsed.netloc}"
        self._api_base = f"{self.instance_url}/services/data/v{SF_API_VERSION}"

        logger.info(
            "SF SOAP login OK as '%s' on %s (%.0f ms)",
            self.username, self.instance, elapsed_ms,
        )

    def _authenticate_oauth(self) -> None:
        """
        OAuth2 Username-Password flow (requires Connected App).

        POST {login_url}/services/oauth2/token
        """
        url = f"{self.login_url}/services/oauth2/token"
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": f"{self.password}{self.security_token}",
        }

        t0 = time.perf_counter()
        resp = self.session.post(url, data=payload, timeout=REQUEST_TIMEOUT)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if not resp.ok:
            error_detail = resp.text[:500]
            logger.error(
                "SF OAuth2 authentication failed (HTTP %s): %s",
                resp.status_code, error_detail,
            )
            raise ConnectionError(
                f"Salesforce OAuth2 auth failed: HTTP {resp.status_code} — {error_detail}"
            )

        data = resp.json()
        self.access_token = data["access_token"]
        self.instance_url = data["instance_url"]
        self._api_base = f"{self.instance_url}/services/data/v{SF_API_VERSION}"

        logger.info(
            "SF OAuth2 authenticated as '%s' on %s (%.0f ms)",
            self.username, self.instance, elapsed_ms,
        )

    def _refresh_token(self) -> None:
        """Re-authenticate when the current token has expired."""
        logger.info("SF token expired, re-authenticating…")
        self._authenticate()

    # ─────────────────────────────────────────────────────────────────
    # Core HTTP helpers
    # ─────────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: Any = None,
        data_body: str | None = None,
        content_type: str = "application/json",
        timeout: int = REQUEST_TIMEOUT,
        _retry_auth: bool = True,
    ) -> requests.Response:
        """
        Execute an HTTP request against the Salesforce instance.

        Handles Bearer auth, rate tracking, and auto-refresh on 401.
        """
        url = f"{self.instance_url}{path}" if path.startswith("/") else path

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type,
        }
        if content_type == "application/json":
            headers["Accept"] = "application/json"

        t0 = time.perf_counter()

        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_body if content_type == "application/json" and json_body else None,
            data=data_body if data_body else (
                json.dumps(json_body) if json_body and content_type != "application/json" else None
            ),
            timeout=timeout,
        )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.tracker.record_call(self.role, response)

        logger.debug(
            "SF API %s %s → %s (%.0f ms, %s bytes)",
            method,
            path[:100],
            response.status_code,
            elapsed_ms,
            len(response.content),
        )

        # Auto-refresh token on 401
        if response.status_code == 401 and _retry_auth:
            self._refresh_token()
            return self._request(
                method, path, params, json_body, data_body,
                content_type, timeout, _retry_auth=False,
            )

        return response

    # ─────────────────────────────────────────────────────────────────
    # Object metadata
    # ─────────────────────────────────────────────────────────────────

    def get_objects(self) -> list[dict]:
        """
        Fetch the list of all SObjects in the org.

        Returns a list of dicts with keys: name, label, queryable, createable.
        Results are cached for efficient search_objects() filtering.
        """
        if self._objects_cache is not None:
            return self._objects_cache

        resp = self._request("GET", f"{self._api_base}/sobjects/")
        resp.raise_for_status()
        sobjects = resp.json().get("sobjects", [])

        self._objects_cache = [
            {
                "name": obj["name"],
                "label": obj.get("label", obj["name"]),
                "queryable": obj.get("queryable", False),
                "createable": obj.get("createable", False),
            }
            for obj in sobjects
        ]

        logger.info(
            "Fetched %d SObject definitions from %s.",
            len(self._objects_cache),
            self.role,
        )
        return self._objects_cache

    def search_objects(self, search_term: str, limit: int = 50) -> list[dict]:
        """
        Search for SObjects whose name or label contains *search_term*.

        Uses client-side filtering of the cached objects list.
        Returns up to *limit* results.
        """
        objects = self.get_objects()
        term = search_term.lower()
        matches = [
            {"name": obj["name"], "label": obj["label"]}
            for obj in objects
            if term in obj["name"].lower() or term in obj["label"].lower()
        ]
        # Sort: exact name match first, then starts-with, then contains
        matches.sort(key=lambda x: (
            0 if x["name"].lower() == term else
            1 if x["name"].lower().startswith(term) else 2,
            x["name"],
        ))
        return matches[:limit]

    def get_object_fields(self, object_name: str) -> list[dict]:
        """
        Fetch field metadata for *object_name* via the Describe endpoint.

        Returns a list of dicts with keys matching ServiceNow's format:
        name, label, type, max_length, reference, externalId, createable, updateable.
        """
        resp = self._request(
            "GET",
            f"{self._api_base}/sobjects/{object_name}/describe/",
        )
        resp.raise_for_status()
        sf_fields = resp.json().get("fields", [])

        fields = []
        for f in sf_fields:
            ref_to = f.get("referenceTo", [])
            reference = ref_to[0] if ref_to else ""
            fields.append({
                "name": f["name"],
                "label": f.get("label", f["name"]),
                "type": f.get("type", ""),
                "max_length": str(f.get("length", "")),
                "reference": reference,
                "externalId": f.get("externalId", False),
                "createable": f.get("createable", False),
                "updateable": f.get("updateable", False),
            })

        # Sort alphabetically by name
        fields.sort(key=lambda x: x["name"])

        logger.info(
            "Fetched %d fields for SObject '%s' from %s.",
            len(fields),
            object_name,
            self.role,
        )
        return fields

    def get_record_count(self, object_name: str) -> int:
        """Return the total record count for *object_name*."""
        soql = f"SELECT COUNT() FROM {object_name}"
        resp = self._request(
            "GET",
            f"{self._api_base}/query/",
            params={"q": soql},
        )
        resp.raise_for_status()
        count = resp.json().get("totalSize", 0)
        logger.info(
            "Record count for '%s' on %s: %d",
            object_name,
            self.role,
            count,
        )
        return count

    # ─────────────────────────────────────────────────────────────────
    # REST Query (small datasets / fallback)
    # ─────────────────────────────────────────────────────────────────

    def query(self, soql: str) -> list[dict]:
        """
        Execute a SOQL query with nextRecordsUrl pagination.

        Returns all records as a flat list[dict].
        """
        records: list[dict] = []
        resp = self._request(
            "GET",
            f"{self._api_base}/query/",
            params={"q": soql},
        )
        if not resp.ok:
            error_detail = resp.text[:800]
            logger.error(
                "SOQL query failed (HTTP %s): %s\nSOQL: %s",
                resp.status_code, error_detail, soql,
            )
            raise RuntimeError(
                f"SOQL query failed (HTTP {resp.status_code}): {error_detail}"
            )
        data = resp.json()
        records.extend(data.get("records", []))

        # Follow nextRecordsUrl for pagination
        while not data.get("done", True):
            next_url = data["nextRecordsUrl"]
            resp = self._request("GET", f"{self.instance_url}{next_url}")
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))

        # Clean SF metadata from records
        for rec in records:
            rec.pop("attributes", None)

        logger.info("SOQL query returned %d records.", len(records))
        return records

    # ─────────────────────────────────────────────────────────────────
    # Bulk API 2.0 — Query
    # ─────────────────────────────────────────────────────────────────

    def bulk_query_create(
        self,
        object_name: str,
        fields: list[str],
        query_filter: str = "",
    ) -> str:
        """
        Create a Bulk API 2.0 Query job.

        Returns the job ID.
        """
        field_list = ",".join(fields)
        soql = f"SELECT {field_list} FROM {object_name}"
        if query_filter:
            soql += f" WHERE {query_filter}"
        soql += " ORDER BY Id"

        resp = self._request(
            "POST",
            f"{self._api_base}/jobs/query",
            json_body={
                "operation": "query",
                "query": soql,
            },
        )
        resp.raise_for_status()
        job_id = resp.json()["id"]
        logger.info(
            "Bulk Query job created: %s (SOQL: %s…)",
            job_id,
            soql[:100],
        )
        return job_id

    def bulk_query_poll(self, job_id: str) -> dict:
        """
        Poll a Bulk API 2.0 Query job until completion.

        Uses exponential backoff: 2s → 4s → 8s → ... max 30s.
        Returns the final job info dict.
        """
        interval = SF_BULK_POLL_INTERVAL
        max_interval = 30

        while True:
            resp = self._request(
                "GET",
                f"{self._api_base}/jobs/query/{job_id}",
            )
            resp.raise_for_status()
            job = resp.json()
            state = job.get("state", "")

            if state == "JobComplete":
                logger.info(
                    "Bulk Query job %s complete: %s records processed.",
                    job_id,
                    job.get("numberRecordsProcessed", "?"),
                )
                return job
            elif state in ("Failed", "Aborted"):
                error = job.get("errorMessage", "Unknown error")
                logger.error(
                    "Bulk Query job %s failed: %s", job_id, error
                )
                raise RuntimeError(
                    f"Bulk Query job {job_id} {state}: {error}"
                )

            logger.debug(
                "Bulk Query job %s state: %s, waiting %ds…",
                job_id, state, interval,
            )
            time.sleep(interval)
            interval = min(interval * 2, max_interval)

    def bulk_query_results(self, job_id: str) -> list[dict]:
        """
        Download results from a completed Bulk API 2.0 Query job.

        Handles locator-based pagination for large result sets.
        Returns all records as list[dict].
        """
        all_records: list[dict] = []
        locator = ""
        chunk = 0

        while True:
            chunk += 1
            params = {}
            if locator:
                params["locator"] = locator

            resp = self._request(
                "GET",
                f"{self._api_base}/jobs/query/{job_id}/results",
                params=params,
            )
            resp.raise_for_status()

            # Parse CSV response
            csv_text = resp.text
            if csv_text.strip():
                reader = csv.DictReader(io.StringIO(csv_text))
                chunk_records = list(reader)
                all_records.extend(chunk_records)
                logger.debug(
                    "Bulk Query results chunk %d: %d records.",
                    chunk, len(chunk_records),
                )

            # Check for more results
            locator = resp.headers.get("Sforce-Locator", "")
            if not locator or locator == "null":
                break

        logger.info(
            "Bulk Query job %s: downloaded %d total records in %d chunks.",
            job_id, len(all_records), chunk,
        )
        return all_records

    # ─────────────────────────────────────────────────────────────────
    # Bulk API 2.0 — Ingest
    # ─────────────────────────────────────────────────────────────────

    def bulk_ingest_create(
        self,
        object_name: str,
        operation: str,
        external_id_field: str | None = None,
    ) -> str:
        """
        Create a Bulk API 2.0 Ingest job.

        Parameters
        ----------
        object_name : str
            SObject API name.
        operation : str
            ``"insert"``, ``"update"``, or ``"upsert"``.
        external_id_field : str | None
            Required for upsert operations.

        Returns the job ID.
        """
        body: dict = {
            "object": object_name,
            "operation": operation,
            "contentType": "CSV",
            "lineEnding": "LF",
        }
        if external_id_field and operation == "upsert":
            body["externalIdFieldName"] = external_id_field

        resp = self._request(
            "POST",
            f"{self._api_base}/jobs/ingest",
            json_body=body,
        )
        resp.raise_for_status()
        job_id = resp.json()["id"]
        logger.info(
            "Bulk Ingest job created: %s (operation=%s, object=%s)",
            job_id, operation, object_name,
        )
        return job_id

    def bulk_ingest_upload(self, job_id: str, csv_data: str) -> None:
        """Upload CSV data to a Bulk API 2.0 Ingest job."""
        resp = self._request(
            "PUT",
            f"{self._api_base}/jobs/ingest/{job_id}/batches",
            data_body=csv_data,
            content_type="text/csv",
        )
        resp.raise_for_status()
        logger.info(
            "Bulk Ingest job %s: uploaded %d bytes of CSV data.",
            job_id, len(csv_data),
        )

    def bulk_ingest_close(self, job_id: str) -> None:
        """Set Bulk Ingest job state to UploadComplete."""
        resp = self._request(
            "PATCH",
            f"{self._api_base}/jobs/ingest/{job_id}",
            json_body={"state": "UploadComplete"},
        )
        resp.raise_for_status()
        logger.info("Bulk Ingest job %s: state set to UploadComplete.", job_id)

    def bulk_ingest_poll(self, job_id: str) -> dict:
        """
        Poll a Bulk API 2.0 Ingest job until completion.

        Returns the final job info dict.
        """
        interval = SF_BULK_POLL_INTERVAL
        max_interval = 30

        while True:
            resp = self._request(
                "GET",
                f"{self._api_base}/jobs/ingest/{job_id}",
            )
            resp.raise_for_status()
            job = resp.json()
            state = job.get("state", "")

            if state == "JobComplete":
                logger.info(
                    "Bulk Ingest job %s complete: %s processed, "
                    "%s failed.",
                    job_id,
                    job.get("numberRecordsProcessed", "?"),
                    job.get("numberRecordsFailed", "?"),
                )
                return job
            elif state in ("Failed", "Aborted"):
                error = job.get("errorMessage", "Unknown error")
                logger.error(
                    "Bulk Ingest job %s failed: %s", job_id, error
                )
                raise RuntimeError(
                    f"Bulk Ingest job {job_id} {state}: {error}"
                )

            logger.debug(
                "Bulk Ingest job %s state: %s, waiting %ds…",
                job_id, state, interval,
            )
            time.sleep(interval)
            interval = min(interval * 2, max_interval)

    def bulk_ingest_results(self, job_id: str) -> dict:
        """
        Get success/failure/unprocessed results from a completed
        Bulk Ingest job.

        Returns dict with keys: successful, failed, unprocessed.
        """
        results = {}

        for result_type in ("successfulResults", "failedResults", "unprocessedrecords"):
            resp = self._request(
                "GET",
                f"{self._api_base}/jobs/ingest/{job_id}/{result_type}",
            )
            if resp.ok and resp.text.strip():
                reader = csv.DictReader(io.StringIO(resp.text))
                results[result_type] = list(reader)
            else:
                results[result_type] = []

        logger.info(
            "Bulk Ingest job %s results: %d successful, %d failed, "
            "%d unprocessed.",
            job_id,
            len(results.get("successfulResults", [])),
            len(results.get("failedResults", [])),
            len(results.get("unprocessedrecords", [])),
        )
        return results

    # ─────────────────────────────────────────────────────────────────
    # SObject Collections (composite)
    # ─────────────────────────────────────────────────────────────────

    def sobject_collections_create(
        self,
        object_name: str,
        records: list[dict],
    ) -> list[dict]:
        """
        Insert records via SObject Collections (200 per call).

        Each record dict should contain field values (no Id).
        Returns list of result dicts with {id, success, errors}.
        """
        # Add sObject type attribute to each record
        payload_records = []
        for rec in records:
            r = dict(rec)
            r["attributes"] = {"type": object_name}
            payload_records.append(r)

        resp = self._request(
            "POST",
            f"{self._api_base}/composite/sobjects",
            json_body={
                "allOrNone": False,
                "records": payload_records,
            },
        )
        resp.raise_for_status()
        return resp.json()

    def sobject_collections_update(
        self,
        object_name: str,
        records: list[dict],
    ) -> list[dict]:
        """
        Update records via SObject Collections (200 per call).

        Each record dict MUST contain 'Id' for matching.
        Returns list of result dicts.
        """
        payload_records = []
        for rec in records:
            r = dict(rec)
            r["attributes"] = {"type": object_name}
            payload_records.append(r)

        resp = self._request(
            "PATCH",
            f"{self._api_base}/composite/sobjects",
            json_body={
                "allOrNone": False,
                "records": payload_records,
            },
        )
        resp.raise_for_status()
        return resp.json()
