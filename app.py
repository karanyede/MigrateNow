"""
Flask application — web UI for the multi-platform migration tool.

Routes implement a wizard flow:
  0. /              → Home (migration type selector)
  1. /connect       → Enter source + target credentials
  2. /tables        → Select source and target tables/objects
  3. /fields        → Map source fields to target fields
  4. /migrate       → Show progress page
  5. /migrate/stream → SSE endpoint for real-time progress
  6. /history       → Migration history
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
import queue
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from config import (
    FLASK_PORT,
    FLASK_SECRET,
    SOURCE_INSTANCE,
    SOURCE_PASSWORD,
    SOURCE_USERNAME,
    TARGET_INSTANCE,
    TARGET_PASSWORD,
    TARGET_USERNAME,
    SF_SOURCE_LOGIN_URL,
    SF_SOURCE_USERNAME,
    SF_SOURCE_PASSWORD,
    SF_SOURCE_SECURITY_TOKEN,
    SF_TARGET_LOGIN_URL,
    SF_TARGET_USERNAME,
    SF_TARGET_PASSWORD,
    SF_TARGET_SECURITY_TOKEN,
    setup_logging,
)
from migration.client import ServiceNowClient
from migration.sf_client import SalesforceClient
from migration.migrator import MigrationOrchestrator
from migration.rate_tracker import RateTracker

# ── Bootstrap ────────────────────────────────────────────────────────
logger = setup_logging()

app = Flask(__name__)
app.secret_key = FLASK_SECRET

@app.template_filter('numberFormat')
def number_format(value):
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return value

# Thread-safe queue for SSE progress messages.
# One queue per session would be ideal but for a single-user tool this
# is sufficient.
_progress_q: queue.Queue = queue.Queue()

# Data file paths
HISTORY_FILE     = Path(__file__).parent / "data" / "migration_history.json"
CONNECTIONS_FILE = Path(__file__).parent / "data" / "connections.json"


def _load_history() -> list[dict]:
    """Load migration history from JSON file."""
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_history(history: list[dict]) -> None:
    """Save migration history to JSON file."""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(
        json.dumps(history, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _append_history(entry: dict) -> None:
    """Append a migration result to history."""
    history = _load_history()
    history.insert(0, entry)  # newest first
    _save_history(history)


# ── Connections persistence ──────────────────────────────────────────

def _load_connections() -> list[dict]:
    """Load saved connections from JSON file."""
    if CONNECTIONS_FILE.exists():
        try:
            return json.loads(CONNECTIONS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_connections(conns: list[dict]) -> None:
    """Persist saved connections to JSON file."""
    CONNECTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONNECTIONS_FILE.write_text(
        json.dumps(conns, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _next_conn_id(conns: list[dict]) -> str:
    """Generate next sequential connection ID."""
    import uuid
    return str(uuid.uuid4())[:8]


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _build_client(role: str) -> ServiceNowClient:
    """Build a ServiceNowClient from session credentials."""
    tracker = _get_tracker()
    if role == "source":
        return ServiceNowClient(
            instance=session["source_instance"],
            username=session["source_username"],
            password=session["source_password"],
            role="source",
            tracker=tracker,
        )
    else:
        return ServiceNowClient(
            instance=session["target_instance"],
            username=session["target_username"],
            password=session["target_password"],
            role="target",
            tracker=tracker,
        )


def _build_sf_client(role: str) -> SalesforceClient:
    """Build a SalesforceClient from session credentials."""
    tracker = _get_tracker()
    prefix = f"sf_{role}_"
    return SalesforceClient(
        login_url=session[f"{prefix}login_url"],
        username=session[f"{prefix}username"],
        password=session[f"{prefix}password"],
        security_token=session.get(f"{prefix}security_token", ""),
        role=role,
        tracker=tracker,
    )


def _get_tracker() -> RateTracker:
    """Return (or create) a per-session RateTracker stored on `app`."""
    if not hasattr(app, "_rate_tracker"):
        app._rate_tracker = RateTracker()
    return app._rate_tracker


def _auto_map_fields(
    source_fields: list[dict], target_fields: list[dict]
) -> dict[str, str]:
    """
    Auto-map source → target fields by **exact name match** (case-insensitive).

    Returns ``{source_name: target_name}`` for every match.
    """
    target_lookup: dict[str, str] = {
        f["name"].lower(): f["name"] for f in target_fields
    }
    mapping: dict[str, str] = {}
    for sf in source_fields:
        key = sf["name"].lower()
        if key in target_lookup:
            mapping[sf["name"]] = target_lookup[key]
    return mapping


# ─────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def home():
    """Home page — MigrateNow dashboard with stats and recent history."""
    return render_template("home.html", history=_load_history())


@app.route("/select-type", methods=["POST"])
def select_type():
    """Store migration_type in session and redirect to connect."""
    mt = request.form.get("migration_type", "sn_sn")
    if mt not in ("sn_sn", "sn_sf", "sf_sf", "sf_sn"):
        mt = "sn_sn"
    session["migration_type"] = mt
    return redirect(url_for("connect_page"))


@app.route("/connect", methods=["GET"])
def connect_page():
    """Render connect page with appropriate credential fields."""
    mt = session.get("migration_type", "sn_sn")
    return render_template(
        "index.html",
        migration_type=mt,
        saved_connections=_load_connections(),
        # SN credentials
        source_instance=session.get("source_instance", ""),
        source_username=session.get("source_username", ""),
        source_password=session.get("source_password", ""),
        target_instance=session.get("target_instance", ""),
        target_username=session.get("target_username", ""),
        target_password=session.get("target_password", ""),
        # SF source credentials
        sf_source_login_url=session.get("sf_source_login_url", "https://login.salesforce.com"),
        sf_source_username=session.get("sf_source_username", ""),
        sf_source_password=session.get("sf_source_password", ""),
        sf_source_security_token=session.get("sf_source_security_token", ""),
        # SF target credentials
        sf_target_login_url=session.get("sf_target_login_url", "https://login.salesforce.com"),
        sf_target_username=session.get("sf_target_username", ""),
        sf_target_password=session.get("sf_target_password", ""),
        sf_target_security_token=session.get("sf_target_security_token", ""),
    )



@app.route("/connect", methods=["POST"])
def connect():
    """Validate credentials and redirect to table selection."""
    source_platform = request.form.get("source_platform", "sn").strip()
    target_platform = request.form.get("target_platform", "sn").strip()
    
    mt = f"{source_platform}_{target_platform}"
    session["migration_type"] = mt

    source_is_sn = source_platform == "sn"
    source_is_sf = source_platform == "sf"
    target_is_sn = target_platform == "sn"
    target_is_sf = target_platform == "sf"

    # ── Store SN credentials ──────────────────────────────────────
    if source_is_sn:
        inst = request.form.get("source_instance", "").strip()
        user = request.form.get("source_username", "").strip()
        pw   = request.form.get("source_password", "")
        if not pw or pw == "••••••••":
            for c in _load_connections():
                if c.get("platform") == "sn" and c.get("instance", "").strip() == inst and c.get("username", "").strip() == user:
                    pw = c.get("password", "")
                    break
        session["source_instance"] = inst
        session["source_username"] = user
        session["source_password"] = pw

    if target_is_sn:
        inst = request.form.get("target_instance", "").strip()
        user = request.form.get("target_username", "").strip()
        pw   = request.form.get("target_password", "")
        if not pw or pw == "••••••••":
            for c in _load_connections():
                if c.get("platform") == "sn" and c.get("instance", "").strip() == inst and c.get("username", "").strip() == user:
                    pw = c.get("password", "")
                    break
        session["target_instance"] = inst
        session["target_username"] = user
        session["target_password"] = pw

    # ── Store SF credentials ──────────────────────────────────────
    if source_is_sf:
        login_url      = request.form.get("sf_source_login_url", "https://login.salesforce.com").strip()
        username       = request.form.get("sf_source_username", "").strip()
        password       = request.form.get("sf_source_password", "")
        security_token = request.form.get("sf_source_security_token", "")
        if not password or password == "••••••••":
            for c in _load_connections():
                if c.get("platform") == "sf" and c.get("login_url", "").strip() == login_url and c.get("username", "").strip() == username:
                    password = c.get("password", "")
                    if not security_token or security_token == "••••••••":
                        security_token = c.get("security_token", "")
                    break
        session["sf_source_login_url"]      = login_url
        session["sf_source_username"]       = username
        session["sf_source_password"]       = password
        session["sf_source_security_token"] = security_token

    if target_is_sf:
        login_url      = request.form.get("sf_target_login_url", "https://login.salesforce.com").strip()
        username       = request.form.get("sf_target_username", "").strip()
        password       = request.form.get("sf_target_password", "")
        security_token = request.form.get("sf_target_security_token", "")
        if not password or password == "••••••••":
            for c in _load_connections():
                if c.get("platform") == "sf" and c.get("login_url", "").strip() == login_url and c.get("username", "").strip() == username:
                    password = c.get("password", "")
                    if not security_token or security_token == "••••••••":
                        security_token = c.get("security_token", "")
                    break
        session["sf_target_login_url"]      = login_url
        session["sf_target_username"]       = username
        session["sf_target_password"]       = password
        session["sf_target_security_token"] = security_token

    # ── Connectivity checks ───────────────────────────────────────
    # Source
    if source_is_sn:
        try:
            src = _build_client("source")
            src.fetch_page("sys_db_object", limit=1, fields=["name"])
            logger.info("Source SN connection OK: %s", session["source_instance"])
        except Exception as exc:
            flash(f"Source SN connection failed: {exc}", "error")
            logger.error("Source SN connection failed: %s", exc)
            return redirect(url_for("connect_page"))
    elif source_is_sf:
        try:
            sf_src = _build_sf_client("source")
            sf_src.get_objects()  # validates auth + fetches object list
            logger.info("Source SF connection OK: %s", sf_src.instance)
        except Exception as exc:
            flash(f"Source SF connection failed: {exc}", "error")
            logger.error("Source SF connection failed: %s", exc)
            return redirect(url_for("connect_page"))

    # Target
    if target_is_sn:
        try:
            tgt = _build_client("target")
            tgt.fetch_page("sys_db_object", limit=1, fields=["name"])
            logger.info("Target SN connection OK: %s", session["target_instance"])
        except Exception as exc:
            flash(f"Target SN connection failed: {exc}", "error")
            logger.error("Target SN connection failed: %s", exc)
            return redirect(url_for("connect_page"))
    elif target_is_sf:
        try:
            sf_tgt = _build_sf_client("target")
            sf_tgt.get_objects()
            logger.info("Target SF connection OK: %s", sf_tgt.instance)
        except Exception as exc:
            flash(f"Target SF connection failed: {exc}", "error")
            logger.error("Target SF connection failed: %s", exc)
            return redirect(url_for("connect_page"))

    flash("Connected to both instances.", "success")
    return redirect(url_for("tables"))


@app.route("/tables", methods=["GET"])
def tables():
    """Show table/object selection page."""
    mt = session.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")

    # Require at least one instance to be connected
    if source_is_sn and "source_instance" not in session:
        flash("Please connect first.", "error")
        return redirect(url_for("connect_page"))
    if not source_is_sn and "sf_source_login_url" not in session:
        flash("Please connect first.", "error")
        return redirect(url_for("connect_page"))

    return render_template(
        "tables.html",
        migration_type=mt,
        selected_source=session.get("source_table", ""),
        selected_target=session.get("target_table", ""),
    )


@app.route("/tables", methods=["POST"])
def select_tables():
    """Store selected tables and redirect to field mapping."""
    session["source_table"] = request.form["source_table"].strip()
    session["target_table"] = request.form["target_table"].strip()
    if not session["source_table"] or not session["target_table"]:
        flash("Please select both source and target tables.", "error")
        return redirect(url_for("tables"))
    return redirect(url_for("fields"))


@app.route("/api/search_tables")
def api_search_tables():
    """AJAX endpoint — search for SN tables by name on source or target."""
    q = request.args.get("q", "").strip()
    instance = request.args.get("instance", "source")

    if len(q) < 2:
        return jsonify([])

    try:
        client = _build_client(instance)
        results = client.search_tables(q, limit=50)
        return jsonify(results)
    except Exception as exc:
        logger.error("SN table search failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/search_objects")
def api_search_objects():
    """AJAX endpoint — search for SF objects by name on source or target."""
    q = request.args.get("q", "").strip()
    instance = request.args.get("instance", "source")

    if len(q) < 2:
        return jsonify([])

    try:
        client = _build_sf_client(instance)
        results = client.search_objects(q, limit=50)
        return jsonify(results)
    except Exception as exc:
        logger.error("SF object search failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/fields", methods=["GET"])
def fields():
    """Show field-mapping UI."""
    src_table = session.get("source_table")
    tgt_table = session.get("target_table")
    mt = session.get("migration_type", "sn_sn")
    if not src_table or not tgt_table:
        flash("Select tables first.", "error")
        return redirect(url_for("tables"))

    source_is_sf = mt in ("sf_sf", "sf_sn")
    target_is_sf = mt in ("sn_sf", "sf_sf")

    # Fetch source and target field metadata in parallel
    results: dict[str, list | Exception] = {}

    def _fetch_fields(role: str, client, table: str, is_sf: bool) -> None:
        try:
            if is_sf:
                results[role] = client.get_object_fields(table)
            else:
                results[role] = client.get_table_fields(table)
        except Exception as exc:
            results[role] = exc

    # Build clients on the main thread
    if source_is_sf:
        src_client = _build_sf_client("source")
    else:
        src_client = _build_client("source")
    if target_is_sf:
        tgt_client = _build_sf_client("target")
    else:
        tgt_client = _build_client("target")

    t_src = threading.Thread(
        target=_fetch_fields, args=("source", src_client, src_table, source_is_sf))
    t_tgt = threading.Thread(
        target=_fetch_fields, args=("target", tgt_client, tgt_table, target_is_sf))
    t_src.start()
    t_tgt.start()
    t_src.join()
    t_tgt.join()

    # Check for errors
    for role in ("source", "target"):
        if isinstance(results.get(role), Exception):
            flash(f"Failed to fetch {role} fields: {results[role]}", "error")
            logger.error("Field fetch failed (%s): %s", role, results[role])
            return redirect(url_for("tables"))

    source_fields = results["source"]
    target_fields = results["target"]

    logger.info(
        "FIELDS DIAGNOSTIC: source has %d fields, target has %d fields "
        "(before filter). Target field names: %s",
        len(source_fields),
        len(target_fields),
        [f["name"] for f in target_fields],
    )

    # Filter out Salesforce system fields that are never user-writable.
    # We use a blocklist instead of createable/updateable because FLS
    # settings can make the API report custom fields as non-createable
    # even though they exist and are writable for admins.
    SF_SYSTEM_FIELDS = {
        "Id", "IsDeleted",
        "CreatedDate", "CreatedById",
        "LastModifiedDate", "LastModifiedById",
        "SystemModstamp",
        "LastActivityDate", "LastViewedDate",
        "LastReferencedDate",
        "OwnerId",
        # Other standard read-only or compound fields
        "MasterRecordId", "PhotoUrl", "Jigsaw", "JigsawCompanyId",
        "BillingAddress", "ShippingAddress", "MailingAddress", "OtherAddress",
        "BillingGeocodeAccuracy", "ShippingGeocodeAccuracy",
    }
    if target_is_sf:
        target_fields = [
            f for f in target_fields
            if f["name"] not in SF_SYSTEM_FIELDS
        ]

    logger.info(
        "FIELDS DIAGNOSTIC: target has %d fields after filter. Names: %s",
        len(target_fields),
        [f["name"] for f in target_fields],
    )

    auto_map = _auto_map_fields(source_fields, target_fields)
    manual_count = len(source_fields) - len(auto_map)

    logger.info(
        "FIELDS DIAGNOSTIC: auto_map has %d matches: %s",
        len(auto_map), auto_map,
    )

    # Store for reuse
    session["_source_fields"] = source_fields
    session["_target_fields"] = target_fields

    return render_template(
        "fields.html",
        migration_type=mt,
        source_table=src_table,
        target_table=tgt_table,
        source_fields=source_fields,
        target_fields=target_fields,
        auto_map=auto_map,
        auto_count=len(auto_map),
        manual_count=manual_count,
    )


@app.route("/fields", methods=["POST"])
def map_fields():
    """Process the field mapping form and start migration."""
    source_fields = session.get("_source_fields", [])
    field_mapping: dict[str, str] = {}

    for key in request.form.keys():
        if key.startswith("include_"):
            name = key[len("include_"):]
            target = request.form.get(f"map_{name}", "").strip()
            if target:
                field_mapping[name] = target

    if not field_mapping:
        flash("No fields mapped. Select at least one.", "error")
        return redirect(url_for("fields"))

    session["field_mapping"] = field_mapping

    # Store the fetch mode selection from the UI
    session["fetch_mode"] = request.form.get("fetch_mode", "auto")

    logger.info(
        "Field mapping: %s | Fetch mode: %s",
        field_mapping,
        session["fetch_mode"],
    )
    return redirect(url_for("migrate"))


# ─────────────────────────────────────────────────────────────────────
# Coalesce / Dedup APIs
# ─────────────────────────────────────────────────────────────────────

@app.route("/api/check_coalesce")
def api_check_coalesce():
    """Check if u_legacy_sysid field exists on the target table."""
    table = request.args.get("table", "").strip()
    if not table:
        return jsonify({"error": "Missing table parameter"}), 400

    cached = session.get("_target_fields")
    if cached:
        field_names = [f["name"] for f in cached]
        exists = "u_legacy_sysid" in field_names
        return jsonify({"exists": exists, "field_name": "u_legacy_sysid"})

    try:
        client = _build_client("target")
        fields_list = client.get_table_fields(table)
        field_names = [f["name"] for f in fields_list]
        exists = "u_legacy_sysid" in field_names
        return jsonify({"exists": exists, "field_name": "u_legacy_sysid"})
    except Exception as exc:
        logger.error("Coalesce check failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/create_coalesce", methods=["POST"])
def api_create_coalesce():
    """Create u_legacy_sysid field on the target SN table."""
    try:
        data = request.get_json() or {}
        table = data.get("table", "").strip()
        if not table:
            return jsonify({"error": "Missing table parameter"}), 400

        client = _build_client("target")
        result = client.create_legacy_sysid_field(table)
        
        # Clear the field cache so the new field shows up immediately
        session.pop("_target_fields", None)
        
        return jsonify({
            "success": True,
            "field_name": "u_legacy_sysid",
            "result": result
        })
    except Exception as exc:
        logger.error("Create coalesce failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/check_external_id")
def api_check_external_id():
    """Check if SN_Legacy_Id__c External ID field exists on SF target object."""
    obj = request.args.get("object", "").strip()
    if not obj:
        return jsonify({"error": "Missing object parameter"}), 400

    cached = session.get("_target_fields")
    if cached:
        for f in cached:
            if f["name"] == "SN_Legacy_Id__c":
                return jsonify({
                    "exists": True,
                    "field_name": "SN_Legacy_Id__c",
                    "is_external_id": f.get("externalId", False),
                })
        return jsonify({"exists": False, "field_name": "SN_Legacy_Id__c"})

    try:
        client = _build_sf_client("target")
        fields_list = client.get_object_fields(obj)
        for f in fields_list:
            if f["name"] == "SN_Legacy_Id__c":
                return jsonify({
                    "exists": True,
                    "field_name": "SN_Legacy_Id__c",
                    "is_external_id": f.get("externalId", False),
                })
        return jsonify({"exists": False, "field_name": "SN_Legacy_Id__c"})
    except Exception as exc:
        logger.error("External ID check failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────
# History
# ─────────────────────────────────────────────────────────────────────

@app.route("/history")
def history():
    """Show migration history."""
    return render_template("history.html", history=_load_history())


@app.route("/history/<int:index>")
def history_detail(index: int):
    """Show detail view for a single history entry."""
    all_history = _load_history()
    if index < 0 or index >= len(all_history):
        flash("History entry not found.", "error")
        return redirect(url_for("history"))
    entry = all_history[index]
    return render_template("history_detail.html", entry=entry, index=index)


@app.route("/api/clear_history", methods=["POST"])
def api_clear_history():
    """Clear all migration history."""
    _save_history([])
    flash("Migration history cleared.", "success")
    return redirect(url_for("history"))


# ─────────────────────────────────────────────────────────────────────
# Connections (saved credential profiles)
# ─────────────────────────────────────────────────────────────────────

@app.route("/connections", methods=["GET"])
def connections_page():
    """Saved connections management page."""
    return render_template("connections.html", connections=_load_connections())


@app.route("/connections", methods=["POST"])
def connections_save():
    """Create a new saved connection."""
    conns = _load_connections()
    platform = request.form.get("platform", "sn")
    name     = request.form.get("name", "").strip()
    if not name:
        flash("Connection name is required.", "error")
        return redirect(url_for("connections_page"))

    conn: dict = {
        "id":       _next_conn_id(conns),
        "name":     name,
        "platform": platform,   # 'sn' or 'sf'
    }
    if platform == "sn":
        conn["instance"] = request.form.get("sn_instance", "").strip()
        conn["username"] = request.form.get("sn_username", "").strip()
        conn["password"] = request.form.get("sn_password", "")
    else:  # sf
        conn["login_url"]      = request.form.get("sf_login_url", "https://login.salesforce.com")
        conn["username"]       = request.form.get("sf_username", "").strip()
        conn["password"]       = request.form.get("sf_password", "")
        conn["security_token"] = request.form.get("sf_security_token", "")

    conns.append(conn)
    _save_connections(conns)
    flash(f'Connection "{name}" saved.', "success")
    return redirect(url_for("connections_page"))


@app.route("/connections/<conn_id>/delete", methods=["POST"])
def connections_delete(conn_id: str):
    """Delete a saved connection by ID."""
    conns = [c for c in _load_connections() if c.get("id") != conn_id]
    _save_connections(conns)
    flash("Connection deleted.", "success")
    return redirect(url_for("connections_page"))


@app.route("/api/connections")
def api_connections():
    """Return saved connections as JSON (optionally filtered by platform)."""
    platform = request.args.get("platform", None)
    conns = _load_connections()
    if platform:
        conns = [c for c in conns if c.get("platform") == platform]
    # Never expose passwords over API
    safe = [
        {k: v for k, v in c.items() if k not in ("password", "security_token")}
        for c in conns
    ]
    return jsonify(safe)


@app.route("/api/test_connection", methods=["POST"])
def api_test_connection():
    """Test connectivity for given credentials. Returns JSON {ok, message}."""
    data     = request.get_json(force=True, silent=True) or {}
    platform = data.get("platform", "sn")
    try:
        if platform == "sn":
            instance = data.get("instance", "").strip()
            username = data.get("username", "").strip()
            password = data.get("password", "")
            if not (instance and username and password):
                return jsonify({"ok": False, "message": "Instance URL, username and password are required."})
            client = ServiceNowClient(instance=instance, username=username, password=password, role="test")
            tables = client.get_tables()  # lightweight connectivity check
            return jsonify({"ok": True, "message": f"Connected to {instance}. {len(tables)} tables visible."})
        else:
            from simple_salesforce import Salesforce, SalesforceAuthenticationFailed
            login_url      = data.get("login_url", "https://login.salesforce.com")
            username       = data.get("username", "").strip()
            password       = data.get("password", "")
            security_token = data.get("security_token", "")
            if not (username and password):
                return jsonify({"ok": False, "message": "Username and password are required."})
            sf = Salesforce(username=username, password=password,
                            security_token=security_token, instance_url=login_url)
            return jsonify({"ok": True, "message": f"Connected as {sf.sf_type}."})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)})


@app.route("/connections/<conn_id>/edit", methods=["POST"])
def connections_edit(conn_id: str):
    """Update an existing saved connection."""
    conns    = _load_connections()
    platform = request.form.get("platform", "sn")
    name     = request.form.get("name", "").strip()
    if not name:
        flash("Connection name is required.", "error")
        return redirect(url_for("connections_page"))

    for c in conns:
        if c.get("id") == conn_id:
            c["name"]     = name
            c["platform"] = platform
            if platform == "sn":
                c["instance"] = request.form.get("sn_instance", "").strip()
                c["username"] = request.form.get("sn_username", "").strip()
                pw = request.form.get("sn_password", "")
                if pw:  # only update password if provided
                    c["password"] = pw
            else:
                c["login_url"]      = request.form.get("sf_login_url", "https://login.salesforce.com")
                c["username"]       = request.form.get("sf_username", "").strip()
                pw = request.form.get("sf_password", "")
                if pw:
                    c["password"] = pw
                st = request.form.get("sf_security_token", "")
                if st:
                    c["security_token"] = st
            break

    _save_connections(conns)
    flash(f'Connection "{name}" updated.', "success")
    return redirect(url_for("connections_page"))


@app.route("/migrate", methods=["GET"])
def migrate():
    """Render migration progress page and kick off background work."""
    mapping = session.get("field_mapping")
    if not mapping:
        flash("Configure field mapping first.", "error")
        return redirect(url_for("fields"))

    mt = session.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")

    # Snapshot session data for the background thread
    ctx = {
        "migration_type": mt,
        "source_table": session["source_table"],
        "target_table": session["target_table"],
        "field_mapping": session["field_mapping"],
        "source_fields_meta": session.get("_source_fields", []),
        "fetch_mode": session.get("fetch_mode", "auto"),
    }

    # SN credentials
    if source_is_sn:
        ctx.update({
            "source_instance": session["source_instance"],
            "source_username": session["source_username"],
            "source_password": session["source_password"],
        })
    if target_is_sn:
        ctx.update({
            "target_instance": session["target_instance"],
            "target_username": session["target_username"],
            "target_password": session["target_password"],
        })

    # SF credentials
    if not source_is_sn:  # source is SF
        for key in ("login_url", "username", "password", "security_token"):
            ctx[f"sf_source_{key}"] = session.get(f"sf_source_{key}", "")
    if not target_is_sn:  # target is SF
        for key in ("login_url", "username", "password", "security_token"):
            ctx[f"sf_target_{key}"] = session.get(f"sf_target_{key}", "")

    # Start the migration on a background thread.
    t = threading.Thread(target=_run_migration, args=(ctx,), daemon=True)
    t.start()

    return render_template(
        "migrate.html",
        source_table=session.get("source_table"),
        target_table=session.get("target_table"),
    )


@app.route("/migrate/stream")
def migrate_stream():
    """SSE endpoint — streams progress events to the browser."""

    def generate() -> Generator[str, None, None]:
        while True:
            try:
                msg = _progress_q.get(timeout=120)
            except queue.Empty:
                # Send keep-alive comment.
                yield ": keepalive\n\n"
                continue

            event_type = msg.get("event", "progress")
            data = json.dumps(msg.get("data", {}))
            yield f"event: {event_type}\ndata: {data}\n\n"

            if event_type == "complete":
                break

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ─────────────────────────────────────────────────────────────────────
# Background migration runner
# ─────────────────────────────────────────────────────────────────────

def _run_migration(ctx: dict):
    """Execute the migration pipeline on a background thread."""
    global _progress_q
    _progress_q = queue.Queue()  # fresh queue for each run

    mt = ctx.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")

    try:
        # Reset tracker for a clean count
        app._rate_tracker = RateTracker()
        tracker = app._rate_tracker

        # ── Build source client ───────────────────────────────────
        if source_is_sn:
            src = ServiceNowClient(
                instance=ctx["source_instance"],
                username=ctx["source_username"],
                password=ctx["source_password"],
                role="source",
                tracker=tracker,
            )
        else:
            src = SalesforceClient(
                login_url=ctx["sf_source_login_url"],
                username=ctx["sf_source_username"],
                password=ctx["sf_source_password"],
                security_token=ctx.get("sf_source_security_token", ""),
                role="source",
                tracker=tracker,
            )

        # ── Build target client ───────────────────────────────────
        if target_is_sn:
            tgt = ServiceNowClient(
                instance=ctx["target_instance"],
                username=ctx["target_username"],
                password=ctx["target_password"],
                role="target",
                tracker=tracker,
            )
        else:
            tgt = SalesforceClient(
                login_url=ctx["sf_target_login_url"],
                username=ctx["sf_target_username"],
                password=ctx["sf_target_password"],
                security_token=ctx.get("sf_target_security_token", ""),
                role="target",
                tracker=tracker,
            )

        # ── Determine external ID field for SF targets ──────────────
        sf_external_id_field = None
        if not target_is_sn:  # target is SF
            sf_external_id_field = "SN_Legacy_Id__c"

        def progress_cb(phase, processed, total, detail=""):
            _progress_q.put(
                {
                    "event": "progress",
                    "data": {
                        "phase": phase,
                        "processed": processed,
                        "total": total,
                        "detail": detail,
                    },
                }
            )

        orchestrator = MigrationOrchestrator(
            source_client=src,
            target_client=tgt,
            source_table=ctx["source_table"],
            target_table=ctx["target_table"],
            field_mapping=ctx["field_mapping"],
            tracker=tracker,
            progress_callback=progress_cb,
            source_fields_meta=ctx.get("source_fields_meta"),
            fetch_mode=ctx.get("fetch_mode", "auto"),
            migration_type=mt,
            sf_external_id_field=sf_external_id_field,
        )

        report = orchestrator.run()

        # Save to history
        status = "success"
        if report.failed > 0 and (report.inserts + report.updates) > 0:
            status = "partial"
        elif report.failed > 0 and (report.inserts + report.updates) == 0:
            status = "failed"

        _append_history({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_instance": report.source_instance,
            "source_table": report.source_table,
            "target_instance": report.target_instance,
            "target_table": report.target_table,
            "total_source_records": report.total_source_records,
            "inserts": report.inserts,
            "updates": report.updates,
            "skipped": report.skipped,
            "failed": report.failed,
            "duration": round(report.timing.total, 1),
            "status": status,
            "fetch_mode": report.fetch_mode_used,
            "migration_type": mt,
        })

        _progress_q.put(
            {"event": "complete", "data": report.to_dict()}
        )

    except Exception as exc:
        logger.exception("Migration failed: %s", exc)

        _append_history({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_instance": ctx.get("source_instance", ctx.get("sf_source_username", "")),
            "source_table": ctx.get("source_table", ""),
            "target_instance": ctx.get("target_instance", ctx.get("sf_target_username", "")),
            "target_table": ctx.get("target_table", ""),
            "total_source_records": 0,
            "inserts": 0,
            "updates": 0,
            "skipped": 0,
            "failed": 0,
            "duration": 0,
            "status": "failed",
            "error": str(exc),
            "migration_type": mt,
        })

        _progress_q.put(
            {
                "event": "error_msg",
                "data": {"message": str(exc)},
            }
        )


# ─────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting SN Migration Tool on port %d", FLASK_PORT)
    app.run(
        host="0.0.0.0",
        port=FLASK_PORT,
        debug=False,
        threaded=True,
    )
