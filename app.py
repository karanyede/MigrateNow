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
from migration.rollback_store import RollbackStore
from migration.rollback_executor import RollbackExecutor

# ── Bootstrap ────────────────────────────────────────────────────────
logger = setup_logging()

app = Flask(__name__)
app.secret_key = FLASK_SECRET
app.config["TEMPLATES_AUTO_RELOAD"] = True

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

# Thread-safe event to pause/resume migrations.
_pause_event = threading.Event()
_pause_event.set()

# Rollback SSE queue and DB path
_rollback_q: queue.Queue = queue.Queue()
_rollback_result: dict = {}

# Data file paths
HISTORY_FILE      = Path(__file__).parent / "data" / "migration_history.json"
CONNECTIONS_FILE  = Path(__file__).parent / "data" / "connections.json"
CONFIGS_FILE      = Path(__file__).parent / "data" / "migration_configs.json"
ROLLBACK_DB       = Path(__file__).parent / "data" / "rollback.db"
USERS_FILE        = Path(__file__).parent / "data" / "users.json"


# ─────────────────────────────────────────────────────────────────────
# User Management Helpers
# ─────────────────────────────────────────────────────────────────────

import bcrypt as _bcrypt
from functools import wraps
import uuid as _uuid


def _load_users() -> list[dict]:
    """Load all user accounts from users.json."""
    if USERS_FILE.exists():
        try:
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_users(users: list[dict]) -> None:
    """Persist user accounts to users.json."""
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(
        json.dumps(users, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _find_user(username: str | None = None, user_id: str | None = None) -> dict | None:
    """Find a user by username or ID."""
    users = _load_users()
    if username:
        return next((u for u in users if u.get("username") == username), None)
    if user_id:
        return next((u for u in users if u.get("id") == user_id), None)
    return None


def _hash_password(plain: str) -> str:
    """Return a bcrypt hash of the given plaintext password."""
    return _bcrypt.hashpw(plain.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")


def _check_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a stored bcrypt hash."""
    try:
        return _bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def _current_user_id() -> str | None:
    return session.get("user_id")


def _current_role() -> str:
    return session.get("role", "user")


def _is_admin() -> bool:
    return _current_role() == "admin"


def _migrate_legacy_data() -> None:
    """Tag all existing data entries (no user_id) with the first admin's ID.
    Called once at startup after the first admin is created."""
    users = _load_users()
    if not users:
        return
    admin = next((u for u in users if u.get("role") == "admin"), users[0])
    admin_id = admin["id"]
    admin_name = admin.get("username", "admin")

    changed = False
    # History
    if HISTORY_FILE.exists():
        try:
            history = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
            for entry in history:
                if not entry.get("user_id"):
                    entry["user_id"] = admin_id
                    entry["username"] = admin_name
                    changed = True
            if changed:
                HISTORY_FILE.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")
                changed = False
        except Exception:
            pass
    # Configs
    if CONFIGS_FILE.exists():
        try:
            configs = json.loads(CONFIGS_FILE.read_text(encoding="utf-8"))
            for c in configs:
                if not c.get("user_id"):
                    c["user_id"] = admin_id
                    c["username"] = admin_name
                    changed = True
            if changed:
                CONFIGS_FILE.write_text(json.dumps(configs, indent=2, ensure_ascii=False), encoding="utf-8")
                changed = False
        except Exception:
            pass
    # Connections
    if CONNECTIONS_FILE.exists():
        try:
            conns = json.loads(CONNECTIONS_FILE.read_text(encoding="utf-8"))
            for c in conns:
                if not c.get("user_id"):
                    c["user_id"] = admin_id
                    c["owner_name"] = admin_name
                    # keep username as login user if not set, else set owner_name
                    if "username" not in c:
                        c["username"] = admin_name
                    changed = True
            if changed:
                CONNECTIONS_FILE.write_text(json.dumps(conns, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass


# ── Auth decorators ──────────────────────────────────────────────────

def login_required(f):
    """Redirect to /login if no user is in session."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login_page"))
        user = _find_user(user_id=session["user_id"])
        if not user or not user.get("is_active", True) or user.get("is_deleted"):
            session.clear()
            flash("Your account has been deactivated. Contact your administrator.", "error")
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Require admin role; otherwise redirect home with error flash."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "admin":
            flash("Admin access required.", "error")
            return redirect(url_for("home"))
        return f(*args, **kwargs)
    return decorated


@app.context_processor
def inject_user_profile():
    """Inject profile details to templates dynamically without storing base64 in cookie-session."""
    uid = session.get("user_id")
    if uid:
        user = _find_user(user_id=uid)
        if user:
            return {
                "current_user_profile_pic": user.get("profile_pic"),
                "current_user_display_name": user.get("display_name", user["username"]),
            }
    return {
        "current_user_profile_pic": None,
        "current_user_display_name": None,
    }



# ─────────────────────────────────────────────────────────────────────
# History persistence
# ─────────────────────────────────────────────────────────────────────

def _load_history() -> list[dict]:
    """Load migration history. Admins see all; regular users see only their own."""
    if HISTORY_FILE.exists():
        try:
            all_history = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    else:
        return []
    if _is_admin():
        return all_history
    uid = _current_user_id()
    return [h for h in all_history if h.get("user_id") == uid]


def _load_all_history() -> list[dict]:
    """Load ALL history regardless of user (admin utility)."""
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
    """Append a migration result to history, tagging the current user."""
    # Tag with current user (fall back gracefully if called before auth is set)
    entry.setdefault("user_id", session.get("user_id", "system"))
    entry.setdefault("username", session.get("username", "system"))
    history = _load_all_history()
    history.insert(0, entry)  # newest first
    _save_history(history)



def _clean_error_message(exc: Exception) -> str:
    """Clean up raw exceptions (especially Salesforce SOAP faults) for display."""
    msg = str(exc)
    import re
    if "<?xml" in msg or "soapenv:Envelope" in msg or "env:Envelope" in msg:
        match = re.search(r"<faultstring>(.*?)</faultstring>", msg)
        if match:
            return f"Salesforce Error: {match.group(1)}"
    return msg


# ── Migration configs persistence ───────────────────────────────────

def _load_configs() -> list[dict]:
    """Load saved migration configs. Admins see all; regular users see only their own."""
    if CONFIGS_FILE.exists():
        try:
            all_configs = json.loads(CONFIGS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    else:
        return []
    if _is_admin():
        return all_configs
    uid = _current_user_id()
    return [c for c in all_configs if c.get("user_id") == uid]


def _load_all_configs() -> list[dict]:
    """Load ALL configs regardless of user (admin utility)."""
    if CONFIGS_FILE.exists():
        try:
            return json.loads(CONFIGS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    return []


def _save_configs(configs: list[dict]) -> None:
    """Save migration configurations to JSON file."""
    CONFIGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIGS_FILE.write_text(
        json.dumps(configs, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _seed_configs_from_history() -> None:
    """Seed migration_configs.json from history if the file doesn't yet exist.

    This handles the case where migrations were done before the config-save
    feature was introduced (or before the toUpperCase Python bug was fixed).
    We build deduplicated config entries from existing history records so that
    users see their Recent Transactions immediately without a new migration.
    """
    if CONFIGS_FILE.exists():
        return  # already seeded / already has data

    history = _load_history()
    if not history:
        return

    configs: list[dict] = []
    seen: set[str] = set()

    for entry in history:
        mt = entry.get("migration_type", "sn_sn")
        st = entry.get("source_table", "")
        tt = entry.get("target_table", "")
        si = entry.get("source_instance", "")
        ti = entry.get("target_instance", "")
        sf_su = entry.get("sf_source_username", "")
        sf_tu = entry.get("sf_target_username", "")

        dedup_key = f"{mt}|{st}|{tt}|{si or sf_su}|{ti or sf_tu}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        import uuid as _uuid
        config_id = _uuid.uuid4().hex[:8]
        cfg = {
            "id": config_id,
            "name": f"{st} \u2794 {tt}",
            "migration_type": mt,
            "source_table": st,
            "target_table": tt,
            "field_mapping": {},   # not stored in history; will be remapped on repeat
            "filter_conditions": [],
            "fetch_mode": entry.get("fetch_mode", "auto"),
            "created_at": entry.get("timestamp", ""),
            "last_run_at": entry.get("timestamp", ""),
        }
        if "sn" in mt.split("_")[0]:   # source is SN
            cfg["source_instance"] = si
        else:
            cfg["sf_source_username"] = sf_su
        if "sn" in mt.split("_")[1]:   # target is SN
            cfg["target_instance"] = ti
        else:
            cfg["sf_target_username"] = sf_tu

        configs.append(cfg)
        if len(configs) >= 15:
            break

    if configs:
        _save_configs(configs)
        logger.info("Seeded %d migration config(s) from history.", len(configs))


def _save_migration_config_from_session() -> str:
    """Save current migration parameters from session into the configs list."""
    configs = _load_configs()
    
    mt = session.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")
    
    source_table = session.get("source_table", "")
    target_table = session.get("target_table", "")
    field_mapping = session.get("field_mapping", {})
    filter_conditions = session.get("filter_conditions", [])
    fetch_mode = session.get("fetch_mode", "auto")
    limit = session.get("limit")
    orderby_field = session.get("orderby_field")
    orderby_dir = session.get("orderby_dir", "ASC")

    # Generate a descriptive name
    name = f"{source_table} ➔ {target_table} ({mt.replace('_', '➔').upper()})"

    # Look for duplicate config
    existing = None
    for c in configs:
        if (c.get("migration_type") == mt and
            c.get("source_table") == source_table and
            c.get("target_table") == target_table and
            c.get("field_mapping") == field_mapping and
            c.get("filter_conditions") == filter_conditions and
            c.get("fetch_mode") == fetch_mode and
            c.get("limit") == limit and
            c.get("orderby_field") == orderby_field and
            c.get("orderby_dir") == orderby_dir):
            
            if source_is_sn and c.get("source_instance") != session.get("source_instance"):
                continue
            if target_is_sn and c.get("target_instance") != session.get("target_instance"):
                continue
            existing = c
            break

    now_iso = datetime.now(timezone.utc).isoformat()
    if existing:
        existing["last_run_at"] = now_iso
        existing["name"] = name
        config_id = existing["id"]
    else:
        import uuid
        config_id = uuid.uuid4().hex[:8]
        new_config = {
            "id": config_id,
            "name": name,
            "migration_type": mt,
            "source_table": source_table,
            "target_table": target_table,
            "field_mapping": field_mapping,
            "filter_conditions": filter_conditions,
            "fetch_mode": fetch_mode,
            "limit": limit,
            "orderby_field": orderby_field,
            "orderby_dir": orderby_dir,
            "created_at": now_iso,
            "last_run_at": now_iso,
            "user_id": session.get("user_id", "system"),
            "username": session.get("username", "system"),
        }

        if source_is_sn:
            new_config["source_instance"] = session.get("source_instance")
        else:
            new_config["sf_source_username"] = session.get("sf_source_username")
            new_config["sf_source_login_url"] = session.get("sf_source_login_url")

        if target_is_sn:
            new_config["target_instance"] = session.get("target_instance")
        else:
            new_config["sf_target_username"] = session.get("sf_target_username")
            new_config["sf_target_login_url"] = session.get("sf_target_login_url")

        configs.insert(0, new_config)


    # Keep only top 15 configs
    _save_configs(configs[:15])
    return config_id


# ── Connections persistence ──────────────────────────────────────────

def _load_connections() -> list[dict]:
    """Load saved connections. Admins see all; regular users see only their own."""
    if CONNECTIONS_FILE.exists():
        try:
            all_conns = json.loads(CONNECTIONS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    else:
        return []
    if _is_admin():
        return all_conns
    uid = _current_user_id()
    return [c for c in all_conns if c.get("user_id") == uid]


def _load_all_connections() -> list[dict]:
    """Load ALL connections regardless of user (admin utility)."""
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


import re

def _clean_error_message(exc: Exception | str) -> str:
    """Extract a user-friendly error string from raw exceptions, SOAP envelopes or HTTP dumps."""
    msg = str(exc).strip()
    if not msg:
        return "An unknown error occurred."

    # If it contains Salesforce SOAP Fault XML
    if "soapenv:Envelope" in msg or "<?xml" in msg:
        fault_match = re.search(r"<faultstring>([^<]+)</faultstring>", msg)
        if fault_match:
            val = fault_match.group(1).strip()
            if "API_CURRENTLY_DISABLED" in val:
                return "Salesforce API is currently disabled (e.g. your trial or org has expired)."
            return f"Salesforce login failed: {val}"
        
        code_match = re.search(r"<exceptionCode>([^<]+)</exceptionCode>", msg)
        if code_match:
            val = code_match.group(1).strip()
            if val == "API_CURRENTLY_DISABLED":
                return "Salesforce API is currently disabled (e.g. your trial or org has expired)."
            return f"Salesforce error: {val}"
            
        return "Salesforce connection failed (API currently disabled or incorrect settings)."

    if "API_CURRENTLY_DISABLED" in msg:
        return "Salesforce API is currently disabled (e.g. your trial or org has expired)."

    if msg.startswith("'") and msg.endswith("'"):
        msg = msg[1:-1]
    if msg.startswith('"') and msg.endswith('"'):
        msg = msg[1:-1]

    if len(msg) > 150:
        return msg[:150] + "..."

    return msg


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
# Auth Routes — Login / Logout / Setup
# ─────────────────────────────────────────────────────────────────────

@app.route("/setup", methods=["GET", "POST"])
def setup():
    """First-run: create the initial admin account. Inaccessible if users exist."""
    users = _load_users()
    if users:
        return redirect(url_for("login_page"))
    if request.method == "POST":
        display_name = request.form.get("display_name", "").strip()
        username     = request.form.get("username", "").strip().lower()
        email        = request.form.get("email", "").strip()
        password     = request.form.get("password", "")
        confirm      = request.form.get("confirm_password", "")
        if not display_name or not username or not password:
            flash("Display name, username and password are required.", "error")
            return render_template("setup.html")
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("setup.html")
        new_user = {
            "id": _uuid.uuid4().hex[:12],
            "username": username,
            "display_name": display_name,
            "email": email,
            "password_hash": _hash_password(password),
            "role": "admin",
            "is_active": True,
            "is_deleted": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "created_by": "setup",
            "last_login": None,
            "must_change_password": False,
        }
        _save_users([new_user])
        _migrate_legacy_data()
        flash("Admin account created. Please log in.", "success")
        return redirect(url_for("login_page"))
    return render_template("setup.html")


@app.route("/login", methods=["GET", "POST"])
def login_page():
    """Login page."""
    # Redirect to setup if no users exist
    if not _load_users():
        return redirect(url_for("setup"))
    if session.get("user_id"):
        return redirect(url_for("home"))
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        remember = request.form.get("remember") == "on"
        user = _find_user(username=username)
        if not user or not _check_password(password, user.get("password_hash", "")):
            flash("Invalid username or password.", "error")
            return render_template("login.html", username=username)
        if not user.get("is_active", True) or user.get("is_deleted"):
            flash("Your account has been deactivated. Contact your administrator.", "error")
            return render_template("login.html", username=username)
        # Set session
        if remember:
            from datetime import timedelta
            app.permanent_session_lifetime = timedelta(days=30)
            session.permanent = True
        session["user_id"]      = user["id"]
        session["username"]     = user["username"]
        session["display_name"] = user.get("display_name", user["username"])
        session["role"]         = user.get("role", "user")
        # Update last_login
        users = _load_users()
        for u in users:
            if u["id"] == user["id"]:
                u["last_login"] = datetime.now(timezone.utc).isoformat()
                break
        _save_users(users)
        if user.get("must_change_password"):
            flash("Your password was reset by an admin. Please set a new password.", "warning")
            return redirect(url_for("profile_page"))
        return redirect(url_for("home"))
    return render_template("login.html", username="")


@app.route("/logout")
def logout():
    """Clear session and redirect to login."""
    session.clear()
    flash("You have been signed out.", "success")
    return redirect(url_for("login_page"))


# ─────────────────────────────────────────────────────────────────────
# Profile Route
# ─────────────────────────────────────────────────────────────────────

@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile_page():
    """User profile — view/edit account info and change password."""
    users = _load_users()
    user = next((u for u in users if u["id"] == session["user_id"]), None)
    if not user:
        session.clear()
        return redirect(url_for("login_page"))

    error = None
    if request.method == "POST":
        action = request.form.get("action", "update_profile")

        if action == "update_profile":
            display_name = request.form.get("display_name", "").strip()
            email        = request.form.get("email", "").strip()
            
            # Profile picture upload
            profile_pic = request.files.get("profile_pic")
            if profile_pic and profile_pic.filename:
                import base64
                pic_data = profile_pic.read()
                if len(pic_data) > 500 * 1024:
                    error = "Profile picture must be under 500KB."
                else:
                    encoded = base64.b64encode(pic_data).decode("utf-8")
                    mime = profile_pic.content_type or "image/png"
                    user["profile_pic"] = f"data:{mime};base64,{encoded}"

            if not display_name:
                error = "Display name cannot be empty."
                
            if not error:
                user["display_name"] = display_name
                user["email"] = email
                _save_users(users)
                session["display_name"] = display_name
                flash("Profile updated.", "success")
                return redirect(url_for("profile_page"))

        elif action == "change_password":
            current_pw  = request.form.get("current_password", "")
            new_pw      = request.form.get("new_password", "")
            confirm_pw  = request.form.get("confirm_password", "")
            if not user.get("must_change_password") and not _check_password(current_pw, user.get("password_hash", "")):
                error = "Current password is incorrect."
            elif not new_pw or len(new_pw) < 6:
                error = "New password must be at least 6 characters."
            elif new_pw != confirm_pw:
                error = "Passwords do not match."
            else:
                user["password_hash"] = _hash_password(new_pw)
                user["must_change_password"] = False
                _save_users(users)
                flash("Password changed successfully.", "success")
                return redirect(url_for("home"))

    # Stats for this user
    all_history = _load_all_history()
    my_history  = [h for h in all_history if h.get("user_id") == user["id"]]
    my_records  = sum((h.get("inserts", 0) or 0) + (h.get("updates", 0) or 0) for h in my_history)
    my_conns    = len([c for c in _load_all_connections() if c.get("user_id") == user["id"]])

    return render_template(
        "profile.html",
        user=user,
        error=error,
        my_migrations=len(my_history),
        my_records=my_records,
        my_connections=my_conns,
    )


# ─────────────────────────────────────────────────────────────────────
# Admin — User Management Routes
# ─────────────────────────────────────────────────────────────────────

@app.route("/admin/users")
@login_required
@admin_required
def admin_users():
    """Admin user management page."""
    all_users = _load_users()
    all_history = _load_all_history()
    # Annotate each user with migration count
    for u in all_users:
        u["_migration_count"] = sum(1 for h in all_history if h.get("user_id") == u["id"])
    total_users    = len([u for u in all_users if not u.get("is_deleted")])
    active_users   = len([u for u in all_users if u.get("is_active") and not u.get("is_deleted")])
    inactive_users = len([u for u in all_users if not u.get("is_active") and not u.get("is_deleted")])
    admin_count    = len([u for u in all_users if u.get("role") == "admin" and not u.get("is_deleted")])
    return render_template(
        "admin_users.html",
        users=all_users,
        total_users=total_users,
        active_users=active_users,
        inactive_users=inactive_users,
        admin_count=admin_count,
    )


@app.route("/admin/users/create", methods=["POST"])
@login_required
@admin_required
def admin_create_user():
    """Admin creates a new user account."""
    users = _load_users()
    username     = request.form.get("username", "").strip().lower()
    display_name = request.form.get("display_name", "").strip()
    email        = request.form.get("email", "").strip()
    role         = request.form.get("role", "user")
    password     = request.form.get("password", "").strip()
    must_change  = request.form.get("must_change_password") == "on"

    if not username or not display_name or not password:
        flash("Username, display name, and password are required.", "error")
        return redirect(url_for("admin_users"))
    if _find_user(username=username):
        flash(f'Username "{username}" is already taken.', "error")
        return redirect(url_for("admin_users"))
    if role not in ("admin", "user"):
        role = "user"

    new_user = {
        "id": _uuid.uuid4().hex[:12],
        "username": username,
        "display_name": display_name,
        "email": email,
        "password_hash": _hash_password(password),
        "role": role,
        "is_active": True,
        "is_deleted": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": session.get("username", "admin"),
        "last_login": None,
        "must_change_password": must_change,
    }
    users.append(new_user)
    _save_users(users)
    flash(f'User "{username}" created successfully.', "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<user_id>/toggle", methods=["POST"])
@login_required
@admin_required
def admin_toggle_user(user_id: str):
    """Toggle a user's active/inactive status."""
    if user_id == session["user_id"]:
        flash("You cannot deactivate your own account.", "error")
        return redirect(url_for("admin_users"))
    users = _load_users()
    for u in users:
        if u["id"] == user_id:
            u["is_active"] = not u.get("is_active", True)
            status = "activated" if u["is_active"] else "deactivated"
            _save_users(users)
            flash(f'User "{u["username"]}" {status}.', "success")
            return redirect(url_for("admin_users"))
    flash("User not found.", "error")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<user_id>/reset-password", methods=["POST"])
@login_required
@admin_required
def admin_reset_password(user_id: str):
    """Admin resets a user's password."""
    new_password = request.form.get("new_password", "").strip()
    if not new_password or len(new_password) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect(url_for("admin_users"))
    users = _load_users()
    for u in users:
        if u["id"] == user_id:
            u["password_hash"] = _hash_password(new_password)
            u["must_change_password"] = True
            _save_users(users)
            flash(f'Password for "{u["username"]}" has been reset. They will be prompted to change it on next login.', "success")
            return redirect(url_for("admin_users"))
    flash("User not found.", "error")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<user_id>/change-role", methods=["POST"])
@login_required
@admin_required
def admin_change_role(user_id: str):
    """Toggle user role between admin and user."""
    if user_id == session["user_id"]:
        flash("You cannot change your own role.", "error")
        return redirect(url_for("admin_users"))
    new_role = request.form.get("role", "user")
    if new_role not in ("admin", "user"):
        new_role = "user"
    users = _load_users()
    for u in users:
        if u["id"] == user_id:
            u["role"] = new_role
            _save_users(users)
            flash(f'Role for "{u["username"]}" changed to {new_role}.', "success")
            return redirect(url_for("admin_users"))
    flash("User not found.", "error")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<user_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_delete_user(user_id: str):
    """Soft-delete a user account (preserves their data)."""
    if user_id == session["user_id"]:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("admin_users"))
    users = _load_users()
    for u in users:
        if u["id"] == user_id:
            u["is_deleted"] = True
            u["is_active"] = False
            _save_users(users)
            flash(f'User "{u["username"]}" has been removed. Their data is preserved.', "success")
            return redirect(url_for("admin_users"))
    flash("User not found.", "error")
    return redirect(url_for("admin_users"))


# ─────────────────────────────────────────────────────────────────────
# Existing App Routes
# ─────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
@login_required
def home():
    """Home page — MigrateNow dashboard with stats and recent history."""
    conns = _load_connections()
    users = _load_users()
    user_count = len([u for u in users if not u.get("is_deleted")])
    return render_template(
        "home.html",
        history=_load_history(),
        connections_count=len(conns),
        user_count=user_count,
    )


@app.route("/select-type", methods=["POST"])
@login_required
def select_type():
    """Store migration_type in session and redirect to connect."""
    mt = request.form.get("migration_type", "sn_sn")
    if mt not in ("sn_sn", "sn_sf", "sf_sf", "sf_sn"):
        mt = "sn_sn"
    session["migration_type"] = mt
    return redirect(url_for("connect_page"))


@app.route("/connect", methods=["GET"])
@login_required
def connect_page():
    """Render connect page with appropriate credential fields."""
    mt = session.get("migration_type", "sn_sn")
    return render_template(
        "index.html",
        migration_type=mt,
        saved_connections=_load_connections(),
        recent_configs=_load_configs()[:5],
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
@login_required
def connect():
    """Validate credentials and redirect to table selection."""
    source_platform = request.form.get("source_platform", "sn").strip()
    target_platform = request.form.get("target_platform", "sn").strip()
    
    mt = f"{source_platform}_{target_platform}"
    session["migration_type"] = mt

    # Clear any previous migration configurations from session to start fresh
    session.pop("source_table", None)
    session.pop("target_table", None)
    session.pop("field_mapping", None)
    session.pop("filter_conditions", None)
    session.pop("_source_fields", None)
    session.pop("_target_fields", None)
    session.pop("fetch_mode", None)

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
            clean_msg = _clean_error_message(exc)
            flash(f"Source SN connection failed: {clean_msg}", "error")
            logger.error("Source SN connection failed: %s", exc)
            return redirect(url_for("connect_page"))
    elif source_is_sf:
        try:
            sf_src = _build_sf_client("source")
            sf_src.get_objects()  # validates auth + fetches object list
            logger.info("Source SF connection OK: %s", sf_src.instance)
        except Exception as exc:
            clean_msg = _clean_error_message(exc)
            flash(f"Source SF connection failed: {clean_msg}", "error")
            logger.error("Source SF connection failed: %s", exc)
            return redirect(url_for("connect_page"))

    # Target
    if target_is_sn:
        try:
            tgt = _build_client("target")
            tgt.fetch_page("sys_db_object", limit=1, fields=["name"])
            logger.info("Target SN connection OK: %s", session["target_instance"])
        except Exception as exc:
            clean_msg = _clean_error_message(exc)
            flash(f"Target SN connection failed: {clean_msg}", "error")
            logger.error("Target SN connection failed: %s", exc)
            return redirect(url_for("connect_page"))
    elif target_is_sf:
        try:
            sf_tgt = _build_sf_client("target")
            sf_tgt.get_objects()
            logger.info("Target SF connection OK: %s", sf_tgt.instance)
        except Exception as exc:
            clean_msg = _clean_error_message(exc)
            flash(f"Target SF connection failed: {clean_msg}", "error")
            logger.error("Target SF connection failed: %s", exc)
            return redirect(url_for("connect_page"))

    flash("Connected to both instances.", "success")
    return redirect(url_for("tables"))


@app.route("/tables", methods=["GET"])
@login_required
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
@login_required
def select_tables():
    """Store selected tables and redirect to field mapping."""
    session["source_table"] = request.form["source_table"].strip()
    session["target_table"] = request.form["target_table"].strip()
    if not session["source_table"] or not session["target_table"]:
        flash("Please select both source and target tables.", "error")
        return redirect(url_for("tables"))
    return redirect(url_for("fields"))


@app.route("/api/search_tables")
@login_required
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
@login_required
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
@login_required
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
                fields = client.get_table_fields(table)
                # Fetch choice metadata and merge
                choices_map = client.get_choice_values(table)
                for f in fields:
                    f["choices"] = choices_map.get(f["name"], [])
                results[role] = fields
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

    # Filter out ServiceNow OOTB standard read-only system fields from the
    # TARGET side. These are auto-populated by SN and writing to them either
    # fails silently or causes API errors.  Users never need to map to them.
    SN_SYSTEM_FIELDS = {
        "sys_id", "sys_created_on", "sys_created_by",
        "sys_updated_on", "sys_updated_by", "sys_mod_count",
        "sys_class_name", "sys_tags", "sys_domain", "sys_domain_path",
        "sys_overrides", "sys_package", "sys_policy", "sys_replace_on_upgrade",
        "sys_scope", "sys_version",
    }
    if not target_is_sf:  # target is ServiceNow
        target_fields = [
            f for f in target_fields
            if f["name"] not in SN_SYSTEM_FIELDS
        ]

    logger.info(
        "FIELDS DIAGNOSTIC: target has %d fields after filter. Names: %s",
        len(target_fields),
        [f["name"] for f in target_fields],
    )

    saved_mapping = session.get("field_mapping")
    if saved_mapping:
        auto_map = saved_mapping
    else:
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
@login_required
def fields_post():
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

    # Read and store coalesce configuration
    coalesce_config = request.form.get("coalesce_config", "")
    try:
        session["coalesce_config"] = json.loads(coalesce_config)
    except Exception:
        session["coalesce_config"] = {}

    logger.info(
        "Field mapping: %s | Fetch mode: %s | Coalesce: %s",
        field_mapping,
        session["fetch_mode"],
        session["coalesce_config"],
    )
    return redirect(url_for("filters_page"))


# ── Filters & Preview Step ──────────────────────────────────────────

def _build_sn_query(filters: list) -> str:
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
            group_queries.append("^OR".join(cond_queries))
    
    if not group_queries:
        return ""
    return "^".join(group_queries)


def _build_sf_where(filters: list) -> str:
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
    return " AND ".join(group_queries)


@app.route("/filters", methods=["GET"])
@login_required
def filters_page():
    """Show filters and data preview UI (Step 4 of 5)."""
    src_table = session.get("source_table")
    tgt_table = session.get("target_table")
    mt = session.get("migration_type", "sn_sn")
    if not src_table or not tgt_table:
        flash("Select tables first.", "error")
        return redirect(url_for("tables"))

    source_fields = session.get("_source_fields", [])
    target_fields = session.get("_target_fields", [])

    if not source_fields:
        source_is_sf = mt in ("sf_sf", "sf_sn")
        try:
            if source_is_sf:
                src_client = _build_sf_client("source")
                source_fields = src_client.get_object_fields(src_table)
            else:
                src_client = _build_client("source")
                source_fields = src_client.get_table_fields(src_table)
                choices_map = src_client.get_choice_values(src_table)
                for f in source_fields:
                    f["choices"] = choices_map.get(f["name"], [])
            session["_source_fields"] = source_fields
        except Exception as exc:
            logger.error("Failed to fetch source fields for filters preview: %s", exc)

    return render_template(
        "filters.html",
        migration_type=mt,
        source_table=src_table,
        target_table=tgt_table,
        source_fields=source_fields,
        target_fields=target_fields,
        filter_conditions=session.get("filter_conditions", []),
        limit=session.get("limit"),
        orderby_field=session.get("orderby_field"),
        orderby_dir=session.get("orderby_dir", "ASC"),
    )


@app.route("/filters", methods=["POST"])
@login_required
def filters_post():
    """Save filter conditions, limit, and orderby config into the session, then proceed to confirm."""
    filter_conditions = request.form.get("filter_conditions", "[]")
    try:
        session["filter_conditions"] = json.loads(filter_conditions)
    except Exception:
        session["filter_conditions"] = []

    limit_str = request.form.get("limit", "").strip()
    if limit_str:
        try:
            session["limit"] = int(limit_str)
        except ValueError:
            session["limit"] = None
    else:
        session["limit"] = None

    session["orderby_field"] = request.form.get("orderby_field", "").strip() or None
    session["orderby_dir"] = request.form.get("orderby_dir", "ASC").strip()

    # Auto-save migration config!
    _save_migration_config_from_session()

    logger.info(
        "Filters saved: %s | Limit: %s | OrderBy: %s %s",
        session["filter_conditions"],
        session["limit"],
        session["orderby_field"],
        session["orderby_dir"],
    )
    return redirect(url_for("migrate_confirm"))


@app.route("/api/preview_data", methods=["POST"])
@login_required
def api_preview_data():
    """Run a query on the source instance using filter conditions, returning first 10 records and total match count."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        filters = data.get("filter_conditions", [])
        orderby_field = data.get("orderby_field")
        orderby_dir = data.get("orderby_dir", "ASC")

        mt = session.get("migration_type", "sn_sn")
        src_table = session.get("source_table")
        if not src_table:
            return jsonify({"error": "No source table in session."}), 400

        source_is_sf = mt in ("sf_sf", "sf_sn")
        tracker = _get_tracker()

        records = []
        total_count = 0

        if source_is_sf:
            sf = _build_sf_client("source")
            f_meta = sf.get_object_fields(src_table)
            field_list = [f["name"] for f in f_meta if f.get("type") not in ("address", "location")]
            if not field_list:
                field_list = ["Id", "Name"]

            query = f"SELECT {', '.join(field_list)} FROM {src_table}"
            where_clause = _build_sf_where(filters)
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if orderby_field:
                query += f" ORDER BY {orderby_field} {orderby_dir}"
                
            query_preview = query + " LIMIT 10"
            
            records = sf.query(query_preview)
            
            count_query = f"SELECT COUNT() FROM {src_table}"
            if where_clause:
                count_query += f" WHERE {where_clause}"
            
            try:
                resp = sf._request("GET", f"{sf._api_base}/query/", params={"q": count_query})
                resp.raise_for_status()
                total_count = resp.json().get("totalSize", 0)
            except Exception as e:
                logger.error("Salesforce preview count failed: %s", e)
                total_count = len(records)
        else:
            client = _build_client("source")
            sn_query = _build_sn_query(filters)
            
            extra_query = sn_query
            if orderby_field:
                if orderby_dir == "DESC":
                    orderby_str = f"ORDERBYDESC{orderby_field}"
                else:
                    orderby_str = f"ORDERBY{orderby_field}"
                if extra_query:
                    extra_query += f"^{orderby_str}"
                else:
                    extra_query = orderby_str

            records = client.fetch_page(src_table, limit=10, extra_query=extra_query)
            
            try:
                params = {"sysparm_count": "true"}
                if sn_query:
                    params["sysparm_query"] = sn_query
                resp = client._request("GET", f"/api/now/stats/{src_table}", params=params)
                resp.raise_for_status()
                stats_res = resp.json().get("result", {})
                total_count = int(stats_res.get("stats", {}).get("count", 0))
            except Exception as e:
                logger.error("ServiceNow preview count failed: %s", e)
                total_count = len(records)

        return jsonify({"ok": True, "records": records, "total_count": total_count})

    except Exception as exc:
        logger.error("Preview data failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────
@app.route("/api/autocomplete_values")
@login_required
def api_autocomplete_values():
    """Fetch unique field values from source for auto-complete filter suggestions."""
    field = request.args.get("field", "").strip()
    q = request.args.get("q", "").strip()
    if not field:
        return jsonify([])

    mt = session.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    
    source_table = session.get("source_table", "")
    if not source_table:
        return jsonify([])

    try:
        values = []
        if source_is_sn:
            client = _build_client("source")
            # Build query like: fieldLIKEq
            query = f"{field}LIKE{q}" if q else ""
            # Limit to 100 to avoid performance bottlenecks
            records = client.fetch_page(
                table_name=source_table,
                limit=100,
                fields=[field],
                extra_query=query,
            )
            seen = set()
            for r in records:
                val = r.get(field)
                if val is not None and val != "":
                    seen.add(str(val))
            values = sorted(list(seen))[:20]
        else:
            client = _build_sf_client("source")
            # Query SF with a SOQL filter
            where_clause = f" WHERE {field} LIKE '%{q}%'" if q else ""
            soql = f"SELECT {field} FROM {source_table}{where_clause} LIMIT 100"
            records = client.query(soql)
            seen = set()
            for r in records:
                val = r.get(field)
                if val is not None and val != "":
                    seen.add(str(val))
            values = sorted(list(seen))[:20]
        return jsonify(values)
    except Exception as e:
        logger.error("Error fetching autocomplete values: %s", e)
        return jsonify([])


@app.route("/api/check_coalesce")
@login_required
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
@login_required
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
@login_required
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
@login_required
def history():
    """Show migration history."""
    history_data = _load_history()
    # Attach live rollback status to each entry that has a rollback job
    rb_job_ids = [h.get("rollback_job_id") for h in history_data if h.get("rollback_job_id")]
    if rb_job_ids:
        try:
            store = RollbackStore(ROLLBACK_DB)
            for h in history_data:
                jid = h.get("rollback_job_id")
                if jid:
                    job = store.get_job(jid)
                    h["rollback_status"] = job["status"] if job else None
        except Exception:
            pass  # non-critical — just show button without status check
    return render_template("history.html", history=history_data)


@app.route("/history/<int:index>")
@login_required
def history_detail(index: int):
    """Show detail view for a single history entry."""
    all_history = _load_history()
    if index < 0 or index >= len(all_history):
        flash("History entry not found.", "error")
        return redirect(url_for("history"))
    entry = all_history[index]
    return render_template("history_detail.html", entry=entry, index=index)


@app.route("/api/clear_history", methods=["POST"])
@login_required
def api_clear_history():
    """Clear migration history. Admin clears all; user clears only their own."""
    if _is_admin():
        _save_history([])
    else:
        uid = _current_user_id()
        all_hist = _load_all_history()
        remaining = [h for h in all_hist if h.get("user_id") != uid]
        _save_history(remaining)
    flash("Migration history cleared.", "success")
    return redirect(url_for("history"))



@app.route("/api/report/pdf", methods=["POST"])
@login_required
def api_report_pdf():
    """AJAX/POST endpoint — generates a custom PDF report from JSON migration stats."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        from simple_pdf import SimplePDFWriter
        pdf_bytes = SimplePDFWriter().generate(data)
        
        from flask import make_response
        import time
        response = make_response(pdf_bytes)
        response.headers["Content-Type"] = "application/pdf"
        response.headers["Content-Disposition"] = f"attachment; filename=migratenow_report_{int(time.time())}.pdf"
        return response
    except Exception as exc:
        logger.error("PDF generation failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────
# Connections (saved credential profiles)
# ─────────────────────────────────────────────────────────────────────

@app.route("/connections", methods=["GET"])
@login_required
def connections_page():
    """Saved connections management page."""
    return render_template("connections.html", connections=_load_connections())


@app.route("/connections", methods=["POST"])
@login_required
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
        "user_id":  session.get("user_id", "system"),
        "owner_name": session.get("username", "system"),
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

    # Re-load ALL connections (not scoped) to append and save full file
    all_conns = _load_all_connections()
    all_conns.append(conn)
    _save_connections(all_conns)
    flash(f'Connection "{name}" saved.', "success")
    return redirect(url_for("connections_page"))


@app.route("/connections/<conn_id>/delete", methods=["POST"])
@login_required
def connections_delete(conn_id: str):
    """Delete a saved connection by ID (user can only delete own; admin can delete any)."""
    uid = session.get("user_id")
    all_conns = _load_all_connections()
    if _is_admin():
        new_conns = [c for c in all_conns if c.get("id") != conn_id]
    else:
        new_conns = [c for c in all_conns if not (c.get("id") == conn_id and c.get("user_id") == uid)]
    _save_connections(new_conns)
    flash("Connection deleted.", "success")
    return redirect(url_for("connections_page"))


@app.route("/api/connections")
@login_required
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
@login_required
def api_test_connection():
    """Test connectivity for given credentials. Returns JSON {ok, message}.
    
    Supports testing saved connections without re-entering the password:
    if password is blank/masked, looks it up from saved connections by
    matching instance+username (SN) or login_url+username (SF).
    """
    data     = request.get_json(force=True, silent=True) or {}
    platform = data.get("platform", "sn")
    conn_id  = data.get("conn_id", "").strip()  # optional: saved connection ID

    def _resolve_password(platform: str, identifier: str, username: str, raw_pw: str, raw_st: str = "") -> tuple[str, str]:
        """Return (password, security_token) — look up from saved connections if raw_pw is blank/masked."""
        if raw_pw and raw_pw != "••••••••":
            return raw_pw, raw_st
        # Try to find saved connection
        saved_conns = _load_connections()
        # First try by conn_id if provided
        if conn_id:
            for c in saved_conns:
                if c.get("id") == conn_id:
                    return c.get("password", ""), c.get("security_token", "")
        # Fall back: match by identifier + username
        for c in saved_conns:
            if c.get("platform") != platform:
                continue
            if platform == "sn" and c.get("instance", "").strip() == identifier and c.get("username", "").strip() == username:
                return c.get("password", ""), ""
            if platform == "sf" and c.get("login_url", "").strip() == identifier and c.get("username", "").strip() == username:
                return c.get("password", ""), c.get("security_token", "")
        return raw_pw, raw_st

    try:
        if platform == "sn":
            instance = data.get("instance", "").strip()
            username = data.get("username", "").strip()
            password = data.get("password", "")
            password, _ = _resolve_password("sn", instance, username, password)
            if not (instance and username and password):
                return jsonify({"ok": False, "message": "Instance URL, username and password are required."})
            tracker = _get_tracker()
            client = ServiceNowClient(instance=instance, username=username, password=password, role="test", tracker=tracker)
            client.fetch_page("sys_db_object", limit=1, fields=["name"])
            return jsonify({"ok": True, "message": f"Connected to {instance} successfully."})
        else:
            login_url      = data.get("login_url", "https://login.salesforce.com").strip()
            username       = data.get("username", "").strip()
            password       = data.get("password", "")
            security_token = data.get("security_token", "")
            password, security_token = _resolve_password("sf", login_url, username, password, security_token)
            if not (username and password):
                return jsonify({"ok": False, "message": "Username and password are required. Please save the connection first or enter credentials manually."})
            tracker = _get_tracker()
            sf_client = SalesforceClient(
                login_url=login_url,
                username=username,
                password=password,
                security_token=security_token,
                role="test",
                tracker=tracker,
            )
            objects = sf_client.get_objects()
            return jsonify({"ok": True, "message": f"Connected to Salesforce as {username}. {len(objects)} objects visible."})
    except Exception as exc:
        return jsonify({"ok": False, "message": _clean_error_message(exc)})


@app.route("/connections/<conn_id>/edit", methods=["POST"])
@login_required
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


@app.route("/migrate/repeat/<config_id>", methods=["POST"])
@login_required
def migrate_repeat(config_id: str):
    """Load a saved config into session and redirect to confirm page."""
    configs = _load_configs()
    cfg = next((c for c in configs if c["id"] == config_id), None)
    if not cfg:
        flash("Migration configuration not found.", "error")
        return redirect(url_for("home"))
    
    conns = _load_connections()
    
    mt = cfg["migration_type"]
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")

    session.clear()  # clear previous state
    session["migration_type"] = mt
    session["source_table"] = cfg["source_table"]
    session["target_table"] = cfg["target_table"]
    session["field_mapping"] = cfg["field_mapping"]
    session["filter_conditions"] = cfg.get("filter_conditions", [])
    session["fetch_mode"] = cfg.get("fetch_mode", "auto")
    session["limit"] = cfg.get("limit")
    session["orderby_field"] = cfg.get("orderby_field")
    session["orderby_dir"] = cfg.get("orderby_dir", "ASC")

    # Load source connection credentials into session
    if source_is_sn:
        inst = cfg.get("source_instance", "")
        conn = next((c for c in conns if c.get("instance") == inst and c.get("platform") == "sn"), None)
        if conn:
            session["source_instance"] = conn["instance"]
            session["source_username"] = conn["username"]
            session["source_password"] = conn["password"]
        else:
            flash(f"ServiceNow source connection '{inst}' not found. Please restore it in Connections page.", "error")
            return redirect(url_for("connect_page"))
    else:
        username = cfg.get("sf_source_username", "")
        login_url = cfg.get("sf_source_login_url", "")
        conn = next((c for c in conns if c.get("username") == username and c.get("login_url") == login_url and c.get("platform") == "sf"), None)
        if conn:
            session["sf_source_login_url"] = conn["login_url"]
            session["sf_source_username"] = conn["username"]
            session["sf_source_password"] = conn["password"]
            session["sf_source_security_token"] = conn.get("security_token", "")
        else:
            flash(f"Salesforce source connection '{username}' not found. Please restore it in Connections page.", "error")
            return redirect(url_for("connect_page"))

    # Load target connection credentials into session
    if target_is_sn:
        inst = cfg.get("target_instance", "")
        conn = next((c for c in conns if c.get("instance") == inst and c.get("platform") == "sn"), None)
        if conn:
            session["target_instance"] = conn["instance"]
            session["target_username"] = conn["username"]
            session["target_password"] = conn["password"]
        else:
            flash(f"ServiceNow target connection '{inst}' not found. Please restore it in Connections page.", "error")
            return redirect(url_for("connect_page"))
    else:
        username = cfg.get("sf_target_username", "")
        login_url = cfg.get("sf_target_login_url", "")
        conn = next((c for c in conns if c.get("username") == username and c.get("login_url") == login_url and c.get("platform") == "sf"), None)
        if conn:
            session["sf_target_login_url"] = conn["login_url"]
            session["sf_target_username"] = conn["username"]
            session["sf_target_password"] = conn["password"]
            session["sf_target_security_token"] = conn.get("security_token", "")
        else:
            flash(f"Salesforce target connection '{username}' not found. Please restore it in Connections page.", "error")
            return redirect(url_for("connect_page"))

    # Also populates _source_fields and metadata since fields.html expects it
    if cfg.get("field_mapping"):
        f_list = [{"name": k, "label": k} for k in cfg["field_mapping"].keys()]
        session["_source_fields"] = f_list
        return redirect(url_for("filters_page"))
    else:
        # Config was seeded from history without field_mapping — send user to the
        # fields step (table and connection are already in session).
        flash("Table and connection loaded. Please set up field mapping to continue.", "info")
        return redirect(url_for("fields"))


@app.route("/migrate/confirm", methods=["GET", "POST"])
@login_required
def migrate_confirm():
    """Show confirmation summary before running a repeated/pre-configured migration."""
    if request.method == "POST":
        return redirect(url_for("migrate"))

    mapping = session.get("field_mapping")
    if not mapping:
        flash("Configure field mapping first.", "error")
        return redirect(url_for("home"))

    mt = session.get("migration_type", "sn_sn")
    source_is_sn = mt in ("sn_sn", "sn_sf")
    target_is_sn = mt in ("sn_sn", "sf_sn")

    source_info = session.get("source_instance", "") if source_is_sn else session.get("sf_source_username", "")
    target_info = session.get("target_instance", "") if target_is_sn else session.get("sf_target_username", "")

    return render_template(
        "migrate_confirm.html",
        migration_type=mt,
        source_table=session.get("source_table"),
        target_table=session.get("target_table"),
        source_info=source_info,
        target_info=target_info,
        field_count=len(mapping),
        filter_conditions=session.get("filter_conditions", []),
        fetch_mode=session.get("fetch_mode", "auto"),
        limit=session.get("limit"),
    )


@app.route("/migrate", methods=["GET"])
@login_required
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
        "filter_conditions": session.get("filter_conditions", []),
        "limit": session.get("limit"),
        "coalesce_config": session.get("coalesce_config", {}),
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
@login_required
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


@app.route("/migrate/pause", methods=["POST"])
@login_required
def migrate_pause():
    _pause_event.clear()
    logger.info("Migration paused by user request.")
    return jsonify({"status": "paused"})


@app.route("/migrate/resume", methods=["POST"])
@login_required
def migrate_resume():
    _pause_event.set()
    logger.info("Migration resumed by user request.")
    return jsonify({"status": "running"})


# ─────────────────────────────────────────────────────────────────────
# Background migration runner
# ─────────────────────────────────────────────────────────────────────

def _run_migration(ctx: dict):
    """Execute the migration pipeline on a background thread."""
    global _progress_q
    _progress_q = queue.Queue()  # fresh queue for each run
    _pause_event.set()  # Make sure we start in running state

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
            event_type = "progress"
            if phase in ("paused", "resuming"):
                event_type = phase
            _progress_q.put(
                {
                    "event": event_type,
                    "data": {
                        "phase": phase,
                        "processed": processed,
                        "total": total,
                        "detail": detail,
                    },
                }
            )

        import uuid as _uuid
        rollback_job_id = _uuid.uuid4().hex[:12]

        rollback_store = RollbackStore(ROLLBACK_DB)
        target_label = ctx.get("target_table", "")
        source_label = ctx.get("source_table", "")
        target_instance_label = (
            ctx.get("target_instance")
            or ctx.get("sf_target_username", "")
        )
        rollback_store.create_job(
            job_id=rollback_job_id,
            label=f"{source_label} → {target_label}",
            migration_type=mt,
            target_platform="sn" if target_is_sn else "sf",
            target_table=target_label,
            target_instance=target_instance_label,
            created_at=datetime.now(timezone.utc).isoformat(),
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
            filter_conditions=ctx.get("filter_conditions"),
            pause_event=_pause_event,
            limit=ctx.get("limit"),
            rollback_store=rollback_store,
            rollback_job_id=rollback_job_id,
            coalesce_config=ctx.get("coalesce_config"),
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
            "rollback_job_id": rollback_job_id,
        })

        report_dict = report.to_dict()
        report_dict["rollback_job_id"] = rollback_job_id
        _progress_q.put(
            {"event": "complete", "data": report_dict}
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
# Rollback API routes
# ─────────────────────────────────────────────────────────────────────

@app.route("/api/rollback/status/<job_id>")
def api_rollback_status(job_id: str):
    """Return rollback job metadata (status, counts, label)."""
    store = RollbackStore(ROLLBACK_DB)
    job = store.get_job(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404
    return jsonify(job)


@app.route("/api/rollback/execute/<job_id>", methods=["POST"])
def api_rollback_execute(job_id: str):
    """Start the rollback for *job_id* on a background thread."""
    global _rollback_q, _rollback_result
    _rollback_q = queue.Queue()
    _rollback_result = {}

    store = RollbackStore(ROLLBACK_DB)
    job = store.get_job(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404
    if job["status"] not in ("captured",):
        return jsonify({"error": f"Cannot roll back — status is '{job['status']}'"}), 409

    # Rebuild the target client from the session (still in memory during this request)
    mt = job.get("migration_type", "sn_sn")
    target_platform = job.get("target_platform", "sn")

    def _build_target_client():
        """Rebuild target client from session."""
        if target_platform == "sf":
            return SalesforceClient(
                login_url=session.get("sf_target_login_url", "https://login.salesforce.com"),
                username=session.get("sf_target_username", ""),
                password=session.get("sf_target_password", ""),
                security_token=session.get("sf_target_security_token", ""),
                role="target",
                tracker=RateTracker(),
            )
        else:
            return ServiceNowClient(
                instance=session.get("target_instance", ""),
                username=session.get("target_username", ""),
                password=session.get("target_password", ""),
                role="target",
                tracker=RateTracker(),
            )

    # Capture session data before leaving request context
    rb_session = {
        "sf_target_login_url": session.get("sf_target_login_url", "https://login.salesforce.com"),
        "sf_target_username": session.get("sf_target_username", ""),
        "sf_target_password": session.get("sf_target_password", ""),
        "sf_target_security_token": session.get("sf_target_security_token", ""),
        "target_instance": session.get("target_instance", ""),
        "target_username": session.get("target_username", ""),
        "target_password": session.get("target_password", ""),
    }

    def _run_rollback():
        try:
            if target_platform == "sf":
                tgt_client = SalesforceClient(
                    login_url=rb_session["sf_target_login_url"],
                    username=rb_session["sf_target_username"],
                    password=rb_session["sf_target_password"],
                    security_token=rb_session["sf_target_security_token"],
                    role="target",
                    tracker=RateTracker(),
                )
            else:
                tgt_client = ServiceNowClient(
                    instance=rb_session["target_instance"],
                    username=rb_session["target_username"],
                    password=rb_session["target_password"],
                    role="target",
                    tracker=RateTracker(),
                )

            rb_store = RollbackStore(ROLLBACK_DB)

            def _rb_progress(phase, processed, total, detail=""):
                _rollback_q.put({
                    "event": "progress",
                    "data": {
                        "phase": phase,
                        "processed": processed,
                        "total": total,
                        "detail": detail,
                    }
                })

            executor = RollbackExecutor(
                store=rb_store,
                target_client=tgt_client,
                target_table=job["target_table"],
                target_platform=target_platform,
                progress_callback=_rb_progress,
            )
            result = executor.run(job_id)
            _rollback_result.update({
                "deleted": result.deleted,
                "restored": result.restored,
                "failed": result.failed,
                "elapsed_seconds": result.elapsed_seconds,
                "errors": result.errors[:10],  # cap for JSON size
            })
            _rollback_q.put({"event": "complete", "data": _rollback_result})
        except Exception as exc:
            logger.exception("Rollback failed: %s", exc)
            _rollback_q.put({
                "event": "error_msg",
                "data": {"message": str(exc)},
            })

    threading.Thread(target=_run_rollback, daemon=True).start()
    return jsonify({"status": "started", "job_id": job_id})


@app.route("/api/rollback/stream/<job_id>")
def api_rollback_stream(job_id: str):
    """SSE stream for rollback progress."""
    def generate() -> Generator[str, None, None]:
        while True:
            try:
                msg = _rollback_q.get(timeout=30)
            except queue.Empty:
                yield "event: heartbeat\ndata: {}\n\n"
                continue

            event_type = msg.get("event", "progress")
            data = json.dumps(msg.get("data", {}))
            yield f"event: {event_type}\ndata: {data}\n\n"

            if event_type in ("complete", "error_msg"):
                break

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/rollback/discard/<job_id>", methods=["POST"])
def api_rollback_discard(job_id: str):
    """Delete rollback data for *job_id* from the database."""
    store = RollbackStore(ROLLBACK_DB)
    job = store.get_job(job_id)
    if not job:
        return jsonify({"error": "not_found"}), 404
    store.delete_job(job_id)
    return jsonify({"status": "discarded"})


# ═════════════════════════════════════════════════════════════════════
# Data Ops (SOQL Queries & Bulk Actions)
# ═════════════════════════════════════════════════════════════════════

@app.route("/data-ops")
@login_required
def data_ops_page():
    """Render the Data Ops manager page."""
    return render_template("data_ops.html", connections=_load_connections())


@app.route("/api/data-ops/query", methods=["POST"])
@login_required
def api_data_ops_query():
    """Execute a query (SOQL for SF, table:query for SN) on a saved connection."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        conn_id = data.get("conn_id")
        query = data.get("query", "").strip()
        if not conn_id or not query:
            return jsonify({"error": "Connection profile and query are required."}), 400

        conns = _load_connections()
        c = next((x for x in conns if x.get("id") == conn_id), None)
        if not c:
            return jsonify({"error": "Connection profile not found."}), 404

        tracker = _get_tracker()
        if c.get("platform") == "sf":
            sf = SalesforceClient(
                login_url=c["login_url"],
                username=c["username"],
                password=c["password"],
                security_token=c.get("security_token", ""),
                role="data_ops",
                tracker=tracker
            )
            records = sf.query(query)
            return jsonify({"ok": True, "records": records, "platform": "sf"})
        else:
            client = ServiceNowClient(
                instance=c["instance"],
                username=c["username"],
                password=c["password"],
                role="data_ops",
                tracker=tracker
            )
            if ":" not in query:
                return jsonify({"error": "ServiceNow query format must be: table_name:encoded_query (e.g. sys_user:active=true)"}), 400
            table, sys_query = query.split(":", 1)
            records = client.fetch_page(table.strip(), limit=200, extra_query=sys_query.strip())
            return jsonify({"ok": True, "records": records, "platform": "sn"})
    except Exception as exc:
        logger.error("Data Ops query failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/data-ops/export", methods=["POST"])
def api_data_ops_export():
    """Export records from a saved connection as CSV or JSON."""
    try:
        import io
        import csv
        data = request.get_json(force=True, silent=True) or {}
        conn_id = data.get("conn_id")
        table = data.get("table", "").strip()
        fields_str = data.get("fields", "").strip()
        fmt = data.get("format", "csv").lower()
        sys_query = data.get("filter", "").strip()
        limit_val = data.get("limit")

        if not conn_id or not table:
            return jsonify({"error": "Connection profile and table name are required."}), 400

        conns = _load_connections()
        c = next((x for x in conns if x.get("id") == conn_id), None)
        if not c:
            return jsonify({"error": "Connection profile not found."}), 404

        tracker = _get_tracker()
        records = []

        try:
            limit = int(limit_val) if limit_val else 1000
        except ValueError:
            limit = 1000

        if c.get("platform") == "sf":
            sf = SalesforceClient(
                login_url=c["login_url"],
                username=c["username"],
                password=c["password"],
                security_token=c.get("security_token", ""),
                role="data_ops",
                tracker=tracker
            )
            # If fields_str is empty, retrieve object fields
            if fields_str:
                field_list = [f.strip() for f in fields_str.split(",") if f.strip()]
            else:
                f_meta = sf.get_object_fields(table)
                field_list = [f["name"] for f in f_meta if f.get("type") not in ("address", "location")]
                if not field_list:
                    field_list = ["Id", "Name"]

            query = f"SELECT {', '.join(field_list)} FROM {table}"
            if sys_query:
                if not sys_query.upper().startswith("WHERE"):
                    query += f" WHERE {sys_query}"
                else:
                    query += f" {sys_query}"
            query += f" LIMIT {limit}"
            records = sf.query(query)
        else:
            client = ServiceNowClient(
                instance=c["instance"],
                username=c["username"],
                password=c["password"],
                role="data_ops",
                tracker=tracker
            )
            field_list = [f.strip() for f in fields_str.split(",") if f.strip()] if fields_str else None
            records = []
            last_sys_id = ""
            remaining = limit
            while remaining > 0:
                fetch_size = min(remaining, 500)
                batch = client.fetch_page(table, last_sys_id=last_sys_id, limit=fetch_size, fields=field_list, extra_query=sys_query)
                if not batch:
                    break
                records.extend(batch)
                remaining -= len(batch)
                if len(batch) < fetch_size:
                    break
                last_sys_id = batch[-1].get("sys_id", "")

        # Export formatting
        if fmt == "json":
            res_content = json.dumps(records, indent=2, ensure_ascii=False)
            return Response(
                res_content,
                mimetype="application/json",
                headers={"Content-disposition": f"attachment; filename=export_{table}_{int(datetime.now().timestamp())}.json"}
            )
        else:
            if not records:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(["No records found"])
                return Response(
                    output.getvalue(),
                    mimetype="text/csv",
                    headers={"Content-disposition": f"attachment; filename=export_{table}_{int(datetime.now().timestamp())}.csv"}
                )
            
            keys = set()
            for r in records:
                for k in r.keys():
                    if k != "attributes":
                        keys.add(k)
            headers = sorted(list(keys))

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(headers)
            for r in records:
                row = []
                for h in headers:
                    val = r.get(h, "")
                    if isinstance(val, dict):
                        val = json.dumps(val)
                    row.append(val)
                writer.writerow(row)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename=export_{table}_{int(datetime.now().timestamp())}.csv"}
            )

    except Exception as exc:
        logger.error("Data Ops export failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/data-ops/insert", methods=["POST"])
def api_data_ops_insert():
    """Insert a list of records into a table/object on a saved connection."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        conn_id = data.get("conn_id")
        table = data.get("table", "").strip()
        records = data.get("records", [])
        if not conn_id or not table or not records:
            return jsonify({"error": "Connection, table/object, and records are required."}), 400

        conns = _load_connections()
        c = next((x for x in conns if x.get("id") == conn_id), None)
        if not c:
            return jsonify({"error": "Connection profile not found."}), 404

        tracker = _get_tracker()
        success_count = 0
        failed_count = 0

        if c.get("platform") == "sf":
            sf = SalesforceClient(
                login_url=c["login_url"],
                username=c["username"],
                password=c["password"],
                security_token=c.get("security_token", ""),
                role="data_ops",
                tracker=tracker
            )
            from migration.sf_loader import SalesforceLoader
            loader = SalesforceLoader(sf, table, {})
            # Use inner helper
            ok, fail, _, _ = loader._collections_insert(records)
            return jsonify({"ok": True, "inserted": ok, "failed": fail})
        else:
            client = ServiceNowClient(
                instance=c["instance"],
                username=c["username"],
                password=c["password"],
                role="data_ops",
                tracker=tracker
            )
            for rec in records:
                try:
                    cleaned = {k: v for k, v in rec.items() if k not in ("sys_id", "sys_created_on", "sys_created_by", "sys_updated_on", "sys_updated_by")}
                    resp = client._request("POST", f"/api/now/table/{table}", json_body=cleaned)
                    resp.raise_for_status()
                    success_count += 1
                except Exception as e:
                    logger.error("ServiceNow single insert failed: %s", e)
                    failed_count += 1
            return jsonify({"ok": True, "inserted": success_count, "failed": failed_count})
    except Exception as exc:
        logger.error("Data Ops insert failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/data-ops/delete", methods=["POST"])
def api_data_ops_delete():
    """Delete records by ID/sys_id from a table/object on a saved connection."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        conn_id = data.get("conn_id")
        table = data.get("table", "").strip()
        ids = data.get("ids", [])
        if not conn_id or not table or not ids:
            return jsonify({"error": "Connection, table/object, and list of IDs are required."}), 400

        conns = _load_connections()
        c = next((x for x in conns if x.get("id") == conn_id), None)
        if not c:
            return jsonify({"error": "Connection profile not found."}), 404

        tracker = _get_tracker()
        success_count = 0
        failed_count = 0

        if c.get("platform") == "sf":
            sf = SalesforceClient(
                login_url=c["login_url"],
                username=c["username"],
                password=c["password"],
                security_token=c.get("security_token", ""),
                role="data_ops",
                tracker=tracker
            )
            for i in range(0, len(ids), 200):
                chunk = ids[i:i+200]
                try:
                    resp = sf._request(
                        "DELETE",
                        "/services/data/v57.0/composite/sobjects",
                        params={"ids": ",".join(chunk)}
                    )
                    resp.raise_for_status()
                    for item in resp.json():
                        if item.get("success"):
                            success_count += 1
                        else:
                            failed_count += 1
                except Exception as e:
                    logger.error("Salesforce chunk delete failed: %s", e)
                    failed_count += len(chunk)
            return jsonify({"ok": True, "deleted": success_count, "failed": failed_count})
        else:
            client = ServiceNowClient(
                instance=c["instance"],
                username=c["username"],
                password=c["password"],
                role="data_ops",
                tracker=tracker
            )
            for sys_id in ids:
                try:
                    resp = client._request("DELETE", f"/api/now/table/{table}/{sys_id}")
                    resp.raise_for_status()
                    success_count += 1
                except Exception as e:
                    logger.error("ServiceNow delete failed: %s", e)
                    failed_count += 1
            return jsonify({"ok": True, "deleted": success_count, "failed": failed_count})
    except Exception as exc:
        logger.error("Data Ops delete failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting SN Migration Tool on port %d", FLASK_PORT)
    _migrate_legacy_data()         # tag existing legacy data with first admin ID
    _seed_configs_from_history()   # populate recent configs from history if not yet seeded
    app.run(
        host="0.0.0.0",
        port=FLASK_PORT,
        debug=False,
        threaded=True,
    )
