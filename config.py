"""
Centralised configuration for the ServiceNow Migration Tool.

All tunables are loaded once from environment variables (via .env) and
exposed as module-level constants so that every other module can simply
``from config import X``.
"""

import os
import logging
import logging.handlers
from pathlib import Path

from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────
load_dotenv()

# ── Paths ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── Source Instance ──────────────────────────────────────────────────
SOURCE_INSTANCE: str = os.getenv("SOURCE_INSTANCE", "")
SOURCE_USERNAME: str = os.getenv("SOURCE_USERNAME", "")
SOURCE_PASSWORD: str = os.getenv("SOURCE_PASSWORD", "")

# ── Target Instance ──────────────────────────────────────────────────
TARGET_INSTANCE: str = os.getenv("TARGET_INSTANCE", "")
TARGET_USERNAME: str = os.getenv("TARGET_USERNAME", "")
TARGET_PASSWORD: str = os.getenv("TARGET_PASSWORD", "")

# ── Salesforce Source Instance ───────────────────────────────────
SF_SOURCE_LOGIN_URL: str = os.getenv("SF_SOURCE_LOGIN_URL", "https://login.salesforce.com")
SF_SOURCE_CLIENT_ID: str = os.getenv("SF_SOURCE_CLIENT_ID", "")
SF_SOURCE_CLIENT_SECRET: str = os.getenv("SF_SOURCE_CLIENT_SECRET", "")
SF_SOURCE_USERNAME: str = os.getenv("SF_SOURCE_USERNAME", "")
SF_SOURCE_PASSWORD: str = os.getenv("SF_SOURCE_PASSWORD", "")
SF_SOURCE_SECURITY_TOKEN: str = os.getenv("SF_SOURCE_SECURITY_TOKEN", "")

# ── Salesforce Target Instance ───────────────────────────────────
SF_TARGET_LOGIN_URL: str = os.getenv("SF_TARGET_LOGIN_URL", "https://login.salesforce.com")
SF_TARGET_CLIENT_ID: str = os.getenv("SF_TARGET_CLIENT_ID", "")
SF_TARGET_CLIENT_SECRET: str = os.getenv("SF_TARGET_CLIENT_SECRET", "")
SF_TARGET_USERNAME: str = os.getenv("SF_TARGET_USERNAME", "")
SF_TARGET_PASSWORD: str = os.getenv("SF_TARGET_PASSWORD", "")
SF_TARGET_SECURITY_TOKEN: str = os.getenv("SF_TARGET_SECURITY_TOKEN", "")

# ── Salesforce Tuning ────────────────────────────────────────────
SF_API_VERSION: str = os.getenv("SF_API_VERSION", "62.0")
SF_BULK_POLL_INTERVAL: int = int(os.getenv("SF_BULK_POLL_INTERVAL", "5"))
SF_COLLECTIONS_BATCH_SIZE: int = int(os.getenv("SF_COLLECTIONS_BATCH_SIZE", "200"))

# ── Migration Tuning ────────────────────────────────────────────────
FETCH_PAGE_SIZE: int = int(os.getenv("FETCH_PAGE_SIZE", "10000"))
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
# Max parallel threads for fetching and loading
MAX_CONCURRENCY: int = int(os.getenv("MAX_CONCURRENCY", "8"))
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "300"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_FACTOR: float = float(os.getenv("RETRY_BACKOFF_FACTOR", "1.0"))
# Add after existing imports
CSV_EXPORT_TIMEOUT: int = int(os.getenv("CSV_EXPORT_TIMEOUT", "600"))

CSV_PARTITIONS: int = int(os.getenv("CSV_PARTITIONS", "4"))   # Number of parallel CSV partitions
MAX_WORKERS_CSV: int = int(os.getenv("MAX_WORKERS_CSV", "4")) # Threads for CSV fetch
MAX_WORKERS_LOAD: int = int(os.getenv("MAX_WORKERS_LOAD", "2")) # Parallel JSONv2 chunks
JSONV2_CHUNK_SIZE: int = int(os.getenv("JSONV2_CHUNK_SIZE", "0")) # If 0, auto-calculates
# ── CSV Export Processor ────────────────────────────────────────────
# Timeout for CSV export streaming downloads (seconds).
# Large tables (100k+ records) may need 600–900s on slower instances.

# ── Logging ──────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Flask ────────────────────────────────────────────────────────────
FLASK_SECRET: str = os.getenv("FLASK_SECRET", "sn-migration-secret-key")
FLASK_PORT: int = int(os.getenv("FLASK_PORT", "5000"))


def setup_logging(name: str = "sn_migration") -> logging.Logger:
    """
    Return a fully-configured logger with both rotating-file and console
    handlers.  Call once at application start; thereafter use
    ``logging.getLogger("sn_migration")`` anywhere.

    - File handler  : 10 MB per file, 5 backups, JSON-like structured lines.
    - Console handler: human-readable, coloured level names.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # ── Formatters ───────────────────────────────────────────────────
    file_fmt = logging.Formatter(
        fmt=(
            '{"ts":"%(asctime)s","level":"%(levelname)s",'
            '"module":"%(module)s","func":"%(funcName)s",'
            '"line":%(lineno)d,"msg":"%(message)s"}'
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    console_fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(module)-16s │ %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── Rotating File Handler ────────────────────────────────────────
    log_file = LOG_DIR / "migration.log"
    fh = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)  # capture everything in the file
    fh.setFormatter(file_fmt)

    # ── Console Handler ──────────────────────────────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    ch.setFormatter(console_fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
