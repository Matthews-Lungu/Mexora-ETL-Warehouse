"""
utils/logger.py — Mexora ETL Logging
=====================================
Sets up a logger that writes simultaneously to:
  - A timestamped file in the /logs directory
  - The terminal (stdout)

Usage:
    from utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Something happened")
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# ── Resolve log directory relative to this file ───────────────────────────
_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

# ── One log file per pipeline run (timestamped) ───────────────────────────
_LOG_FILE = _LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# ── Shared formatter ──────────────────────────────────────────────────────
_FORMATTER = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(name)-30s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger with file + stream handlers attached.
    Calling this multiple times with the same name is safe — handlers
    are only added once.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — avoid adding duplicate handlers
        return logger

    logger.setLevel(logging.DEBUG)

    # File handler — captures DEBUG and above
    fh = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_FORMATTER)

    # Stream handler — shows INFO and above in terminal
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(_FORMATTER)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger
