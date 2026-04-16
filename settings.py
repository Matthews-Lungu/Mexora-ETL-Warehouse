"""
settings.py — Mexora ETL Configuration
=======================================
All connection parameters and file paths are centralised here.
Sensitive credentials are loaded from a .env file using python-dotenv.
Never hard-code passwords in source code.

Usage:
    from config.settings import DB_URL, DATA_DIR
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env file (must exist at project root) ────────────────────────────
load_dotenv()

# ── Project root (one level above this file) ──────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent

# ── Data source paths ─────────────────────────────────────────────────────
DATA_DIR = ROOT_DIR / "data"

ORDERS_FILE   = DATA_DIR / "orders_mexora.csv"
CLIENTS_FILE  = DATA_DIR / "clients_mexora.csv"
PRODUCTS_FILE = DATA_DIR / "products_mexora.json"
REGIONS_FILE  = DATA_DIR / "regions_maroc.csv"

# ── Log directory ─────────────────────────────────────────────────────────
LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ── PostgreSQL connection ─────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mexora_dwh")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── DWH schema names ──────────────────────────────────────────────────────
SCHEMA_STAGING   = "staging_mexora"
SCHEMA_DWH       = "dwh_mexora"
SCHEMA_REPORTING = "reporting_mexora"

# ── Business rules ────────────────────────────────────────────────────────
# Customer segmentation thresholds (12-month revenue in MAD)
SEGMENT_GOLD   = 15_000
SEGMENT_SILVER =  5_000

# Time dimension range
DIM_TEMPS_START = "2020-01-01"
DIM_TEMPS_END   = "2025-12-31"

# VAT rate in Morocco (standard)
TVA_RATE = 0.20

# Delivery delay threshold for "late" classification (days)
LATE_DELIVERY_DAYS = 3
