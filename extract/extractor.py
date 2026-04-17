"""
extract/extractor.py — Mexora ETL Extraction Layer
====================================================
Each function reads ONE raw source file and returns a DataFrame
with NO transformations applied.  All columns are kept as strings
(dtype=str) to prevent pandas from making silent type conversions
that would hide data quality issues we need to detect in the
Transform phase.

Functions:
    extract_orders(filepath)   → DataFrame  (CSV)
    extract_clients(filepath)  → DataFrame  (CSV)
    extract_products(filepath) → DataFrame  (JSON nested under "produits")
    extract_regions(filepath)  → DataFrame  (CSV — clean reference)
"""

import json
import pandas as pd
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)


def extract_orders(filepath: str | Path) -> pd.DataFrame:
    """
    Extract raw orders from orders_mexora.csv.

    All columns are read as strings to preserve mixed date formats,
    non-standard status codes, and other quality issues for downstream
    cleaning.

    Returns:
        pd.DataFrame — raw orders, unmodified.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"[EXTRACT] Orders file not found: {filepath}")

    df = pd.read_csv(
        filepath,
        dtype=str,           # everything as string — no silent conversions
        encoding="utf-8",
        keep_default_na=False,   # keep empty strings as "" not NaN (for livreur check)
    )

    logger.info(f"[EXTRACT] orders     : {len(df):,} rows from {filepath.name}")
    logger.debug(f"[EXTRACT] orders columns: {df.columns.tolist()}")
    return df


def extract_clients(filepath: str | Path) -> pd.DataFrame:
    """
    Extract raw client records from clients_mexora.csv.

    Returns:
        pd.DataFrame — raw clients, unmodified.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"[EXTRACT] Clients file not found: {filepath}")

    df = pd.read_csv(
        filepath,
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
    )

    logger.info(f"[EXTRACT] clients    : {len(df):,} rows from {filepath.name}")
    return df


def extract_products(filepath: str | Path) -> pd.DataFrame:
    """
    Extract product catalogue from products_mexora.json.
    The JSON structure is: {"produits": [...]}

    Returns:
        pd.DataFrame — one row per product, all columns as object dtype.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"[EXTRACT] Products file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if "produits" not in raw:
        raise KeyError("[EXTRACT] Expected key 'produits' not found in products JSON.")

    df = pd.DataFrame(raw["produits"])

    # Normalise boolean column to string to keep uniform dtype handling
    df["actif"] = df["actif"].astype(str)

    logger.info(f"[EXTRACT] products   : {len(df):,} rows from {filepath.name}")
    return df


def extract_regions(filepath: str | Path) -> pd.DataFrame:
    """
    Extract the official Morocco geographic reference table.
    This file is clean — minimal processing needed.

    Returns:
        pd.DataFrame — region reference data.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"[EXTRACT] Regions file not found: {filepath}")

    df = pd.read_csv(filepath, dtype=str, encoding="utf-8")

    logger.info(f"[EXTRACT] regions    : {len(df):,} rows from {filepath.name}")
    return df
