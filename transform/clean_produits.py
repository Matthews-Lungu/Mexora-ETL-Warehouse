"""
transform/clean_produits.py — Product Cleaning
================================================
Applies cleaning rules to raw product data extracted from the JSON.

Business Rules:
    R1 — Normalise category capitalisation to Title Case
    R2 — Flag inactive products that still have associated orders
         (these are valid for SCD Type 2 history — do NOT delete them)
    R3 — Fill null catalogue prices with the median price of their
         subcategory (business assumption: null = data entry omission)
"""

import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)


def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw products DataFrame.

    Args:
        df : Raw products DataFrame from extract_products().

    Returns:
        Cleaned pd.DataFrame.
    """
    initial = len(df)
    logger.info(f"[TRANSFORM] produits  : début avec {initial:,} lignes")

    # ── R1 — Normalise category capitalisation ────────────────────────────
    # Issue: "electronique", "ELECTRONIQUE", "Electronique" must all → "Electronique"
    for col in ["categorie", "sous_categorie"]:
        before_unique = df[col].nunique()
        df[col] = df[col].str.strip().str.title()
        after_unique  = df[col].nunique()
        logger.info(
            f"[TRANSFORM] R1 normalisation ({col}) : "
            f"{before_unique} variantes → {after_unique} valeurs uniques"
        )

    # ── R2 — Identify inactive products with order risk ───────────────────
    # We keep inactive products in the dimension (SCD Type 2 will handle them)
    # but we log a warning so analysts are aware.
    df["actif"] = df["actif"].str.lower().str.strip() == "true"
    n_inactive = (~df["actif"]).sum()
    logger.info(
        f"[TRANSFORM] R2 produits inactifs : {n_inactive} produits inactifs "
        f"conservés pour historique SCD Type 2"
    )

    # ── R3 — Fill null catalogue prices ───────────────────────────────────
    df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")
    n_null_prices = df["prix_catalogue"].isna().sum()

    if n_null_prices > 0:
        # Fill with subcategory median; fall back to category median
        subcat_median = df.groupby("sous_categorie")["prix_catalogue"].transform("median")
        cat_median    = df.groupby("categorie")["prix_catalogue"].transform("median")
        df["prix_catalogue"] = (
            df["prix_catalogue"]
            .fillna(subcat_median)
            .fillna(cat_median)
            .round(2)
        )
        logger.info(
            f"[TRANSFORM] R3 prix nuls : {n_null_prices} prix null → "
            f"médiane sous-catégorie/catégorie"
        )

    # ── Standardise date columns ───────────────────────────────────────────
    df["date_creation"] = pd.to_datetime(df["date_creation"], errors="coerce")

    logger.info(f"[TRANSFORM] produits  : TERMINÉ  {len(df):,} lignes (aucune suppression)")
    return df
