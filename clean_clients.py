"""
transform/clean_clients.py — Client Cleaning & Segmentation
=============================================================
Applies all 5 cleaning rules plus customer segmentation.

Business Rules:
    R1 — Deduplicate on normalised email (keep most recent inscription)
    R2 — Standardise gender to 'm' / 'f' / 'inconnu'
    R3 — Validate birth dates (age must be between 16 and 100)
    R4 — Validate email format (regex check)
    R5 — Compute customer segment Gold/Silver/Bronze from order history

Design choice on R1:
    We deduplicate on email (normalised to lowercase + stripped).
    We keep the MOST RECENT inscription date because it reflects the
    customer's latest known state. The older duplicate ID is discarded.
    The surviving record keeps its original id_client.
"""

import re
import pandas as pd
from datetime import date
from utils.logger import get_logger
from config.settings import SEGMENT_GOLD, SEGMENT_SILVER

logger = get_logger(__name__)

# ── Gender normalisation map ───────────────────────────────────────────────
GENDER_MAP: dict[str, str] = {
    "m":      "m", "f":      "f",
    "1":      "m", "0":      "f",
    "h":      "m",
    "homme":  "m", "femme":  "f",
    "male":   "m", "female": "f",
    "m":      "m", "f":      "f",
}

# ── Email validation regex ─────────────────────────────────────────────────
EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw client data.

    Args:
        df : Raw clients DataFrame (all columns as str).

    Returns:
        Cleaned pd.DataFrame with an added 'tranche_age' column.
        Note: segment_client is NOT added here — it requires the
        cleaned orders DataFrame and is computed in build_dimensions.py.
    """
    initial = len(df)
    logger.info(f"[TRANSFORM] clients   : début avec {initial:,} lignes")

    # ── R1 — Deduplicate on normalised email ──────────────────────────────
    df["email_norm"] = df["email"].str.lower().str.strip()
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")
    before = len(df)
    df = (
        df.sort_values("date_inscription", ascending=True)
          .drop_duplicates(subset=["email_norm"], keep="last")
    )
    logger.info(f"[TRANSFORM] R1 doublons email  : {before - len(df)} doublons supprimés")

    # ── R2 — Standardise gender ───────────────────────────────────────────
    df["sexe"] = (
        df["sexe"].str.lower().str.strip()
                  .map(GENDER_MAP)
                  .fillna("inconnu")
    )
    n_inconnu = (df["sexe"] == "inconnu").sum()
    logger.info(f"[TRANSFORM] R2 sexe            : {n_inconnu} valeurs non reconnues → 'inconnu'")

    # ── R3 — Validate birth dates ─────────────────────────────────────────
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    today = pd.Timestamp(date.today())
    df["age"] = ((today - df["date_naissance"]).dt.days // 365).astype("Int64")

    invalid_age_mask = (df["age"] < 16) | (df["age"] > 100) | df["age"].isna()
    n_invalid = invalid_age_mask.sum()
    df.loc[invalid_age_mask, "date_naissance"] = pd.NaT
    df.loc[invalid_age_mask, "age"] = pd.NA
    logger.info(f"[TRANSFORM] R3 date_naissance  : {n_invalid} âges invalides → NaT")

    # Derive age bracket — unknown age clients fall into a dedicated bucket
    age_filled = df["age"].fillna(-1).astype(int)
    df["tranche_age"] = pd.cut(
        age_filled,
        bins=[-2, 0, 18, 25, 35, 45, 55, 65, 200],
        labels=["inconnu", "<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
    ).astype(str)
    df.loc[df["age"].isna(), "tranche_age"] = "inconnu"

    # ── R4 — Validate email format ────────────────────────────────────────
    valid_mask   = df["email"].str.match(EMAIL_RE, na=False)
    n_malformed  = (~valid_mask).sum()
    df.loc[~valid_mask, "email"] = None
    logger.info(f"[TRANSFORM] R4 emails          : {n_malformed} emails invalides → NULL")

    # ── Clean city names (same pattern as orders) ─────────────────────────
    # Note: harmonisation to standard name is done in build_dim_client
    # where we have the regions reference available.

    # ── Drop helper columns ───────────────────────────────────────────────
    df = df.drop(columns=["email_norm"])

    total_removed = initial - len(df)
    logger.info(
        f"[TRANSFORM] clients   : TERMINÉ  {initial:,} → {len(df):,} lignes "
        f"({total_removed:,} supprimées)"
    )
    return df


def calculer_segments_clients(
    df_commandes: pd.DataFrame,
    df_clients: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute customer segment (Gold/Silver/Bronze) from last 12 months
    of delivered orders and merge back onto the clients DataFrame.

    Business rules (from settings.py):
        Gold   : cumulative revenue >= 15,000 MAD
        Silver : cumulative revenue >=  5,000 MAD
        Bronze : cumulative revenue <   5,000 MAD

    Clients with no orders in the period are classified Bronze.

    Returns:
        df_clients with added 'segment_client' and 'ca_12m' columns.
    """
    from datetime import timedelta

    # Use the most recent order date in the data as the reference point.
    # This prevents all clients from being Bronze when running with
    # historical data whose dates predate "today" by more than 12 months.
    if df_commandes["date_commande"].isna().all():
        date_limite = pd.Timestamp(date.today() - timedelta(days=365))
    else:
        latest_order = df_commandes["date_commande"].max()
        date_limite  = latest_order - pd.DateOffset(months=12)

    df_recents = df_commandes[
        (df_commandes["date_commande"] >= date_limite) &
        (df_commandes["statut"] == "livré")
    ].copy()

    if df_recents.empty:
        logger.warning("[TRANSFORM] R5 segmentation : aucune commande récente — tous clients → Bronze")
        df_clients["segment_client"] = "Bronze"
        df_clients["ca_12m"] = 0.0
        return df_clients

    ca_par_client = (
        df_recents.groupby("id_client")["montant_ttc"]
                  .sum()
                  .reset_index()
                  .rename(columns={"montant_ttc": "ca_12m"})
    )

    def segmenter(ca: float) -> str:
        if ca >= SEGMENT_GOLD:   return "Gold"
        if ca >= SEGMENT_SILVER: return "Silver"
        return "Bronze"

    ca_par_client["segment_client"] = ca_par_client["ca_12m"].apply(segmenter)

    df_clients = df_clients.merge(ca_par_client, on="id_client", how="left")
    df_clients["segment_client"] = df_clients["segment_client"].fillna("Bronze")
    df_clients["ca_12m"]         = df_clients["ca_12m"].fillna(0.0)

    dist = df_clients["segment_client"].value_counts().to_dict()
    logger.info(f"[TRANSFORM] R5 segmentation    : {dist}")
    return df_clients
