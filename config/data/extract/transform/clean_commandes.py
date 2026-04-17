"""
transform/clean_commandes.py — Order Cleaning
===============================================
Applies all 7 business rules for cleaning raw order data.

Business Rules:
    R1 — Remove duplicates on id_commande (keep last occurrence)
    R2 — Standardise date formats to YYYY-MM-DD
    R3 — Harmonise city names via the regions_maroc reference file
    R4 — Standardise order statuses to canonical values
    R5 — Remove rows with quantity <= 0 (data entry errors)
    R6 — Remove rows with unit_price = 0 (test orders)
    R7 — Replace missing delivery person IDs with '-1' (unknown)

Each rule logs: lines before, lines after, and lines affected.
"""

import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Canonical status mapping ───────────────────────────────────────────────
# Maps every known dirty variant → one of the 4 canonical values.
STATUS_MAPPING: dict[str, str] = {
    # livré variants
    "livré":    "livré",
    "livre":    "livré",
    "LIVRE":    "livré",
    "DONE":     "livré",
    # annulé variants
    "annulé":   "annulé",
    "annule":   "annulé",
    "KO":       "annulé",
    # en_cours variants
    "en_cours": "en_cours",
    "OK":       "en_cours",
    # retourné variants
    "retourné": "retourné",
    "retourne": "retourné",
}

VALID_STATUSES = {"livré", "annulé", "en_cours", "retourné"}


def charger_referentiel_villes(filepath: str) -> dict[str, str]:
    """
    Build a mapping {dirty_variant → standard_name} from regions_maroc.csv.

    Strategy:
      - All lowercase versions of the standard name map to it
      - The city code (e.g. "TNG") maps to the standard name
      - The standard name itself maps to itself
    This covers the main dirty patterns: lowercase, codes, abbreviations.

    Returns:
        dict — keys are lowercase dirty variants, values are standard names.
    """
    df = pd.read_csv(filepath, dtype=str)
    mapping: dict[str, str] = {}

    for _, row in df.iterrows():
        std = row["nom_ville_standard"].strip()
        code = row["code_ville"].strip().lower()

        # Map: standard name (various cases) → standard
        mapping[std.lower()]        = std
        mapping[std.upper()]        = std
        mapping[std]                = std
        # Map: city code → standard
        mapping[code]               = std
        mapping[code.upper()]       = std
        # Handle accent-stripped forms (e.g. "fes" → "Fès")
        stripped = (
            std.lower()
            .replace("è", "e").replace("é", "e").replace("ê", "e")
            .replace("â", "a").replace("à", "a")
            .replace("ô", "o").replace("î", "i")
            .replace("ï", "i").replace("ü", "u").replace("û", "u")
        )
        mapping[stripped] = std

    # Manually add known aliases not derivable from the reference file
    MANUAL_ALIASES = {
        "tnja":       "Tanger",
        "tng":        "Tanger",
        "casa":       "Casablanca",
        "cas":        "Casablanca",
        "rbat":       "Rabat",
        "mrk":        "Marrakech",
        "marrakesh":  "Marrakech",
        "fez":        "Fès",
        "meknes":     "Meknès",
        "tetouan":    "Tétouan",
        "kenitra":    "Kénitra",
        "sale":       "Salé",
        "oujda":      "Oujda",
        "nador":      "Nador",
        "safi":       "Safi",
        "larache":    "Larache",
        "beni mellal":"Béni Mellal",
        "el jadida":  "El Jadida",
        "agadir":     "Agadir",
    }
    mapping.update(MANUAL_ALIASES)
    return mapping


def transform_commandes(
    df: pd.DataFrame,
    regions_filepath: str = "data/regions_maroc.csv",
) -> pd.DataFrame:
    """
    Apply all 7 cleaning rules to raw orders.

    Args:
        df              : Raw orders DataFrame (all columns as str).
        regions_filepath: Path to regions_maroc.csv reference file.

    Returns:
        Cleaned pd.DataFrame.
    """
    initial = len(df)
    logger.info(f"[TRANSFORM] commandes : début avec {initial:,} lignes")

    # ── R1 — Remove duplicates on id_commande (keep last occurrence) ──────
    before = len(df)
    df = df.drop_duplicates(subset=["id_commande"], keep="last")
    removed = before - len(df)
    logger.info(f"[TRANSFORM] R1 doublons      : {removed:,} lignes supprimées "
                f"({removed/before*100:.1f}%)")

    # ── R2 — Standardise date formats ─────────────────────────────────────
    # pd.to_datetime with format='mixed' handles: YYYY-MM-DD, MM/DD/YYYY, Mon DD YYYY
    for col in ["date_commande", "date_livraison"]:
        before_nulls = df[col].isna().sum() + (df[col].str.strip() == "").sum()
        df[col] = pd.to_datetime(
            df[col].replace("", pd.NA),
            format="mixed",
            dayfirst=False,   # MM/DD/YYYY is US format, not European
            errors="coerce",
        )
        after_nulls  = df[col].isna().sum()
        new_invalids = after_nulls - before_nulls
        logger.info(f"[TRANSFORM] R2 dates ({col}) : {new_invalids} dates invalides → NaT")

    # Drop rows where order date could not be parsed (can't place them in time dim)
    before = len(df)
    df = df.dropna(subset=["date_commande"])
    logger.info(f"[TRANSFORM] R2 dates supprimées : {before - len(df)} lignes (date_commande invalide)")

    # ── R3 — Harmonise city names ──────────────────────────────────────────
    mapping_villes = charger_referentiel_villes(regions_filepath)
    df["ville_livraison"] = df["ville_livraison"].str.strip().str.lower()
    df["ville_livraison_clean"] = df["ville_livraison"].map(mapping_villes)
    non_mapped = df["ville_livraison_clean"].isna().sum()
    df["ville_livraison"] = df["ville_livraison_clean"].fillna("Non renseignée")
    df = df.drop(columns=["ville_livraison_clean"])
    logger.info(f"[TRANSFORM] R3 villes        : {non_mapped} villes non trouvées → 'Non renseignée'")

    # ── R4 — Standardise order statuses ───────────────────────────────────
    df["statut"] = df["statut"].str.strip()
    df["statut"] = df["statut"].replace(STATUS_MAPPING)
    invalides_mask = ~df["statut"].isin(VALID_STATUSES)
    n_invalides = invalides_mask.sum()
    if n_invalides > 0:
        logger.warning(f"[TRANSFORM] R4 statuts       : {n_invalides} valeurs non reconnues → 'inconnu'")
        df.loc[invalides_mask, "statut"] = "inconnu"

    # ── R5 — Remove negative / zero quantities ────────────────────────────
    before = len(df)
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
    df = df[df["quantite"].fillna(0) > 0]
    logger.info(f"[TRANSFORM] R5 quantités     : {before - len(df)} lignes supprimées (quantité ≤ 0)")

    # ── R6 — Remove test orders (unit price = 0) ──────────────────────────
    before = len(df)
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
    df = df[df["prix_unitaire"].fillna(0) > 0]
    logger.info(f"[TRANSFORM] R6 prix          : {before - len(df)} commandes test supprimées (prix = 0)")

    # ── R7 — Fill missing delivery persons ────────────────────────────────
    nb_manquants = (df["id_livreur"].isna() | (df["id_livreur"].str.strip() == "")).sum()
    df["id_livreur"] = df["id_livreur"].replace("", "-1").fillna("-1")
    logger.info(f"[TRANSFORM] R7 livreurs      : {nb_manquants} valeurs manquantes → '-1'")

    # ── Derived measure: montant_ttc ──────────────────────────────────────
    from config.settings import TVA_RATE
    df["montant_ht"]  = (df["quantite"] * df["prix_unitaire"]).round(2)
    df["montant_ttc"] = (df["montant_ht"] * (1 + TVA_RATE)).round(2)

    total_removed = initial - len(df)
    logger.info(
        f"[TRANSFORM] commandes : TERMINÉ  {initial:,} → {len(df):,} lignes "
        f"({total_removed:,} supprimées au total, {total_removed/initial*100:.1f}%)"
    )
    return df
