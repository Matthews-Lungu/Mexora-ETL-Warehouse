"""
transform/build_dimensions.py — Dimension Construction
=========================================================
Takes cleaned source DataFrames and builds the 5 star-schema
dimension tables ready for loading into dwh_mexora.

Functions:
    build_dim_temps(start, end)                         → DataFrame
    build_dim_produit(df_produits)                      → DataFrame
    build_dim_client(df_clients, df_commandes, regions) → DataFrame
    build_dim_region(df_regions)                        → DataFrame
    build_dim_livreur(df_commandes)                     → DataFrame
    build_fait_ventes(...)                              → DataFrame
"""

import pandas as pd
from utils.logger import get_logger
from config.settings import TVA_RATE

logger = get_logger(__name__)

# ── Moroccan public holidays (fixed) ─────────────────────────────────────
FERIES_MAROC = {
    # 2022
    "2022-01-01","2022-01-11","2022-05-01","2022-07-30",
    "2022-08-14","2022-11-06","2022-11-18",
    # 2023
    "2023-01-01","2023-01-11","2023-05-01","2023-07-30",
    "2023-08-14","2023-11-06","2023-11-18",
    # 2024
    "2024-01-01","2024-01-11","2024-05-01","2024-07-30",
    "2024-08-14","2024-11-06","2024-11-18",
    # 2025
    "2025-01-01","2025-01-11","2025-05-01","2025-07-30",
    "2025-08-14","2025-11-06","2025-11-18",
}

# Ramadan periods (approximate, adjusted annually)
RAMADAN_PERIODS = [
    ("2022-04-02", "2022-05-01"),
    ("2023-03-22", "2023-04-20"),
    ("2024-03-10", "2024-04-09"),
    ("2025-03-01", "2025-03-29"),
]


def build_dim_temps(date_debut: str, date_fin: str) -> pd.DataFrame:
    """
    Generate the complete time dimension between two dates.
    Each row represents one calendar day.

    Args:
        date_debut : Start date string "YYYY-MM-DD"
        date_fin   : End date string   "YYYY-MM-DD"

    Returns:
        pd.DataFrame with id_date (YYYYMMDD integer) as primary key.
    """
    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")

    df = pd.DataFrame({
        "id_date":        dates.strftime("%Y%m%d").astype(int),
        "date_complete":  dates,
        "jour":           dates.day,
        "mois":           dates.month,
        "trimestre":      dates.quarter,
        "annee":          dates.year,
        "semaine":        dates.isocalendar().week.astype(int),
        "libelle_jour":   dates.strftime("%A"),
        "libelle_mois":   dates.strftime("%B"),
        "est_weekend":    dates.dayofweek >= 5,
        "est_ferie_maroc":dates.strftime("%Y-%m-%d").isin(FERIES_MAROC),
    })

    # Mark Ramadan periods
    df["periode_ramadan"] = False
    for debut, fin in RAMADAN_PERIODS:
        mask = (df["date_complete"] >= debut) & (df["date_complete"] <= fin)
        df.loc[mask, "periode_ramadan"] = True

    # Remove helper column — not in the DWH schema
    df = df.drop(columns=["date_complete"])

    logger.info(f"[BUILD] dim_temps     : {len(df):,} jours ({date_debut} → {date_fin})")
    return df


def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Build DIM_PRODUIT with SCD Type 2 columns.

    SCD Type 2 design:
        - id_produit_sk : surrogate key (auto-generated here, SERIAL in DB)
        - id_produit_nk : natural key from the source system
        - date_debut / date_fin / est_actif : SCD Type 2 validity window
          All loaded rows start as current records (est_actif = True).
          Updates to category/supplier will create new rows in the DB.

    Returns:
        pd.DataFrame ready for loading.
    """
    df = df_produits.copy()

    df = df.rename(columns={
        "id_produit":    "id_produit_nk",
        "nom":           "nom_produit",
        "sous_categorie":"sous_categorie",
        "marque":        "marque",
        "fournisseur":   "fournisseur",
        "prix_catalogue":"prix_standard",
        "origine_pays":  "origine_pays",
    })

    # SCD Type 2 metadata
    df["date_debut"] = pd.Timestamp("today").normalize()
    df["date_fin"]   = pd.Timestamp("2099-12-31")
    df["est_actif"]  = df["actif"].astype(bool)

    cols = [
        "id_produit_nk","nom_produit","categorie","sous_categorie",
        "marque","fournisseur","prix_standard","origine_pays",
        "date_debut","date_fin","est_actif",
    ]
    df = df[cols].reset_index(drop=True)
    # Surrogate key — DB will use SERIAL; here we simulate it for FK building
    df.insert(0, "id_produit_sk", range(1, len(df) + 1))

    logger.info(f"[BUILD] dim_produit   : {len(df):,} produits")
    return df


def build_dim_client(
    df_clients: pd.DataFrame,
    df_commandes: pd.DataFrame,
    df_regions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build DIM_CLIENT with SCD Type 1 for city/region and
    calculated segment from order history.

    SCD design choice:
        - Segment (Gold/Silver/Bronze): SCD Type 1 — we overwrite because
          current value is what matters for analytics (no need to track
          historical segment changes for this project's scope).
        - City: SCD Type 1 — correction, not a meaningful historical change.

    Returns:
        pd.DataFrame ready for loading.
    """
    from transform.clean_clients import calculer_segments_clients

    df = df_clients.copy()

    # Harmonise city names using regions reference
    city_map = _build_city_map(df_regions)
    df["ville"] = df["ville"].str.strip().str.lower().map(city_map).fillna("Non renseignée")

    # Join region_admin from regions reference
    region_map = (
        df_regions.set_index("nom_ville_standard")["region_admin"]
                  .to_dict()
    )
    df["region_admin"] = df["ville"].map(region_map).fillna("Non renseignée")

    # Add segmentation
    df = calculer_segments_clients(df_commandes, df)

    # Full name
    df["nom_complet"] = (df["prenom"].str.title() + " " + df["nom"].str.title()).str.strip()

    # SCD Type 2 metadata (matches dim_produit pattern)
    df["date_debut"] = pd.Timestamp("today").normalize()
    df["date_fin"]   = pd.Timestamp("2099-12-31")
    df["est_actif"]  = True

    cols = [
        "id_client","nom_complet","tranche_age","sexe","ville",
        "region_admin","segment_client","canal_acquisition",
        "date_debut","date_fin","est_actif",
    ]
    df = df[cols].rename(columns={"id_client": "id_client_nk"})
    df = df.reset_index(drop=True)
    df.insert(0, "id_client_sk", range(1, len(df) + 1))

    # Add a placeholder row for orders whose client was lost in deduplication
    unknown_row = pd.DataFrame([{
        "id_client_sk":    0,
        "id_client_nk":   "UNKNOWN",
        "nom_complet":    "Client Inconnu",
        "tranche_age":    "inconnu",
        "sexe":           None,
        "ville":          "Non renseignée",
        "region_admin":   "Non renseignée",
        "segment_client": "Bronze",
        "canal_acquisition": None,
        "date_debut":     pd.Timestamp("2020-01-01"),
        "date_fin":       pd.Timestamp("2099-12-31"),
        "est_actif":      True,
        "ca_12m":         0.0,
    }])
    df = pd.concat([unknown_row, df], ignore_index=True)

    logger.info(f"[BUILD] dim_client    : {len(df):,} clients")
    return df


def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Build DIM_REGION directly from the clean reference file.

    Returns:
        pd.DataFrame ready for loading.
    """
    df = df_regions.copy()
    df = df.rename(columns={
        "nom_ville_standard":"ville",
        "region_admin":      "region_admin",
        "zone_geo":          "zone_geo",
    })
    df["pays"] = "Maroc"
    cols = ["ville","province","region_admin","zone_geo","pays"]
    df = df[cols].reset_index(drop=True)
    df.insert(0, "id_region", range(1, len(df) + 1))

    logger.info(f"[BUILD] dim_region    : {len(df):,} villes")
    return df


def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Build DIM_LIVREUR from distinct delivery person IDs found in orders.
    Since the source data has no livreur master file, we derive names
    and attributes from the order data.

    Design assumption:
        - Livreur ID '-1' represents 'Unknown' — special case.
        - Zone coverage is inferred from most frequent delivery city.
        - Transport type is randomly assigned (no source for this).

    Returns:
        pd.DataFrame ready for loading.
    """
    import random
    random.seed(0)

    TRANSPORT_TYPES = ["Moto", "Voiture", "Camionnette", "Vélo"]

    df = df_commandes[["id_livreur", "ville_livraison"]].copy()

    # Most frequent city per livreur
    city_mode = (
        df[df["id_livreur"] != "-1"]
        .groupby("id_livreur")["ville_livraison"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "Inconnue")
    )

    livreurs = df["id_livreur"].unique()
    records = []
    for nk in sorted(livreurs):
        if nk == "-1":
            records.append({
                "id_livreur_nk":  "-1",
                "nom_livreur":    "Livreur Inconnu",
                "type_transport": "Inconnu",
                "zone_couverture":"Inconnue",
            })
        else:
            records.append({
                "id_livreur_nk":  nk,
                "nom_livreur":    f"Livreur {nk}",
                "type_transport": random.choice(TRANSPORT_TYPES),
                "zone_couverture":city_mode.get(nk, "Nationale"),
            })

    df_livreur = pd.DataFrame(records).reset_index(drop=True)
    df_livreur.insert(0, "id_livreur", range(1, len(df_livreur) + 1))

    logger.info(f"[BUILD] dim_livreur   : {len(df_livreur):,} livreurs (dont 1 inconnu)")
    return df_livreur


def build_fait_ventes(
    df_commandes: pd.DataFrame,
    dim_temps:    pd.DataFrame,
    dim_client:   pd.DataFrame,
    dim_produit:  pd.DataFrame,
    dim_region:   pd.DataFrame,
    dim_livreur:  pd.DataFrame,
) -> pd.DataFrame:
    """
    Build FAIT_VENTES by joining cleaned orders to dimension surrogate keys.

    Granularity: one row = one order line (one product per order).
    This is the finest grain available from the source — justified because
    the CEO's questions require product-level analysis.

    Measures:
        quantite_vendue   — additive
        montant_ht        — additive
        montant_ttc       — additive
        cout_livraison    — additive (set to 0 — not in source data)
        delai_livraison   — semi-additive (average meaningful, sum not)
        remise_pct        — non-additive (must be recalculated)

    Returns:
        pd.DataFrame ready for UPSERT loading.
    """
    df = df_commandes.copy()

    # ── Join id_date (YYYYMMDD integer) ───────────────────────────────────
    df["id_date"] = df["date_commande"].dt.strftime("%Y%m%d").astype(int)
    valid_dates = set(dim_temps["id_date"])
    before = len(df)
    df = df[df["id_date"].isin(valid_dates)]
    if before > len(df):
        logger.warning(f"[BUILD] fait_ventes : {before - len(df)} lignes hors plage temporelle supprimées")

    # ── Join id_produit_sk ────────────────────────────────────────────────
    prod_map = dim_produit.set_index("id_produit_nk")["id_produit_sk"].to_dict()
    df["id_produit"] = df["id_produit"].map(prod_map)
    n_missing_prod = df["id_produit"].isna().sum()
    if n_missing_prod:
        logger.warning(f"[BUILD] fait_ventes : {n_missing_prod} produits inconnus → lignes supprimées")
        df = df.dropna(subset=["id_produit"])
    df["id_produit"] = df["id_produit"].astype(int)

    # ── Join id_client_sk ─────────────────────────────────────────────────
    client_map = dim_client.set_index("id_client_nk")["id_client_sk"].to_dict()
    df["id_client"] = df["id_client"].map(client_map)
    n_missing_client = df["id_client"].isna().sum()
    if n_missing_client:
        # Design decision: orders from clients lost in deduplication
        # are assigned to a special "Client Inconnu" record (sk=0)
        # rather than being dropped, to preserve revenue figures.
        logger.warning(
            f"[BUILD] fait_ventes : {n_missing_client} clients inconnus "
            f"→ assignés au client générique (sk=0)"
        )
        df["id_client"] = df["id_client"].fillna(0)
    df["id_client"] = df["id_client"].astype(int)

    # ── Join id_region ────────────────────────────────────────────────────
    region_map = dim_region.set_index("ville")["id_region"].to_dict()
    df["id_region"] = df["ville_livraison"].map(region_map)
    df["id_region"] = df["id_region"].fillna(
        dim_region[dim_region["ville"] == "Non renseignée"]["id_region"].values[0]
        if "Non renseignée" in dim_region["ville"].values
        else 1
    ).astype(int)

    # ── Join id_livreur ───────────────────────────────────────────────────
    livreur_map = dim_livreur.set_index("id_livreur_nk")["id_livreur"].to_dict()
    df["id_livreur_fk"] = df["id_livreur"].map(livreur_map).astype("Int64")

    # ── Compute delivery delay ────────────────────────────────────────────
    df["delai_livraison_jours"] = (
        (df["date_livraison"] - df["date_commande"]).dt.days
        .clip(lower=0)  # no negative delays
        .astype("Int64")
    )

    # ── Assemble fact table ───────────────────────────────────────────────
    fait = pd.DataFrame({
        "id_date":              df["id_date"],
        "id_produit":           df["id_produit"],
        "id_client":            df["id_client"],
        "id_region":            df["id_region"],
        "id_livreur":           df["id_livreur_fk"],
        "quantite_vendue":      df["quantite"].astype(int),
        "montant_ht":           df["montant_ht"].round(2),
        "montant_ttc":          df["montant_ttc"].round(2),
        "cout_livraison":       0.00,   # not available in source data
        "delai_livraison_jours":df["delai_livraison_jours"],
        "remise_pct":           0.00,   # not available in source data
        "statut_commande":      df["statut"],
    }).reset_index(drop=True)

    logger.info(f"[BUILD] fait_ventes   : {len(fait):,} lignes prêtes pour chargement")
    return fait


# ── Private helper ────────────────────────────────────────────────────────

def _build_city_map(df_regions: pd.DataFrame) -> dict[str, str]:
    """Build lowercase → standard city name mapping from regions reference."""
    mapping: dict[str, str] = {}
    for _, row in df_regions.iterrows():
        std = row["nom_ville_standard"].strip()
        mapping[std.lower()]  = std
        mapping[std.upper()]  = std
        mapping[std]          = std
        code = row["code_ville"].strip().lower()
        mapping[code]         = std
        mapping[code.upper()] = std
    # Manual aliases
    manual = {
        "tnja":"Tanger","tng":"Tanger","casa":"Casablanca","cas":"Casablanca",
        "rbat":"Rabat","mrk":"Marrakech","marrakesh":"Marrakech","fez":"Fès",
        "meknes":"Meknès","tetouan":"Tétouan","kenitra":"Kénitra",
        "sale":"Salé","beni mellal":"Béni Mellal","el jadida":"El Jadida",
    }
    mapping.update(manual)
    return mapping
