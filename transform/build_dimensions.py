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

FERIES_MAROC = {
    "2022-01-01","2022-01-11","2022-05-01","2022-07-30",
    "2022-08-14","2022-11-06","2022-11-18",
    "2023-01-01","2023-01-11","2023-05-01","2023-07-30",
    "2023-08-14","2023-11-06","2023-11-18",
    "2024-01-01","2024-01-11","2024-05-01","2024-07-30",
    "2024-08-14","2024-11-06","2024-11-18",
    "2025-01-01","2025-01-11","2025-05-01","2025-07-30",
    "2025-08-14","2025-11-06","2025-11-18",
}

RAMADAN_PERIODS = [
    ("2022-04-02", "2022-05-01"),
    ("2023-03-22", "2023-04-20"),
    ("2024-03-10", "2024-04-09"),
    ("2025-03-01", "2025-03-29"),
]


def build_dim_temps(date_debut: str, date_fin: str) -> pd.DataFrame:
    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")

    df = pd.DataFrame({
        "id_date":         dates.strftime("%Y%m%d").astype(int),
        "date_complete":   dates,
        "jour":            dates.day,
        "mois":            dates.month,
        "trimestre":       dates.quarter,
        "annee":           dates.year,
        "semaine":         dates.isocalendar().week.astype(int),
        "libelle_jour":    dates.strftime("%A"),
        "libelle_mois":    dates.strftime("%B"),
        "est_weekend":     dates.dayofweek >= 5,
        "est_ferie_maroc": dates.strftime("%Y-%m-%d").isin(FERIES_MAROC),
    })

    df["periode_ramadan"] = False
    for debut, fin in RAMADAN_PERIODS:
        mask = (df["date_complete"] >= debut) & (df["date_complete"] <= fin)
        df.loc[mask, "periode_ramadan"] = True

    df = df.drop(columns=["date_complete"])

    logger.info(f"[BUILD] dim_temps     : {len(df):,} jours ({date_debut} → {date_fin})")
    return df


def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    df = df_produits.copy()

    df = df.rename(columns={
        "id_produit":     "id_produit_nk",
        "nom":            "nom_produit",
        "prix_catalogue": "prix_standard",
        "origine_pays":   "origine_pays",
    })

    df["date_debut"] = pd.Timestamp("today").normalize()
    df["date_fin"]   = pd.Timestamp("2099-12-31")
    df["est_actif"]  = df["actif"].astype(bool)

    cols = [
        "id_produit_nk", "nom_produit", "categorie", "sous_categorie",
        "marque", "fournisseur", "prix_standard", "origine_pays",
        "date_debut", "date_fin", "est_actif",
    ]
    df = df[cols].reset_index(drop=True)
    df.insert(0, "id_produit_sk", range(1, len(df) + 1))

    logger.info(f"[BUILD] dim_produit   : {len(df):,} produits")
    return df


def build_dim_client(
    df_clients: pd.DataFrame,
    df_commandes: pd.DataFrame,
    df_regions: pd.DataFrame,
) -> pd.DataFrame:
    from transform.clean_clients import calculer_segments_clients

    df = df_clients.copy()

    city_map = _build_city_map(df_regions)
    df["ville"] = df["ville"].str.strip().str.lower().map(city_map).fillna("Non renseignée")

    region_map = (
        df_regions.set_index("nom_ville_standard")["region_admin"]
                  .to_dict()
    )
    df["region_admin"] = df["ville"].map(region_map).fillna("Non renseignée")

    # calculer_segments_clients adds 'segment_client' and 'ca_12m'
    df = calculer_segments_clients(df_commandes, df)

    df["nom_complet"] = (
        df["prenom"].str.title() + " " + df["nom"].str.title()
    ).str.strip()

    df["date_debut"] = pd.Timestamp("today").normalize()
    df["date_fin"]   = pd.Timestamp("2099-12-31")
    df["est_actif"]  = True

    # ca_12m is deliberately excluded here — it is not a column in the
    # PostgreSQL dim_client table. It is an internal calculation only.
    cols = [
        "id_client", "nom_complet", "tranche_age", "sexe", "ville",
        "region_admin", "segment_client", "canal_acquisition",
        "date_debut", "date_fin", "est_actif",
    ]
    df = df[cols].rename(columns={"id_client": "id_client_nk"})
    df = df.reset_index(drop=True)
    df.insert(0, "id_client_sk", range(1, len(df) + 1))

    # Placeholder row for orders whose client was lost in deduplication.
    # ca_12m is also NOT included here.
    unknown_row = pd.DataFrame([{
        "id_client_sk":      0,
        "id_client_nk":      "UNKNOWN",
        "nom_complet":       "Client Inconnu",
        "tranche_age":       "inconnu",
        "sexe":              None,
        "ville":             "Non renseignée",
        "region_admin":      "Non renseignée",
        "segment_client":    "Bronze",
        "canal_acquisition": None,
        "date_debut":        pd.Timestamp("2020-01-01"),
        "date_fin":          pd.Timestamp("2099-12-31"),
        "est_actif":         True,
    }])

    df = pd.concat([unknown_row, df], ignore_index=True)

    logger.info(f"[BUILD] dim_client    : {len(df):,} clients")
    return df


def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    df = df_regions.copy()
    df = df.rename(columns={
        "nom_ville_standard": "ville",
        "region_admin":       "region_admin",
        "zone_geo":           "zone_geo",
    })
    df["pays"] = "Maroc"
    cols = ["ville", "province", "region_admin", "zone_geo", "pays"]
    df = df[cols].reset_index(drop=True)
    df.insert(0, "id_region", range(1, len(df) + 1))

    logger.info(f"[BUILD] dim_region    : {len(df):,} villes")
    return df


def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    import random
    random.seed(0)

    TRANSPORT_TYPES = ["Moto", "Voiture", "Camionnette", "Vélo"]

    df = df_commandes[["id_livreur", "ville_livraison"]].copy()

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
    df = df_commandes.copy()

    df["id_date"] = df["date_commande"].dt.strftime("%Y%m%d").astype(int)
    valid_dates   = set(dim_temps["id_date"])
    before        = len(df)
    df = df[df["id_date"].isin(valid_dates)]
    if before > len(df):
        logger.warning(
            f"[BUILD] fait_ventes : {before - len(df)} lignes "
            f"hors plage temporelle supprimées"
        )

    prod_map         = dim_produit.set_index("id_produit_nk")["id_produit_sk"].to_dict()
    df["id_produit"] = df["id_produit"].map(prod_map)
    n_missing_prod   = df["id_produit"].isna().sum()
    if n_missing_prod:
        logger.warning(f"[BUILD] fait_ventes : {n_missing_prod} produits inconnus → supprimés")
        df = df.dropna(subset=["id_produit"])
    df["id_produit"] = df["id_produit"].astype(int)

    client_map       = dim_client.set_index("id_client_nk")["id_client_sk"].to_dict()
    df["id_client"]  = df["id_client"].map(client_map)
    n_missing_client = df["id_client"].isna().sum()
    if n_missing_client:
        logger.warning(
            f"[BUILD] fait_ventes : {n_missing_client} clients inconnus "
            f"→ assignés au client générique (sk=0)"
        )
        df["id_client"] = df["id_client"].fillna(0)
    df["id_client"] = df["id_client"].astype(int)

    region_map      = dim_region.set_index("ville")["id_region"].to_dict()
    df["id_region"] = df["ville_livraison"].map(region_map)
    fallback_region = (
        dim_region[dim_region["ville"] == "Non renseignée"]["id_region"].values[0]
        if "Non renseignée" in dim_region["ville"].values
        else 1
    )
    df["id_region"] = df["id_region"].fillna(fallback_region).astype(int)

    livreur_map         = dim_livreur.set_index("id_livreur_nk")["id_livreur"].to_dict()
    df["id_livreur_fk"] = df["id_livreur"].map(livreur_map).astype("Int64")

    df["delai_livraison_jours"] = (
        (df["date_livraison"] - df["date_commande"]).dt.days
        .clip(lower=0)
        .astype("Int64")
    )

    fait = pd.DataFrame({
        "id_date":               df["id_date"],
        "id_produit":            df["id_produit"],
        "id_client":             df["id_client"],
        "id_region":             df["id_region"],
        "id_livreur":            df["id_livreur_fk"],
        "quantite_vendue":       df["quantite"].astype(int),
        "montant_ht":            df["montant_ht"].round(2),
        "montant_ttc":           df["montant_ttc"].round(2),
        "cout_livraison":        0.00,
        "delai_livraison_jours": df["delai_livraison_jours"],
        "remise_pct":            0.00,
        "statut_commande":       df["statut"],
    }).reset_index(drop=True)

    logger.info(f"[BUILD] fait_ventes   : {len(fait):,} lignes prêtes pour chargement")
    return fait


def _build_city_map(df_regions: pd.DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for _, row in df_regions.iterrows():
        std  = row["nom_ville_standard"].strip()
        code = row["code_ville"].strip().lower()
        mapping[std.lower()]  = std
        mapping[std.upper()]  = std
        mapping[std]          = std
        mapping[code]         = std
        mapping[code.upper()] = std

    manual = {
        "tnja":        "Tanger",
        "tng":         "Tanger",
        "casa":        "Casablanca",
        "cas":         "Casablanca",
        "rbat":        "Rabat",
        "mrk":         "Marrakech",
        "marrakesh":   "Marrakech",
        "fez":         "Fès",
        "meknes":      "Meknès",
        "tetouan":     "Tétouan",
        "kenitra":     "Kénitra",
        "sale":        "Salé",
        "beni mellal": "Béni Mellal",
        "el jadida":   "El Jadida",
    }
    mapping.update(manual)
    return mapping