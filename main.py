"""
main.py — Mexora ETL Pipeline Orchestration
=============================================
Entry point for the full ETL pipeline.

Run:
    python main.py

Environment:
    Requires a .env file at project root with DB credentials.
    See config/settings.py for the expected variables.

Pipeline phases:
    1. EXTRACT  — read raw files, no transformation
    2. TRANSFORM — clean data, build dimensions, build fact table
    3. LOAD     — write dimensions and facts to PostgreSQL
    4. REFRESH  — refresh materialized views

Exit codes:
    0 — success
    1 — pipeline failure (logged with full traceback)
"""

import sys
import logging
from datetime import datetime

from utils.logger import get_logger
from config.settings import (
    ORDERS_FILE, CLIENTS_FILE, PRODUCTS_FILE, REGIONS_FILE,
    DB_URL, DIM_TEMPS_START, DIM_TEMPS_END,
)

from extract.extractor import (
    extract_orders, extract_clients, extract_products, extract_regions,
)
from transform.clean_commandes import transform_commandes
from transform.clean_clients   import transform_clients
from transform.clean_produits  import transform_produits
from transform.build_dimensions import (
    build_dim_temps,
    build_dim_produit,
    build_dim_client,
    build_dim_region,
    build_dim_livreur,
    build_dim_statut, 
    build_fait_ventes,
)
from load.loader import get_engine, charger_dimension, charger_faits, refresh_materialized_views

logger = get_logger("main")

SEPARATOR = "=" * 70


def run_pipeline() -> None:
    """Execute the full ETL pipeline end to end."""
    start = datetime.now()

    logger.info(SEPARATOR)
    logger.info("  DÉMARRAGE PIPELINE ETL MEXORA")
    logger.info(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(SEPARATOR)

    # ──────────────────────────────────────────────────────────────────────
    # PHASE 1 — EXTRACT
    # ──────────────────────────────────────────────────────────────────────
    logger.info("--- PHASE 1 : EXTRACT ---")

    df_commandes_raw = extract_orders(ORDERS_FILE)
    df_clients_raw   = extract_clients(CLIENTS_FILE)
    df_produits_raw  = extract_products(PRODUCTS_FILE)
    df_regions       = extract_regions(REGIONS_FILE)

    # ──────────────────────────────────────────────────────────────────────
    # PHASE 2 — TRANSFORM
    # ──────────────────────────────────────────────────────────────────────
    logger.info("--- PHASE 2 : TRANSFORM ---")

    # 2a. Clean source tables
    df_commandes = transform_commandes(df_commandes_raw, regions_filepath=str(REGIONS_FILE))
    df_clients   = transform_clients(df_clients_raw)
    df_produits  = transform_produits(df_produits_raw)

    # 2b. Build dimension tables
    dim_statut   = build_dim_statut()
    dim_temps   = build_dim_temps(DIM_TEMPS_START, DIM_TEMPS_END)
    dim_produit = build_dim_produit(df_produits)
    dim_region  = build_dim_region(df_regions)
    dim_livreur = build_dim_livreur(df_commandes)
    dim_client  = build_dim_client(df_clients, df_commandes, df_regions)

    # 2c. Build fact table (requires all dimension surrogate key maps)
    fait_ventes = build_fait_ventes(
        df_commandes, dim_temps, dim_client, dim_produit, dim_region, dim_livreur
    )

    # ──────────────────────────────────────────────────────────────────────
    # PHASE 3 — LOAD
    # ──────────────────────────────────────────────────────────────────────
    logger.info("--- PHASE 3 : LOAD ---")

    engine = get_engine(DB_URL)

    # Load dimensions first (fact table has FK references)
    charger_dimension(dim_statut,  "dim_statut_commande", engine)
    charger_dimension(dim_temps,   "dim_temps",   engine)
    charger_dimension(dim_produit, "dim_produit", engine)
    charger_dimension(dim_region,  "dim_region",  engine)
    charger_dimension(dim_livreur, "dim_livreur", engine)
    charger_dimension(dim_client,  "dim_client",  engine)

    # Load fact table
    charger_faits(fait_ventes, engine)

    # ──────────────────────────────────────────────────────────────────────
    # PHASE 4 — REFRESH MATERIALIZED VIEWS
    # ──────────────────────────────────────────────────────────────────────
    logger.info("--- PHASE 4 : REFRESH VUES MATÉRIALISÉES ---")
    refresh_materialized_views(engine)

    # ──────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ──────────────────────────────────────────────────────────────────────
    duration = (datetime.now() - start).seconds
    logger.info(SEPARATOR)
    logger.info(f"  PIPELINE TERMINÉ EN {duration} secondes")
    logger.info(f"  Lignes dans fait_ventes : {len(fait_ventes):,}")
    logger.info(SEPARATOR)


if __name__ == "__main__":
    try:
        run_pipeline()
        sys.exit(0)
    except Exception as e:
        logger.error(f"ERREUR FATALE : {e}", exc_info=True)
        sys.exit(1)
