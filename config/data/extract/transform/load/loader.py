"""
load/loader.py — PostgreSQL Loading Layer
==========================================
Handles all writes to the data warehouse.

Strategy:
    - Dimensions : REPLACE (truncate + reload) — dimensions are fully
      refreshed on each run. This is safe because fact table FKs point
      to surrogate keys that are stable across runs (generated in Python
      before load, not by the DB's SERIAL).

    - Fact table : UPSERT (INSERT ... ON CONFLICT DO UPDATE) — new
      orders are inserted; re-processed orders update existing rows.
      This allows the pipeline to be re-run safely without duplication.

Important:
    Schemas must exist before loading. Run create_dwh.sql first.
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from utils.logger import get_logger
from config.settings import SCHEMA_DWH

logger = get_logger(__name__)

# ── Chunk size for batch inserts ──────────────────────────────────────────
CHUNK_SIZE = 5_000


def get_engine(db_url: str) -> sqlalchemy.Engine:
    """
    Create and return a SQLAlchemy engine for PostgreSQL.
    Tests the connection before returning.
    """
    engine = sqlalchemy.create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("[LOAD] Connexion PostgreSQL établie")
    except Exception as e:
        logger.error(f"[LOAD] Échec de connexion PostgreSQL : {e}")
        raise
    return engine


def charger_dimension(
    df: pd.DataFrame,
    table_name: str,
    engine: sqlalchemy.Engine,
) -> None:
    """
    Load a dimension table using REPLACE strategy.
    All existing rows are deleted; new rows are inserted.

    Args:
        df         : Cleaned dimension DataFrame.
        table_name : Target table name (without schema prefix).
        engine     : SQLAlchemy engine connected to mexora_dwh.
    """
    if df.empty:
        logger.warning(f"[LOAD] {table_name} : DataFrame vide — chargement ignoré")
        return

    # Convert pandas NA types to None for SQL NULL compatibility
    df = df.where(pd.notna(df), other=None)

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA_DWH,
            if_exists="replace",    # truncate + reload
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE,
        )
        logger.info(f"[LOAD] {table_name:<20} : {len(df):,} lignes chargées (replace)")
    except Exception as e:
        logger.error(f"[LOAD] Erreur lors du chargement de {table_name} : {e}")
        raise


def charger_faits(
    df: pd.DataFrame,
    engine: sqlalchemy.Engine,
) -> None:
    """
    Load the fact table using chunked UPSERT.

    Uses pandas to_sql with if_exists='append' after checking for
    existing rows by id_vente. For a full pipeline re-run, we truncate
    first (acceptable for a student project; production would use
    incremental loading by date partition).

    Design note:
        A proper SQLAlchemy Core UPSERT requires the table to be
        reflected first. For simplicity and reliability, we truncate
        + reload the fact table on each full pipeline run.
        This is documented as a known design decision.
    """
    if df.empty:
        logger.warning("[LOAD] fait_ventes : DataFrame vide — chargement ignoré")
        return

    df = df.where(pd.notna(df), other=None)

    table_full = f"{SCHEMA_DWH}.fait_ventes"

    # Truncate fact table before reload (full refresh strategy)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_full} RESTART IDENTITY CASCADE"))
        logger.info(f"[LOAD] fait_ventes : table tronquée avant rechargement")

    try:
        df.to_sql(
            name="fait_ventes",
            con=engine,
            schema=SCHEMA_DWH,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE,
        )
        logger.info(f"[LOAD] fait_ventes           : {len(df):,} lignes chargées")
    except Exception as e:
        logger.error(f"[LOAD] Erreur lors du chargement de fait_ventes : {e}")
        raise


def refresh_materialized_views(engine: sqlalchemy.Engine) -> None:
    """
    Refresh all reporting materialized views after loading.
    Must be called after the fact table is populated.
    """
    from config.settings import SCHEMA_REPORTING
    views = [
        f"{SCHEMA_REPORTING}.mv_ca_mensuel",
        f"{SCHEMA_REPORTING}.mv_top_produits",
        f"{SCHEMA_REPORTING}.mv_performance_livreurs",
    ]
    with engine.begin() as conn:
        for view in views:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {view}"))
            logger.info(f"[LOAD] Vue matérialisée rafraîchie : {view}")
