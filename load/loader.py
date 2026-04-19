"""
load/loader.py — PostgreSQL Loading Layer
==========================================
Strategy:
    - Dimensions : TRUNCATE RESTART IDENTITY CASCADE then INSERT.
      Avoids DROP TABLE which fails when FK constraints or materialized
      views depend on the table.
    - Fact table : TRUNCATE then bulk INSERT in chunks.
    - Materialized views : REFRESH after all tables are loaded.
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from utils.logger import get_logger
from config.settings import SCHEMA_DWH

logger = get_logger(__name__)

CHUNK_SIZE = 5_000


def get_engine(db_url: str) -> sqlalchemy.Engine:
    """Create and return a tested SQLAlchemy engine for PostgreSQL."""
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
    Load a dimension table safely using TRUNCATE + INSERT.

    Uses TRUNCATE ... RESTART IDENTITY CASCADE instead of DROP so that
    FK constraints and materialized views are preserved.
    """
    if df.empty:
        logger.warning(f"[LOAD] {table_name} : DataFrame vide — ignoré")
        return

    df = df.where(pd.notna(df), other=None)
    full_table = f"{SCHEMA_DWH}.{table_name}"

    try:
        with engine.begin() as conn:
            conn.execute(text(
                f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE"
            ))
        logger.info(f"[LOAD] {table_name:<20} : table vidée (TRUNCATE CASCADE)")

        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA_DWH,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE,
        )
        logger.info(f"[LOAD] {table_name:<20} : {len(df):,} lignes chargées")

    except Exception as e:
        logger.error(f"[LOAD] Erreur chargement {table_name} : {e}")
        raise


def charger_faits(
    df: pd.DataFrame,
    engine: sqlalchemy.Engine,
) -> None:
    """Load the fact table using TRUNCATE + bulk INSERT."""
    if df.empty:
        logger.warning("[LOAD] fait_ventes : DataFrame vide — ignoré")
        return

    df = df.where(pd.notna(df), other=None)

    try:
        with engine.begin() as conn:
            conn.execute(text(
                f"TRUNCATE TABLE {SCHEMA_DWH}.fait_ventes RESTART IDENTITY"
            ))
        logger.info("[LOAD] fait_ventes           : table vidée (TRUNCATE)")

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
        logger.error(f"[LOAD] Erreur chargement fait_ventes : {e}")
        raise


def refresh_materialized_views(engine: sqlalchemy.Engine) -> None:
    """Refresh all reporting materialized views after loading."""
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