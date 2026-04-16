-- =============================================================================
-- create_dwh.sql — Mexora Data Warehouse Schema
-- =============================================================================
-- Run this script ONCE before executing the ETL pipeline.
-- It creates all schemas, dimension tables, the fact table, indexes,
-- and the 3 required materialized views.
--
-- Execution order matters:
--   1. Schemas
--   2. Dimension tables (no FK dependencies)
--   3. Fact table (depends on all dimensions)
--   4. Indexes
--   5. Materialized views (depend on fact + dimensions)
--
-- Compatible with: PostgreSQL 15+
-- Run as: psql -U postgres -d mexora_dwh -f create_dwh.sql
-- Or paste directly into DBeaver / pgAdmin query editor.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 0 — Schemas
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS staging_mexora;
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;


-- -----------------------------------------------------------------------------
-- STEP 1 — Drop existing tables (safe re-run)
-- Dropped in reverse dependency order to respect foreign keys.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS dwh_mexora.fait_ventes    CASCADE;
DROP TABLE IF EXISTS dwh_mexora.dim_client     CASCADE;
DROP TABLE IF EXISTS dwh_mexora.dim_produit    CASCADE;
DROP TABLE IF EXISTS dwh_mexora.dim_region     CASCADE;
DROP TABLE IF EXISTS dwh_mexora.dim_livreur    CASCADE;
DROP TABLE IF EXISTS dwh_mexora.dim_temps      CASCADE;

DROP MATERIALIZED VIEW IF EXISTS reporting_mexora.mv_ca_mensuel          CASCADE;
DROP MATERIALIZED VIEW IF EXISTS reporting_mexora.mv_top_produits        CASCADE;
DROP MATERIALIZED VIEW IF EXISTS reporting_mexora.mv_performance_livreurs CASCADE;


-- -----------------------------------------------------------------------------
-- STEP 2 — Dimension tables
-- -----------------------------------------------------------------------------

-- DIM_TEMPS
-- Primary key: id_date in YYYYMMDD integer format (e.g. 20240315)
-- Design choice: integer PK is faster for date range joins than DATE type.

CREATE TABLE dwh_mexora.dim_temps (
    id_date          INTEGER      PRIMARY KEY,   -- format YYYYMMDD
    jour             SMALLINT     NOT NULL CHECK (jour BETWEEN 1 AND 31),
    mois             SMALLINT     NOT NULL CHECK (mois BETWEEN 1 AND 12),
    trimestre        SMALLINT     NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee            SMALLINT     NOT NULL,
    semaine          SMALLINT,
    libelle_jour     VARCHAR(20),
    libelle_mois     VARCHAR(20),
    est_weekend      BOOLEAN      DEFAULT FALSE,
    est_ferie_maroc  BOOLEAN      DEFAULT FALSE,
    periode_ramadan  BOOLEAN      DEFAULT FALSE
);

COMMENT ON TABLE  dwh_mexora.dim_temps IS 'Dimension temporelle — un enregistrement par jour calendaire (2020-2025)';
COMMENT ON COLUMN dwh_mexora.dim_temps.id_date IS 'Clé primaire au format YYYYMMDD';
COMMENT ON COLUMN dwh_mexora.dim_temps.periode_ramadan IS 'TRUE si la date tombe dans la période Ramadan (dates approximatives)';


-- DIM_PRODUIT  (SCD Type 2)
-- Natural key: id_produit_nk  (source system ID)
-- Surrogate key: id_produit_sk (auto-increment, referenced by fact table)
-- SCD Type 2 columns: date_debut, date_fin, est_actif

CREATE TABLE dwh_mexora.dim_produit (
    id_produit_sk    SERIAL        PRIMARY KEY,
    id_produit_nk    VARCHAR(20)   NOT NULL,
    nom_produit      VARCHAR(200)  NOT NULL,
    categorie        VARCHAR(100),
    sous_categorie   VARCHAR(100),
    marque           VARCHAR(100),
    fournisseur      VARCHAR(100),
    prix_standard    NUMERIC(10,2),
    origine_pays     VARCHAR(50),
    -- SCD Type 2 validity window
    date_debut       DATE          NOT NULL DEFAULT CURRENT_DATE,
    date_fin         DATE          NOT NULL DEFAULT '2099-12-31',
    est_actif        BOOLEAN       NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE  dwh_mexora.dim_produit IS 'Dimension produit avec SCD Type 2 — historique des changements de catégorie/fournisseur';
COMMENT ON COLUMN dwh_mexora.dim_produit.id_produit_sk IS 'Clé de substitution (surrogate key) — utilisée par la table de faits';
COMMENT ON COLUMN dwh_mexora.dim_produit.id_produit_nk IS 'Clé naturelle issue du système source';
COMMENT ON COLUMN dwh_mexora.dim_produit.est_actif IS 'TRUE = enregistrement courant ; FALSE = version historique remplacée';


-- DIM_CLIENT  (SCD Type 1 for segment + city)
-- Design choice: segment is SCD Type 1 (overwrite) because the CEO's
-- questions ask about current segmentation, not historical.

CREATE TABLE dwh_mexora.dim_client (
    id_client_sk     SERIAL        PRIMARY KEY,
    id_client_nk     VARCHAR(20)   NOT NULL,
    nom_complet      VARCHAR(200),
    tranche_age      VARCHAR(10),
    sexe             CHAR(1)       CHECK (sexe IN ('m','f') OR sexe IS NULL),
    ville            VARCHAR(100),
    region_admin     VARCHAR(100),
    segment_client   VARCHAR(20)   CHECK (segment_client IN ('Gold','Silver','Bronze')),
    canal_acquisition VARCHAR(50),
    -- SCD Type 2 validity window (kept for future use)
    date_debut       DATE          NOT NULL DEFAULT CURRENT_DATE,
    date_fin         DATE          NOT NULL DEFAULT '2099-12-31',
    est_actif        BOOLEAN       NOT NULL DEFAULT TRUE
);

COMMENT ON TABLE  dwh_mexora.dim_client IS 'Dimension client — segmentation Gold/Silver/Bronze basée sur CA 12 mois';
COMMENT ON COLUMN dwh_mexora.dim_client.segment_client IS 'Gold ≥ 15 000 MAD | Silver ≥ 5 000 MAD | Bronze < 5 000 MAD (CA 12 derniers mois, commandes livrées)';


-- DIM_REGION
-- Clean reference from regions_maroc.csv — no SCD needed.

CREATE TABLE dwh_mexora.dim_region (
    id_region        SERIAL        PRIMARY KEY,
    ville            VARCHAR(100)  NOT NULL,
    province         VARCHAR(100),
    region_admin     VARCHAR(100),
    zone_geo         VARCHAR(50),
    pays             VARCHAR(50)   DEFAULT 'Maroc'
);

COMMENT ON TABLE dwh_mexora.dim_region IS 'Dimension géographique — référentiel officiel des villes du Maroc';


-- DIM_LIVREUR

CREATE TABLE dwh_mexora.dim_livreur (
    id_livreur       SERIAL        PRIMARY KEY,
    id_livreur_nk    VARCHAR(20),
    nom_livreur      VARCHAR(100),
    type_transport   VARCHAR(50),
    zone_couverture  VARCHAR(100)
);

COMMENT ON TABLE  dwh_mexora.dim_livreur IS 'Dimension livreur — inclut le livreur fictif id=-1 pour les valeurs manquantes';


-- -----------------------------------------------------------------------------
-- STEP 3 — Fact table
-- -----------------------------------------------------------------------------

-- FAIT_VENTES
-- Granularity: one row = one delivered order line (one product per order).
-- This is the finest grain available from the source system.
--
-- Measures:
--   Additive      : quantite_vendue, montant_ht, montant_ttc, cout_livraison
--   Semi-additive : delai_livraison_jours  (AVG meaningful, SUM is not)
--   Non-additive  : remise_pct  (must be recalculated, never summed)

CREATE TABLE dwh_mexora.fait_ventes (
    id_vente                BIGSERIAL    PRIMARY KEY,

    -- Foreign keys to dimensions
    id_date                 INTEGER      NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit              INTEGER      NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client               INTEGER      NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region               INTEGER      NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur              INTEGER               REFERENCES dwh_mexora.dim_livreur(id_livreur),

    -- Additive measures
    quantite_vendue         INTEGER      NOT NULL CHECK (quantite_vendue > 0),
    montant_ht              NUMERIC(12,2) NOT NULL,
    montant_ttc             NUMERIC(12,2) NOT NULL,
    cout_livraison          NUMERIC(8,2)  DEFAULT 0.00,

    -- Semi-additive measure
    delai_livraison_jours   SMALLINT,

    -- Non-additive measure
    remise_pct              NUMERIC(5,2)  DEFAULT 0.00,

    -- ETL metadata
    statut_commande         VARCHAR(20)   CHECK (statut_commande IN ('livré','annulé','en_cours','retourné','inconnu')),
    date_chargement         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  dwh_mexora.fait_ventes IS 'Table de faits des ventes — granularité : une ligne par ligne de commande';
COMMENT ON COLUMN dwh_mexora.fait_ventes.montant_ttc IS 'Montant TTC calculé : montant_ht * 1.20 (TVA 20% Maroc)';
COMMENT ON COLUMN dwh_mexora.fait_ventes.delai_livraison_jours IS 'Semi-additif : utiliser AVG, pas SUM. Nombre de jours entre date_commande et date_livraison';
COMMENT ON COLUMN dwh_mexora.fait_ventes.remise_pct IS 'Non-additif : ne jamais sommer. Recalculer sur la granularité souhaitée';


-- -----------------------------------------------------------------------------
-- STEP 4 — Indexes
-- -----------------------------------------------------------------------------

-- Foreign key indexes (required for analytical join performance)
CREATE INDEX idx_fv_date     ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX idx_fv_produit  ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX idx_fv_client   ON dwh_mexora.fait_ventes(id_client);
CREATE INDEX idx_fv_region   ON dwh_mexora.fait_ventes(id_region);
CREATE INDEX idx_fv_livreur  ON dwh_mexora.fait_ventes(id_livreur);

-- Composite indexes for the dashboard's most frequent query patterns
-- Pattern 1: Revenue by region and month (Q1, Q3, Q5 on dashboard)
CREATE INDEX idx_fv_date_region ON dwh_mexora.fait_ventes(id_date, id_region)
    INCLUDE (montant_ttc, quantite_vendue);

-- Pattern 2: Delivered orders only (most analytical queries filter on this)
CREATE INDEX idx_fv_statut_livre ON dwh_mexora.fait_ventes(statut_commande)
    WHERE statut_commande = 'livré';

-- Pattern 3: Product + date for top-product queries (Q2 on dashboard)
CREATE INDEX idx_fv_produit_date ON dwh_mexora.fait_ventes(id_produit, id_date)
    INCLUDE (quantite_vendue, montant_ttc);

-- Indexes on dimension lookup columns
CREATE INDEX idx_dp_nk      ON dwh_mexora.dim_produit(id_produit_nk);
CREATE INDEX idx_dp_actif   ON dwh_mexora.dim_produit(est_actif);
CREATE INDEX idx_dc_nk      ON dwh_mexora.dim_client(id_client_nk);
CREATE INDEX idx_dr_ville   ON dwh_mexora.dim_region(ville);


-- -----------------------------------------------------------------------------
-- STEP 5 — Materialized views (reporting layer)
-- -----------------------------------------------------------------------------
-- These views are populated by the ETL pipeline after loading.
-- To manually refresh: REFRESH MATERIALIZED VIEW reporting_mexora.<view_name>;

-- VIEW 1: Monthly revenue by region and product category
-- Used for: Q1 (revenue trends), Q5 (Ramadan effect)

CREATE MATERIALIZED VIEW reporting_mexora.mv_ca_mensuel AS
SELECT
    t.annee,
    t.mois,
    t.libelle_mois,
    t.trimestre,
    t.periode_ramadan,
    r.region_admin,
    r.zone_geo,
    r.ville,
    p.categorie,
    p.sous_categorie,
    SUM(f.montant_ttc)              AS ca_ttc,
    SUM(f.montant_ht)               AS ca_ht,
    COUNT(DISTINCT f.id_client)     AS nb_clients_actifs,
    SUM(f.quantite_vendue)          AS volume_vendu,
    ROUND(AVG(f.montant_ttc), 2)    AS panier_moyen,
    COUNT(DISTINCT f.id_vente)      AS nb_commandes
FROM  dwh_mexora.fait_ventes  f
JOIN  dwh_mexora.dim_temps    t ON f.id_date    = t.id_date
JOIN  dwh_mexora.dim_region   r ON f.id_region  = r.id_region
JOIN  dwh_mexora.dim_produit  p ON f.id_produit = p.id_produit_sk
WHERE f.statut_commande = 'livré'
GROUP BY
    t.annee, t.mois, t.libelle_mois, t.trimestre, t.periode_ramadan,
    r.region_admin, r.zone_geo, r.ville,
    p.categorie, p.sous_categorie
WITH DATA;

CREATE INDEX ON reporting_mexora.mv_ca_mensuel(annee, mois);
CREATE INDEX ON reporting_mexora.mv_ca_mensuel(region_admin);
CREATE INDEX ON reporting_mexora.mv_ca_mensuel(categorie);
CREATE INDEX ON reporting_mexora.mv_ca_mensuel(periode_ramadan);


-- VIEW 2: Top products per quarter with rank within category
-- Used for: Q2 (top products in Tangier per quarter)

CREATE MATERIALIZED VIEW reporting_mexora.mv_top_produits AS
SELECT
    t.annee,
    t.trimestre,
    r.ville,
    r.region_admin,
    p.nom_produit,
    p.categorie,
    p.sous_categorie,
    p.marque,
    SUM(f.quantite_vendue)              AS qte_totale,
    SUM(f.montant_ttc)                  AS ca_total,
    COUNT(DISTINCT f.id_client)         AS nb_clients_distincts,
    RANK() OVER (
        PARTITION BY t.annee, t.trimestre, r.ville, p.categorie
        ORDER BY SUM(f.montant_ttc) DESC
    )                                   AS rang_dans_categorie
FROM  dwh_mexora.fait_ventes  f
JOIN  dwh_mexora.dim_temps    t ON f.id_date    = t.id_date
JOIN  dwh_mexora.dim_produit  p ON f.id_produit = p.id_produit_sk
JOIN  dwh_mexora.dim_region   r ON f.id_region  = r.id_region
WHERE f.statut_commande = 'livré'
GROUP BY
    t.annee, t.trimestre,
    r.ville, r.region_admin,
    p.nom_produit, p.categorie, p.sous_categorie, p.marque
WITH DATA;

CREATE INDEX ON reporting_mexora.mv_top_produits(annee, trimestre);
CREATE INDEX ON reporting_mexora.mv_top_produits(ville);
CREATE INDEX ON reporting_mexora.mv_top_produits(categorie);


-- VIEW 3: Delivery driver performance — late delivery rate
-- Used for: operational monitoring, not directly in Step 4 dashboard
-- but required by the project brief.

CREATE MATERIALIZED VIEW reporting_mexora.mv_performance_livreurs AS
SELECT
    l.nom_livreur,
    l.id_livreur_nk,
    l.zone_couverture,
    l.type_transport,
    t.annee,
    t.mois,
    COUNT(*)                                                        AS nb_livraisons,
    ROUND(AVG(f.delai_livraison_jours), 2)                          AS delai_moyen_jours,
    COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3)             AS nb_livraisons_retard,
    ROUND(
        COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    )                                                               AS taux_retard_pct
FROM  dwh_mexora.fait_ventes  f
JOIN  dwh_mexora.dim_livreur  l ON f.id_livreur = l.id_livreur
JOIN  dwh_mexora.dim_temps    t ON f.id_date    = t.id_date
WHERE f.statut_commande IN ('livré', 'retourné')
  AND f.delai_livraison_jours IS NOT NULL
  AND l.nom_livreur != 'Livreur Inconnu'    -- exclude the surrogate unknown driver
GROUP BY
    l.nom_livreur, l.id_livreur_nk, l.zone_couverture, l.type_transport,
    t.annee, t.mois
WITH DATA;

CREATE INDEX ON reporting_mexora.mv_performance_livreurs(annee, mois);
CREATE INDEX ON reporting_mexora.mv_performance_livreurs(nom_livreur);


-- -----------------------------------------------------------------------------
-- DONE
-- -----------------------------------------------------------------------------
SELECT 'DWH Mexora créé avec succès — ' || NOW()::text AS status;
