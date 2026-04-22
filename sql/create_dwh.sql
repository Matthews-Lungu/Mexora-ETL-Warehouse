-- =============================================================================
-- create_dwh.sql — Mexora Data Warehouse Implementation
-- =============================================================================

-- 1. SCHEMAS
CREATE SCHEMA IF NOT EXISTS staging_mexora;
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;

-- 2. TABLES
-- Dimension Temps
CREATE TABLE dwh_mexora.dim_temps (
    id_date         INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    jour            SMALLINT NOT NULL CHECK (jour BETWEEN 1 AND 31),
    mois            SMALLINT NOT NULL CHECK (mois BETWEEN 1 AND 12),
    trimestre       SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee           SMALLINT NOT NULL,
    semaine         SMALLINT,
    libelle_jour    VARCHAR(20),
    libelle_mois    VARCHAR(20),
    est_weekend      BOOLEAN DEFAULT FALSE,
    est_ferie_maroc  BOOLEAN DEFAULT FALSE,
    periode_ramadan  BOOLEAN DEFAULT FALSE
);

-- Dimension Produit (SCD Type 2)
CREATE TABLE dwh_mexora.dim_produit (
    id_produit_sk     SERIAL PRIMARY KEY,         -- Surrogate key
    id_produit_nk     VARCHAR(20) NOT NULL,       -- Natural key from source
    nom_produit       VARCHAR(200) NOT NULL,
    categorie         VARCHAR(100),
    sous_categorie    VARCHAR(100),
    marque            VARCHAR(100),
    fournisseur       VARCHAR(100),
    prix_standard     DECIMAL(10,2),
    origine_pays      VARCHAR(50),
    -- SCD Type 2 Management
    date_debut        DATE NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension Client (SCD Type 2)
CREATE TABLE dwh_mexora.dim_client (
    id_client_sk      SERIAL PRIMARY KEY,
    id_client_nk      VARCHAR(20) NOT NULL,
    nom_complet       VARCHAR(200),
    tranche_age       VARCHAR(10),
    sexe              CHAR(1),
    ville             VARCHAR(100),
    region_admin      VARCHAR(100),
    segment_client    VARCHAR(20) CHECK (segment_client IN ('Gold', 'Silver', 'Bronze')),
    canal_acquisition VARCHAR(50),
    -- SCD Type 2 Management
    date_debut        DATE NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension Région
CREATE TABLE dwh_mexora.dim_region (
    id_region         SERIAL PRIMARY KEY,
    ville             VARCHAR(100) NOT NULL,
    province          VARCHAR(100),
    region_admin      VARCHAR(100),
    zone_geo          VARCHAR(50),
    pays              VARCHAR(50) DEFAULT 'Maroc'
);

-- Dimension Livreur
CREATE TABLE dwh_mexora.dim_livreur (
    id_livreur        SERIAL PRIMARY KEY,
    id_livreur_nk     VARCHAR(20),
    nom_livreur       VARCHAR(100),
    type_transport    VARCHAR(50),
    zone_couverture   VARCHAR(100)
);

-- DIMENSION STATUT COMMANDE
CREATE TABLE dwh_mexora.dim_statut_commande (
    id_statut     SERIAL PRIMARY KEY,
    code_statut   VARCHAR(20)  NOT NULL UNIQUE,
    libelle       VARCHAR(100) NOT NULL,
    categorie     VARCHAR(50)  NOT NULL,
    est_terminal  BOOLEAN      NOT NULL DEFAULT FALSE,
    est_positif   BOOLEAN      NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE dwh_mexora.dim_statut_commande IS 
'Dimension conformed — statuts possibles d''une commande Mexora';
-- Table de Faits
CREATE TABLE dwh_mexora.fait_ventes (
    id_vente              BIGSERIAL PRIMARY KEY,
    id_date               INTEGER NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit            INTEGER NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client             INTEGER NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region             INTEGER NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur            INTEGER REFERENCES dwh_mexora.dim_livreur(id_livreur),
    -- Measures
    quantite_vendue       INTEGER NOT NULL CHECK (quantite_vendue > 0),
    montant_ht            DECIMAL(12,2) NOT NULL,
    montant_ttc           DECIMAL(12,2) NOT NULL,
    cout_livraison        DECIMAL(8,2),
    delai_livraison_jours SMALLINT,
    remise_pct            DECIMAL(5,2) DEFAULT 0,
    -- Metadata
    date_chargement       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_statut           INTEGER      REFERENCES dwh_mexora.dim_statut_commande(id_statut));

-- 3. INDEXING
CREATE INDEX idx_fv_date    ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX idx_fv_produit ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX idx_fv_client  ON dwh_mexora.fait_ventes(id_client);
CREATE INDEX idx_fv_region  ON dwh_mexora.fait_ventes(id_region);
CREATE INDEX idx_fv_livreur ON dwh_mexora.fait_ventes(id_livreur);

-- Composite Index for Analytics
CREATE INDEX idx_fv_date_region ON dwh_mexora.fait_ventes(id_date, id_region) 
    INCLUDE (montant_ttc, quantite_vendue);

CREATE INDEX idx_fv_statut ON dwh_mexora.fait_ventes(id_statut);

-- 4. MATERIALIZED VIEWS
-- View 1: Monthly Revenue
CREATE MATERIALIZED VIEW reporting_mexora.mv_ca_mensuel AS
SELECT
    t.annee, t.mois, t.libelle_mois, t.periode_ramadan,
    r.region_admin, r.zone_geo, p.categorie,
    SUM(f.montant_ttc)           AS ca_ttc,
    SUM(f.montant_ht)            AS ca_ht,
    COUNT(DISTINCT f.id_client)  AS nb_clients_actifs,
    SUM(f.quantite_vendue)       AS volume_vendu,
    AVG(f.montant_ttc)           AS panier_moyen,
    COUNT(DISTINCT f.id_vente)   AS nb_commandes
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_region  r ON f.id_region  = r.id_region
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
JOIN dwh_mexora.dim_statut_commande s ON f.id_statut = s.id_statut
WHERE s.code_statut = 'livré'
GROUP BY t.annee, t.mois, t.libelle_mois, t.periode_ramadan, r.region_admin, r.zone_geo, p.categorie
WITH DATA;

-- View 2: Top Products
CREATE MATERIALIZED VIEW reporting_mexora.mv_top_produits AS
SELECT
    t.annee, t.trimestre, p.nom_produit, p.categorie, p.marque,
    SUM(f.quantite_vendue)      AS qte_totale,
    SUM(f.montant_ttc)          AS ca_total,
    RANK() OVER (PARTITION BY t.annee, t.trimestre, p.categorie ORDER BY SUM(f.montant_ttc) DESC) AS rang_dans_categorie
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
JOIN dwh_mexora.dim_statut_commande s ON f.id_statut = s.id_statut
WHERE s.code_statut = 'livré'
GROUP BY t.annee, t.trimestre, p.nom_produit, p.categorie, p.marque
WITH DATA;

-- View 3: Driver Performance
CREATE MATERIALIZED VIEW reporting_mexora.mv_performance_livreurs AS
SELECT
    l.nom_livreur, l.zone_couverture, t.annee, t.mois,
    COUNT(*)                                     AS nb_livraisons,
    AVG(f.delai_livraison_jours)                 AS delai_moyen_jours,
    COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3) AS nb_livraisons_retard,
    ROUND(COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3) * 100.0 / NULLIF(COUNT(*), 0), 2) AS taux_retard_pct
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_livreur l ON f.id_livreur = l.id_livreur
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_statut_commande s ON f.id_statut = s.id_statut
WHERE s.code_statut IN ('livré', 'retourné') AND f.delai_livraison_jours IS NOT NULL
GROUP BY l.nom_livreur, l.zone_couverture, t.annee, t.mois
WITH DATA;