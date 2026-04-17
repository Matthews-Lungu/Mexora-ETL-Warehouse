-- =============================================================================
-- check_integrity.sql — Mexora DWH Referential Integrity Checks
-- =============================================================================
-- Run this script AFTER the ETL pipeline has loaded all tables.
-- Every query should return 0 rows. Any rows returned indicate a problem.
--
-- Run as: psql -U postgres -d mexora_dwh -f check_integrity.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CHECK 1 — Orphan fact rows: id_date not in dim_temps
-- -----------------------------------------------------------------------------
SELECT 'CHECK 1 — Orphan id_date' AS check_name,
       COUNT(*)                   AS nb_problemes
FROM   dwh_mexora.fait_ventes f
WHERE  NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_temps t WHERE t.id_date = f.id_date
);

-- -----------------------------------------------------------------------------
-- CHECK 2 — Orphan fact rows: id_produit not in dim_produit
-- -----------------------------------------------------------------------------
SELECT 'CHECK 2 — Orphan id_produit' AS check_name,
       COUNT(*)                      AS nb_problemes
FROM   dwh_mexora.fait_ventes f
WHERE  NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_produit p WHERE p.id_produit_sk = f.id_produit
);

-- -----------------------------------------------------------------------------
-- CHECK 3 — Orphan fact rows: id_client not in dim_client
-- -----------------------------------------------------------------------------
SELECT 'CHECK 3 — Orphan id_client' AS check_name,
       COUNT(*)                     AS nb_problemes
FROM   dwh_mexora.fait_ventes f
WHERE  NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_client c WHERE c.id_client_sk = f.id_client
);

-- -----------------------------------------------------------------------------
-- CHECK 4 — Orphan fact rows: id_region not in dim_region
-- -----------------------------------------------------------------------------
SELECT 'CHECK 4 — Orphan id_region' AS check_name,
       COUNT(*)                     AS nb_problemes
FROM   dwh_mexora.fait_ventes f
WHERE  NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_region r WHERE r.id_region = f.id_region
);

-- -----------------------------------------------------------------------------
-- CHECK 5 — Invalid quantities (should be 0 after ETL cleaning)
-- -----------------------------------------------------------------------------
SELECT 'CHECK 5 — Quantités invalides' AS check_name,
       COUNT(*)                        AS nb_problemes
FROM   dwh_mexora.fait_ventes
WHERE  quantite_vendue <= 0;

-- -----------------------------------------------------------------------------
-- CHECK 6 — Invalid amounts (negative montant_ttc)
-- -----------------------------------------------------------------------------
SELECT 'CHECK 6 — Montants négatifs' AS check_name,
       COUNT(*)                      AS nb_problemes
FROM   dwh_mexora.fait_ventes
WHERE  montant_ttc <= 0 OR montant_ht <= 0;

-- -----------------------------------------------------------------------------
-- CHECK 7 — Invalid statuses
-- -----------------------------------------------------------------------------
SELECT 'CHECK 7 — Statuts invalides' AS check_name,
       COUNT(*)                      AS nb_problemes
FROM   dwh_mexora.fait_ventes
WHERE  statut_commande NOT IN ('livré','annulé','en_cours','retourné','inconnu');

-- -----------------------------------------------------------------------------
-- CHECK 8 — Duplicate natural keys in dim_produit (active records only)
-- One active record per natural key at any point in time
-- -----------------------------------------------------------------------------
SELECT 'CHECK 8 — Doublons dim_produit (actif)' AS check_name,
       COUNT(*)                                  AS nb_problemes
FROM (
    SELECT id_produit_nk, COUNT(*) AS cnt
    FROM   dwh_mexora.dim_produit
    WHERE  est_actif = TRUE
    GROUP  BY id_produit_nk
    HAVING COUNT(*) > 1
) sub;

-- -----------------------------------------------------------------------------
-- CHECK 9 — Duplicate natural keys in dim_client (active records only)
-- -----------------------------------------------------------------------------
SELECT 'CHECK 9 — Doublons dim_client (actif)' AS check_name,
       COUNT(*)                                 AS nb_problemes
FROM (
    SELECT id_client_nk, COUNT(*) AS cnt
    FROM   dwh_mexora.dim_client
    WHERE  est_actif = TRUE
    GROUP  BY id_client_nk
    HAVING COUNT(*) > 1
) sub;

-- -----------------------------------------------------------------------------
-- CHECK 10 — TTC / HT consistency (TTC should equal HT * 1.20)
-- Tolerance: ± 0.02 MAD to allow for rounding
-- -----------------------------------------------------------------------------
SELECT 'CHECK 10 — Incohérence TTC/HT' AS check_name,
       COUNT(*)                         AS nb_problemes
FROM   dwh_mexora.fait_ventes
WHERE  ABS(montant_ttc - montant_ht * 1.20) > 0.02;

-- -----------------------------------------------------------------------------
-- SUMMARY — Row counts per table
-- -----------------------------------------------------------------------------
SELECT 'dim_temps'    AS table_name, COUNT(*) AS nb_lignes FROM dwh_mexora.dim_temps
UNION ALL
SELECT 'dim_produit',  COUNT(*) FROM dwh_mexora.dim_produit
UNION ALL
SELECT 'dim_client',   COUNT(*) FROM dwh_mexora.dim_client
UNION ALL
SELECT 'dim_region',   COUNT(*) FROM dwh_mexora.dim_region
UNION ALL
SELECT 'dim_livreur',  COUNT(*) FROM dwh_mexora.dim_livreur
UNION ALL
SELECT 'fait_ventes',  COUNT(*) FROM dwh_mexora.fait_ventes
ORDER BY table_name;
