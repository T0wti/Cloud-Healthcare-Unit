-- =====================================================================
-- Projet CHU - Benchmark : rendre les 3 bases visibles et décrites pour Impala
-- Exécution : impala-shell -i localhost:21000 -f 04_impala_metadata.sql
--
-- Ni DDL ni DML : seulement les métadonnées d'Impala. Les tables sont créées
-- par Hive (03_gold_partitioning.hql, 03a_gold_flat.hql, 03b_gold_bucketed.hql).
-- Repères : [MÉTADONNÉES] INVALIDATE METADATA, [STATS] COMPUTE STATS,
--           [CONTRÔLE] EXPLAIN (lecture seule)
--
-- POURQUOI
--   Impala a son propre cache : une table écrite par Hive reste invisible ou
--   périmée jusqu'à INVALIDATE METADATA.
--   Sans COMPUTE STATS, Impala choisit mal l'ordre et le type des jointures.
--
-- QUAND : après chaque exécution de 03_gold_partitioning.hql, 03a ou 03b.
-- =====================================================================

-- =====================================================================
-- 1. [MÉTADONNÉES] Recharger le catalogue : les tables écrites par Hive
-- deviennent visibles
-- =====================================================================
INVALIDATE METADATA;

-- =====================================================================
-- 2. [STATS] Statistiques des 10 tables de chaque base
-- =====================================================================

-- gold (scénario v1) : fait_deces et fait_consultation partitionnés par année
COMPUTE STATS gold.dim_temps;
COMPUTE STATS gold.dim_localisation;
COMPUTE STATS gold.dim_etablissement;
COMPUTE STATS gold.dim_patient;
COMPUTE STATS gold.dim_diagnostic;
COMPUTE STATS gold.dim_professionnel;
COMPUTE STATS gold.fait_consultation;
COMPUTE STATS gold.fait_hospitalisation;
COMPUTE STATS gold.fait_satisfaction;
COMPUTE STATS gold.fait_deces;

-- gold_flat (scénario v0) : sans partition ni bucket
COMPUTE STATS gold_flat.dim_temps;
COMPUTE STATS gold_flat.dim_localisation;
COMPUTE STATS gold_flat.dim_etablissement;
COMPUTE STATS gold_flat.dim_patient;
COMPUTE STATS gold_flat.dim_diagnostic;
COMPUTE STATS gold_flat.dim_professionnel;
COMPUTE STATS gold_flat.fait_consultation;
COMPUTE STATS gold_flat.fait_hospitalisation;
COMPUTE STATS gold_flat.fait_satisfaction;
COMPUTE STATS gold_flat.fait_deces;

-- gold_bucketed (scénario v2) : v1 + buckets id_professionnel sur
-- fait_consultation et dim_professionnel.
-- Ce scénario se mesure dans Hive : Impala ignore les buckets écrits par Hive.
COMPUTE STATS gold_bucketed.dim_temps;
COMPUTE STATS gold_bucketed.dim_localisation;
COMPUTE STATS gold_bucketed.dim_etablissement;
COMPUTE STATS gold_bucketed.dim_patient;
COMPUTE STATS gold_bucketed.dim_diagnostic;
COMPUTE STATS gold_bucketed.dim_professionnel;
COMPUTE STATS gold_bucketed.fait_consultation;
COMPUTE STATS gold_bucketed.fait_hospitalisation;
COMPUTE STATS gold_bucketed.fait_satisfaction;
COMPUTE STATS gold_bucketed.fait_deces;

-- =====================================================================
-- 3. [CONTRÔLE] Preuve de l'élagage de partitions, à copier dans le rapport
-- gold_flat lit toute la table, gold ne lit qu'une partition
-- (« partitions=1/N » dans le plan).
-- =====================================================================
EXPLAIN SELECT count(*) FROM gold_flat.fait_deces WHERE annee = 2019;
EXPLAIN SELECT count(*) FROM gold.fait_deces      WHERE annee = 2019;
