-- =====================================================================
-- Projet CHU - Benchmark : rendre les 3 bases visibles et décrites pour Impala
-- Exécution : impala-shell -i localhost:21000 -f 04_impala_metadata.sql
--
-- Impala garde son propre cache de métadonnées : une table créée ou rechargée
-- par Hive reste invisible (ou périmée) tant qu'on n'a pas fait INVALIDATE
-- METADATA. COMPUTE STATS donne ensuite au planificateur d'Impala le nombre de
-- lignes et les valeurs distinctes : sans lui, Impala choisit mal l'ordre et le
-- type des jointures, et le benchmark mesure ce mauvais choix.
-- À relancer après chaque exécution de 02_gold.hql, 03a ou 03b.
-- =====================================================================

INVALIDATE METADATA;

-- gold : fait_deces et fait_consultation partitionnés par année (scénario v1)
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

-- gold_flat : sans partition (scénario v0)
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

-- gold_bucketed : v1 + buckets id_professionnel sur fait_consultation et
-- dim_professionnel (scénario v2, à mesurer dans Hive : Impala ignore les buckets)
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

-- Preuve de l'élagage de partitions, à copier dans le rapport :
-- gold_flat lit tout, gold ne lit qu'une partition (« partitions=1/N »).
EXPLAIN SELECT count(*) FROM gold_flat.fait_deces WHERE annee = 2019;
EXPLAIN SELECT count(*) FROM gold.fait_deces      WHERE annee = 2019;
