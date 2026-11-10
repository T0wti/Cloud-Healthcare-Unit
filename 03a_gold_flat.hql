SET hive.mapred.mode=nonstrict;
SET parquet.compression=SNAPPY;
SET parquet.block.size=33554432;
SET hive.execution.engine=spark;
SET spark.executor.instances=1;
SET spark.executor.cores=2;
SET spark.executor.memory=1500m;
SET spark.yarn.executor.memoryOverhead=384;
SET spark.driver.memory=512m;
SET spark.yarn.driver.memoryOverhead=384;
-- =====================================================================
-- Projet CHU - Benchmark : scénario v0, SANS OPTIMISATION (gold_flat)
-- Exécution : beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 03a_gold_flat.hql
-- Prérequis : gold chargé et conforme (03_gold_partitioning.hql)
-- Repères : mêmes conventions que 03_gold_partitioning.hql (dont aucun
-- commentaire juste avant un SET)
--
-- PLAN
--   0. [CONFIG] + [DDL]  Réglages de session (en tête) et base gold_flat
--   1. [DDL+DML]         Dimensions : copie conforme de gold
--   2. [DDL] puis [DML]  Faits : table créée, puis chargée depuis gold
--   3. [CONTRÔLE]        Mêmes volumes que gold
--   4. [STATS]           Statistiques pour l'optimiseur
--
-- gold_flat : même constellation (4 faits, 6 dimensions, Parquet Snappy),
-- sans partition ni bucket, annee en colonne ordinaire. Ce n'est pas une
-- table plate dénormalisée : c'est la référence « avant optimisation
-- physique » du benchmark.
-- =====================================================================

-- =====================================================================
-- 0. [CONFIG] RÉGLAGES DE SESSION (SET en tête), [DDL] BASE gold_flat
-- Parquet Snappy 32 Mo, Hive on Spark : mêmes réglages que
-- 03_gold_partitioning et 03b_gold_bucketed
-- =====================================================================
CREATE DATABASE IF NOT EXISTS gold_flat
    COMMENT 'Benchmark : constellation Parquet sans partition ni bucket'
    LOCATION '/gold_flat';
USE gold_flat;

-- =====================================================================
-- 1. [DDL+DML] DIMENSIONS : copie conforme de gold
-- =====================================================================
DROP TABLE IF EXISTS dim_temps;
CREATE TABLE dim_temps STORED AS PARQUET AS SELECT * FROM gold.dim_temps;

DROP TABLE IF EXISTS dim_localisation;
CREATE TABLE dim_localisation STORED AS PARQUET AS SELECT * FROM gold.dim_localisation;

DROP TABLE IF EXISTS dim_etablissement;
CREATE TABLE dim_etablissement STORED AS PARQUET AS SELECT * FROM gold.dim_etablissement;

DROP TABLE IF EXISTS dim_patient;
CREATE TABLE dim_patient STORED AS PARQUET AS SELECT * FROM gold.dim_patient;

DROP TABLE IF EXISTS dim_diagnostic;
CREATE TABLE dim_diagnostic STORED AS PARQUET AS SELECT * FROM gold.dim_diagnostic;

DROP TABLE IF EXISTS dim_professionnel;
CREATE TABLE dim_professionnel STORED AS PARQUET AS SELECT * FROM gold.dim_professionnel;

-- =====================================================================
-- 2. FAITS : sans partition, annee en colonne ordinaire
-- Colonnes listées (pas de SELECT *) : la colonne de partition annee de gold
-- devient ici la dernière colonne ordinaire.
-- =====================================================================

-- [DDL] fait_consultation
DROP TABLE IF EXISTS fait_consultation;
CREATE TABLE fait_consultation (
    num_consultation  INT,
    id_patient        INT,
    id_diagnostic     INT,
    id_etablissement  INT,
    id_temps          INT,
    id_localisation   INT,
    id_professionnel  INT,
    age               INT,
    nb_consultation   INT,
    creneau           STRING,
    annee             INT
) STORED AS PARQUET;

-- [DML] fait_consultation : chargement depuis gold
INSERT OVERWRITE TABLE fait_consultation
SELECT num_consultation, id_patient, id_diagnostic, id_etablissement, id_temps,
       id_localisation, id_professionnel, age, nb_consultation, creneau, annee
FROM gold.fait_consultation;

-- [DDL] fait_hospitalisation
DROP TABLE IF EXISTS fait_hospitalisation;
CREATE TABLE fait_hospitalisation (
    num_hospitalisation  INT,
    id_patient           INT,
    id_diagnostic        INT,
    id_etablissement     INT,
    id_temps             INT,
    id_localisation      INT,
    age                  INT,
    nb_hospitalisation   INT,
    duree_sejour_jours   INT,
    annee                INT
) STORED AS PARQUET;

-- [DML] fait_hospitalisation : chargement depuis gold
INSERT OVERWRITE TABLE fait_hospitalisation
SELECT num_hospitalisation, id_patient, id_diagnostic, id_etablissement, id_temps,
       id_localisation, age, nb_hospitalisation, duree_sejour_jours, annee
FROM gold.fait_hospitalisation;

-- [DDL] fait_satisfaction
DROP TABLE IF EXISTS fait_satisfaction;
CREATE TABLE fait_satisfaction (
    id_etablissement     INT,
    id_temps             INT,
    id_localisation      INT,
    classement           STRING,
    note_satisfaction    DECIMAL(5,2),
    nb_avis              INT,
    score_accueil        DECIMAL(5,2),
    score_pec_infirmier  DECIMAL(5,2),
    score_pec_medical    DECIMAL(5,2),
    score_chambre        DECIMAL(5,2),
    score_repas          DECIMAL(5,2),
    score_sortie         DECIMAL(5,2),
    taux_recommandation  DECIMAL(5,2),
    nb_reco              INT,
    annee                INT
) STORED AS PARQUET;

-- [DML] fait_satisfaction : chargement depuis gold
INSERT OVERWRITE TABLE fait_satisfaction
SELECT id_etablissement, id_temps, id_localisation, classement, note_satisfaction, nb_avis,
       score_accueil, score_pec_infirmier, score_pec_medical, score_chambre, score_repas,
       score_sortie, taux_recommandation, nb_reco, annee
FROM gold.fait_satisfaction;

-- [DDL] fait_deces
DROP TABLE IF EXISTS fait_deces;
CREATE TABLE fait_deces (
    num_deces        BIGINT,
    id_temps         INT,
    id_localisation  INT,
    sexe             STRING,
    age              INT,
    nb_deces         INT,
    annee            INT
) STORED AS PARQUET;

-- [DML] fait_deces : chargement depuis gold
INSERT OVERWRITE TABLE fait_deces
SELECT num_deces, id_temps, id_localisation, sexe, age, nb_deces, annee
FROM gold.fait_deces;

SET hive.compute.query.using.stats=false;

-- =====================================================================
-- 3. [CONTRÔLE] MÊMES VOLUMES QUE gold (assert_true arrête le script sinon)
-- SET ci-dessus : lignes réellement comptées, pas lues dans le métastore.
-- =====================================================================
SELECT g.tbl, g.n AS lignes_gold, c.n AS lignes_gold_flat,
       assert_true(g.n = c.n) AS conforme
FROM (
              SELECT 'fait_consultation'    AS tbl, count(*) AS n FROM gold.fait_consultation
    UNION ALL SELECT 'fait_hospitalisation' AS tbl, count(*) AS n FROM gold.fait_hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS tbl, count(*) AS n FROM gold.fait_satisfaction
    UNION ALL SELECT 'fait_deces'           AS tbl, count(*) AS n FROM gold.fait_deces
) g
JOIN (
              SELECT 'fait_consultation'    AS tbl, count(*) AS n FROM fait_consultation
    UNION ALL SELECT 'fait_hospitalisation' AS tbl, count(*) AS n FROM fait_hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS tbl, count(*) AS n FROM fait_satisfaction
    UNION ALL SELECT 'fait_deces'           AS tbl, count(*) AS n FROM fait_deces
) c ON c.tbl = g.tbl
ORDER BY g.tbl;

-- =====================================================================
-- 4. [STATS] STATISTIQUES : les mêmes que dans gold et gold_bucketed
-- =====================================================================
ANALYZE TABLE dim_temps            COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_localisation     COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_etablissement    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_patient          COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_diagnostic       COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_professionnel    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_consultation    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_hospitalisation COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_satisfaction    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_deces           COMPUTE STATISTICS FOR COLUMNS;
