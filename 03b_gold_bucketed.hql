SET hive.mapred.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=500;
SET hive.exec.max.dynamic.partitions.pernode=500;
SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
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
-- Projet CHU - Benchmark : scénario v2, PARTITIONNÉ + BUCKETÉ (gold_bucketed)
-- Exécution : beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 03b_gold_bucketed.hql
-- Prérequis : gold chargé et conforme (03_gold_partitioning.hql)
-- Repères : mêmes conventions que 03_gold_partitioning.hql (dont aucun
-- commentaire juste avant un SET)
--
-- PLAN
--   0. [CONFIG] + [DDL]  Réglages de session (en tête) et base gold_bucketed
--   1. Dimensions        [DDL+DML] 5 copies de gold
--                        [DDL] puis [DML] dim_professionnel (en buckets)
--   2. Faits             [DDL] puis [DML] fait_consultation et fait_deces
--                        [DDL+DML] fait_hospitalisation et fait_satisfaction
--   3. [CONTRÔLE]        Volumes et nombre de fichiers par bucket
--   4. [STATS]           Statistiques pour l'optimiseur
--
-- v2 = v1 (gold) + buckets sur UNE seule jointure, la plus lourde.
-- Partitionnement identique à gold (fait_deces, fait_consultation par annee).
--
-- BUCKETS : fait_consultation et dim_professionnel, 4 buckets triés sur
-- id_professionnel (un par vcore de la VM).
--   Consultations x professionnels (1 M x 1 M) est la plus grosse jointure
--   du modèle. Même clé, même nombre de buckets, données triées : Hive la
--   fait bucket par bucket (sort-merge bucket join), sans table de hachage
--   ni redistribution. id_patient (100 000 patients) pèse moins : non retenu.
--   Ailleurs, pas de bucket : fait_deces ne joint que dim_localisation
--   (112 lignes, map join) et multiplierait les fichiers de ~140 partitions,
--   les autres tables sont petites.
--
-- MESURE dans Hive uniquement : Impala ignore les buckets écrits par Hive.
-- 05_benchmark.sh pose les réglages de jointure (bucketmapjoin, sortmerge),
-- EXPLAIN doit alors afficher « Sorted Merge Bucket Map Join Operator ».
-- =====================================================================

-- =====================================================================
-- 0. [CONFIG] RÉGLAGES DE SESSION (SET en tête), [DDL] BASE gold_bucketed
-- Partitions dynamiques, Parquet Snappy 32 Mo, Hive on Spark : mêmes réglages
-- que 03_gold_partitioning et 03a_gold_flat. enforce.bucketing donne un
-- réducteur par bucket : vérifié en 3c.
-- =====================================================================
CREATE DATABASE IF NOT EXISTS gold_bucketed
    COMMENT 'Benchmark v2 : constellation Parquet partitionnee, jointure consultation-professionnel en buckets'
    LOCATION '/gold_bucketed';
USE gold_bucketed;

-- =====================================================================
-- 1. DIMENSIONS
-- =====================================================================

-- [DDL+DML] 5 dimensions sans bucket : copie conforme de gold
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

-- [DDL] dim_professionnel : buckets alignés sur fait_consultation (déclarés
-- dans le CREATE TABLE, d'où la création à part)
DROP TABLE IF EXISTS dim_professionnel;
CREATE TABLE dim_professionnel (
    id_professionnel           INT,
    profession                 STRING,
    code_specialite            STRING,
    specialite                 STRING,
    categorie_professionnelle  STRING
)
CLUSTERED BY (id_professionnel) SORTED BY (id_professionnel ASC) INTO 4 BUCKETS
STORED AS PARQUET;

-- [DML] dim_professionnel : chargement depuis gold
INSERT OVERWRITE TABLE dim_professionnel
SELECT id_professionnel, profession, code_specialite, specialite, categorie_professionnelle
FROM gold.dim_professionnel;

SET hive.optimize.sort.dynamic.partition=false;

-- =====================================================================
-- 2. FAITS
-- [CONFIG] SET ci-dessus : une dizaine d'années de consultations seulement,
-- chaque réducteur (un par bucket) garde peu de fichiers ouverts, inutile de
-- trier par partition.
-- =====================================================================

-- [DDL] fait_consultation : partitionné par année ET en buckets
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
    creneau           STRING
)
PARTITIONED BY (annee INT)
CLUSTERED BY (id_professionnel) SORTED BY (id_professionnel ASC) INTO 4 BUCKETS
STORED AS PARQUET;

-- [DML] fait_consultation (annee en dernière colonne du SELECT)
INSERT OVERWRITE TABLE fait_consultation PARTITION (annee)
SELECT num_consultation, id_patient, id_diagnostic, id_etablissement, id_temps,
       id_localisation, id_professionnel, age, nb_consultation, creneau, annee
FROM gold.fait_consultation;

SET mapreduce.job.reduces=30;

-- [CONFIG] SET ci-dessus : 30 réducteurs pour fait_deces, remis à -1 ensuite
-- (même logique que 03_gold_partitioning.hql)
-- [DDL] fait_deces : partitionné par année, sans bucket (comme dans gold)
DROP TABLE IF EXISTS fait_deces;
CREATE TABLE fait_deces (
    num_deces        BIGINT,
    id_temps         INT,
    id_localisation  INT,
    sexe             STRING,
    age              INT,
    nb_deces         INT
)
PARTITIONED BY (annee INT)
STORED AS PARQUET;

-- [DML] fait_deces : DISTRIBUTE BY annee, une seule tâche par année
INSERT OVERWRITE TABLE fait_deces PARTITION (annee)
SELECT num_deces, id_temps, id_localisation, sexe, age, nb_deces, annee
FROM gold.fait_deces
DISTRIBUTE BY annee;

SET mapreduce.job.reduces=-1;

-- [DDL+DML] fait_hospitalisation, fait_satisfaction : ni partition ni bucket
-- (comme dans gold), copie conforme
DROP TABLE IF EXISTS fait_hospitalisation;
CREATE TABLE fait_hospitalisation STORED AS PARQUET AS SELECT * FROM gold.fait_hospitalisation;

DROP TABLE IF EXISTS fait_satisfaction;
CREATE TABLE fait_satisfaction STORED AS PARQUET AS SELECT * FROM gold.fait_satisfaction;

SET hive.compute.query.using.stats=false;

-- =====================================================================
-- 3. [CONTRÔLE] VÉRIFICATIONS (assert_true arrête le script en cas d'échec)
-- SET ci-dessus : lignes réellement comptées, pas lues dans le métastore.
-- =====================================================================

-- 3a. Mêmes volumes que gold
SELECT g.tbl, g.n AS lignes_gold, c.n AS lignes_gold_bucketed,
       assert_true(g.n = c.n) AS conforme
FROM (
              SELECT 'dim_professionnel'    AS tbl, count(*) AS n FROM gold.dim_professionnel
    UNION ALL SELECT 'fait_consultation'    AS tbl, count(*) AS n FROM gold.fait_consultation
    UNION ALL SELECT 'fait_hospitalisation' AS tbl, count(*) AS n FROM gold.fait_hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS tbl, count(*) AS n FROM gold.fait_satisfaction
    UNION ALL SELECT 'fait_deces'           AS tbl, count(*) AS n FROM gold.fait_deces
) g
JOIN (
              SELECT 'dim_professionnel'    AS tbl, count(*) AS n FROM dim_professionnel
    UNION ALL SELECT 'fait_consultation'    AS tbl, count(*) AS n FROM fait_consultation
    UNION ALL SELECT 'fait_hospitalisation' AS tbl, count(*) AS n FROM fait_hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS tbl, count(*) AS n FROM fait_satisfaction
    UNION ALL SELECT 'fait_deces'           AS tbl, count(*) AS n FROM fait_deces
) c ON c.tbl = g.tbl
ORDER BY g.tbl;

-- 3b. Nombre de fichiers des tables en buckets, par partition (affiché pour
-- le rapport, sans assert)
SELECT x.tbl, x.annee, x.nb_fichiers
FROM (
              SELECT 'dim_professionnel' AS tbl, CAST(NULL AS INT) AS annee,
                     count(DISTINCT INPUT__FILE__NAME) AS nb_fichiers
              FROM dim_professionnel
    UNION ALL SELECT 'fait_consultation' AS tbl, annee,
                     count(DISTINCT INPUT__FILE__NAME) AS nb_fichiers
              FROM fait_consultation GROUP BY annee
) x
ORDER BY x.tbl, x.annee;

-- 3c. Condition du sort-merge bucket join : 4 fichiers dans dim_professionnel
-- et dans les partitions 2019 et 2020 (lues par le benchmark)
SELECT assert_true(count(DISTINCT INPUT__FILE__NAME) = 4) AS dim_professionnel_4_buckets
FROM dim_professionnel;

SELECT assert_true(min(x.nb) = 4 AND max(x.nb) = 4) AS consultation_2019_2020_4_buckets
FROM (
    SELECT annee, count(DISTINCT INPUT__FILE__NAME) AS nb
    FROM fait_consultation
    WHERE annee IN (2019, 2020)
    GROUP BY annee
) x;

-- =====================================================================
-- 4. [STATS] STATISTIQUES : les mêmes que dans gold et gold_flat
-- =====================================================================
ANALYZE TABLE dim_temps            COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_localisation     COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_etablissement    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_patient          COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_diagnostic       COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_professionnel    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_consultation    PARTITION (annee) COMPUTE STATISTICS;
ANALYZE TABLE fait_consultation    PARTITION (annee) COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_hospitalisation COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_satisfaction    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_deces           PARTITION (annee) COMPUTE STATISTICS;
ANALYZE TABLE fait_deces           PARTITION (annee) COMPUTE STATISTICS FOR COLUMNS;
