-- =====================================================================
-- Projet CHU - Benchmark : scénario v2, PARTITIONNÉ + BUCKETÉ (gold_bucketed)
-- Exécution : beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 03b_gold_bucketed.hql
--
-- v2 = v1 (gold, 02_gold.hql) + buckets sur UNE seule jointure : la plus lourde.
--
-- Partitionnement : identique à gold
--   fait_deces, fait_consultation : PARTITIONED BY (annee)
--   fait_hospitalisation, fait_satisfaction, dimensions : sans partition
--
-- Bucketing : fait_consultation et dim_professionnel
--   CLUSTERED BY (id_professionnel) SORTED BY (id_professionnel) INTO 4 BUCKETS
--   Pourquoi cette clé : « taux de consultation par professionnel » joint
--   1 M de consultations à 1 M de professionnels, la plus grosse jointure
--   entre deux tables du modèle. Même clé, même nombre de buckets, données
--   triées : Hive peut la faire bucket par bucket (sort-merge bucket join),
--   sans table de hachage ni redistribution des données.
--   Une table ne se bucketise que sur une clé : id_patient (100 000 patients)
--   pèse moins lourd, il n'est pas retenu.
--   4 buckets : un par vcore de la VM.
--
-- Pas de bucket ailleurs :
--   fait_deces : sa seule jointure est dim_localisation (112 lignes), déjà
--   chargée en mémoire par un map join ; les buckets ne feraient que
--   multiplier les fichiers de ~140 partitions.
--   fait_hospitalisation, fait_satisfaction : quelques milliers de lignes.
--   Autres dimensions : petites, chargées en mémoire par chaque jointure.
--
-- Mesure : dans Hive (beeline). Impala ignore les buckets écrits par Hive.
-- Au moment de la requête de benchmark (pas au chargement) :
--   SET hive.optimize.bucketmapjoin=true;
--   SET hive.optimize.bucketmapjoin.sortedmerge=true;
--   SET hive.auto.convert.sortmerge.join=true;
--   SET hive.auto.convert.sortmerge.join.to.mapjoin=false;
--   SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
-- EXPLAIN doit alors afficher « Sorted Merge Bucket Map Join Operator ».
-- Prérequis : gold chargé et conforme (02_gold.hql).
-- =====================================================================

SET hive.mapred.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=500;
SET hive.exec.max.dynamic.partitions.pernode=500;
-- Écriture des buckets : un réducteur par bucket, lignes triées par
-- id_professionnel (SORTED BY).
SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
SET parquet.compression=SNAPPY;
SET parquet.block.size=33554432;

-- Moteur : Hive on Spark, mêmes réglages que 02_gold et 03a_gold_flat.
-- Mémoire : YARN dispose de 3 Go, soit un exécuteur (1,5 Go + 384 Mo) et le
-- driver (512 Mo + 384 Mo).
-- Hive on Spark respecte hive.enforce.bucketing (un réducteur par bucket) :
-- le contrôle 3c vérifie bien 4 fichiers par partition.
SET hive.execution.engine=spark;
SET spark.executor.instances=1;
SET spark.executor.cores=2;
SET spark.executor.memory=1500m;
SET spark.yarn.executor.memoryOverhead=384;
SET spark.driver.memory=512m;
SET spark.yarn.driver.memoryOverhead=384;

CREATE DATABASE IF NOT EXISTS gold_bucketed
    COMMENT 'Benchmark v2 : constellation Parquet partitionnee, jointure consultation-professionnel en buckets'
    LOCATION '/gold_bucketed';
USE gold_bucketed;

-- =====================================================================
-- 1. DIMENSIONS
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

-- dim_professionnel : buckets triés sur id_professionnel, alignés sur fait_consultation
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

INSERT OVERWRITE TABLE dim_professionnel
SELECT id_professionnel, profession, code_specialite, specialite, categorie_professionnelle
FROM gold.dim_professionnel;

-- =====================================================================
-- 2. FAITS
-- =====================================================================

-- fait_consultation : partitionné par année ET bucketé sur id_professionnel.
-- Une dizaine d'années seulement : chaque réducteur (un par bucket) garde peu
-- de fichiers ouverts, le tri par partition n'est pas nécessaire.
SET hive.optimize.sort.dynamic.partition=false;

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

INSERT OVERWRITE TABLE fait_consultation PARTITION (annee)
SELECT num_consultation, id_patient, id_diagnostic, id_etablissement, id_temps,
       id_localisation, id_professionnel, age, nb_consultation, creneau, annee
FROM gold.fait_consultation;

-- fait_deces : partitionné par année, sans bucket (comme dans gold).
-- DISTRIBUTE BY annee : chaque année est écrite par une seule tâche, peu de
-- fichiers Parquet ouverts à la fois (même logique que 02_gold.hql).
SET mapreduce.job.reduces=30;

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

INSERT OVERWRITE TABLE fait_deces PARTITION (annee)
SELECT num_deces, id_temps, id_localisation, sexe, age, nb_deces, annee
FROM gold.fait_deces
DISTRIBUTE BY annee;

SET mapreduce.job.reduces=-1;

-- fait_hospitalisation, fait_satisfaction : ni partition ni bucket (comme dans gold)
DROP TABLE IF EXISTS fait_hospitalisation;
CREATE TABLE fait_hospitalisation STORED AS PARQUET AS SELECT * FROM gold.fait_hospitalisation;

DROP TABLE IF EXISTS fait_satisfaction;
CREATE TABLE fait_satisfaction STORED AS PARQUET AS SELECT * FROM gold.fait_satisfaction;

-- =====================================================================
-- 3. CONTRÔLES (le script s'arrête si l'un échoue)
-- =====================================================================
SET hive.compute.query.using.stats=false;

-- 3a. Mêmes volumes que gold (comptage réel)
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

-- 3b. Nombre de fichiers des tables bucketées, par partition : pour le rapport.
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

-- 3c. Condition du SMB join : 4 fichiers dans dim_professionnel et dans
-- chacune des partitions 2019 et 2020 de fait_consultation.
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
-- 4. STATISTIQUES : les mêmes que dans gold et gold_flat, pour que
-- l'optimiseur parte des mêmes informations dans les trois bases.
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
