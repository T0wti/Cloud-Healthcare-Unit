SET hive.mapred.mode=nonstrict;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=500;
SET hive.exec.max.dynamic.partitions.pernode=500;
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
-- Projet CHU - Étape 3 : du Silver au Gold (constellation Parquet)
-- Benchmark : scénario v1, PARTITIONNÉ (v0 = 03a gold_flat, v2 = 03b gold_bucketed)
-- Exécution : nohup beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 03_gold_partitioning.hql > 03.log 2>&1 &
--
-- Repères : [DDL] structures, [DML] chargement, [DDL+DML] CREATE TABLE AS SELECT,
--           [CONTRÔLE] lecture seule, [CONFIG] SET, [STATS] ANALYZE TABLE
-- Aucun commentaire juste avant un SET : beeline (CDH) ne le reconnaîtrait
-- plus comme SET. Les SET sont donc en tête, ou commentés après coup.
--
-- PLAN
--   0. [CONFIG]   Réglages de session (en tête du fichier)
--   1. [DDL]      Macros, base gold, puis [CONTRÔLE] du Silver
--   2. [DDL]      Tables : 6 dimensions, 4 faits, 1 table de contrôle
--   3. [DML]      Dimensions
--   4. [DDL+DML]  Tables de travail des faits (temporaires)
--   5. [DML]      Faits et rejets
--   6. [DML]      Audit, puis [CONTRÔLE] vérifications
--   7. [STATS]    Statistiques pour l'optimiseur
--
-- IDENTIFIANTS D'EXÉCUTION en dur, à remplacer partout avant de relancer :
--   '20261104_1' exécution Silver vérifiée en entrée, '20261105_1' cette exécution Gold
--
-- RÈGLES
--   clés    : une clé attribuée ne change jamais, nouvelle entité = plus grande clé + 1
--   membres : -1 Inconnu (toutes les dimensions), -2 Plusieurs (établissement, localisation)
--   faits   : clé étrangère jamais NULL, -1 si absente ou introuvable
--   rejets dans silver.rejets (partitions gold_<table>), audit dans silver.audit
--   et gold.controle_membres
--
-- PARTITIONNEMENT par annee (= id_temps DIV 10000, -1 si id_temps = -1), là
-- seulement où les requêtes filtrent l'année et où le volume le justifie :
--   oui : fait_deces (24,8 M lignes, ~140 années), fait_consultation (1 M)
--   non : fait_hospitalisation (2 479) et fait_satisfaction (3 459), des
--         partitions de quelques lignes coûtent plus à ouvrir qu'à lire,
--         ni les dimensions (jointes, jamais filtrées par plage)
--   INSERT OVERWRITE dynamique : seules les années reçues sont remplacées.
--
-- ÉCARTS AU DICTIONNAIRE : num_deces en BIGINT (AAAAMMJJ x 100000 + rang),
-- colonne annee ajoutée aux faits.
-- =====================================================================

-- =====================================================================
-- 0. [CONFIG] RÉGLAGES DE SESSION (SET en tête du fichier)
--   partitions dynamiques (annee lue dans les données), Parquet Snappy 32 Mo
--   Hive on Spark, mêmes réglages dans 03a et 03b : temps de chargement comparables
--   YARN dispose de 3 Go : un exécuteur (1,5 Go + 384 Mo) et le driver (512 Mo + 384 Mo)
-- =====================================================================

-- =====================================================================
-- 1. [DDL] MACROS DE CALCUL (temporaires, limitées à la session) ET BASE gold
-- =====================================================================

-- Âge en années révolues entre deux dates AAAA-MM-JJ, NULL si une date manque
CREATE TEMPORARY MACRO age_revolu(naiss STRING, evt STRING)
    CAST(substr(evt, 1, 4) AS INT) - CAST(substr(naiss, 1, 4) AS INT)
      - CASE WHEN substr(evt, 6, 5) < substr(naiss, 6, 5) THEN 1 ELSE 0 END;

-- Âge gardé seulement de 0 à 120 ans, NULL sinon
CREATE TEMPORARY MACRO age_borne(a INT)
    CASE WHEN a BETWEEN 0 AND 120 THEN a END;

-- Partition d'un fait à partir de id_temps
CREATE TEMPORARY MACRO annee_partition(t INT)
    CAST(CASE WHEN t = -1 THEN -1 ELSE t DIV 10000 END AS INT);

CREATE DATABASE IF NOT EXISTS gold COMMENT 'Entrepôt décisionnel : constellation, Parquet' LOCATION '/gold';
USE gold;

-- ---------------------------------------------------------------------
-- [CONTRÔLE] Silver conforme avant toute écriture : assert_true arrête le
-- script si l'exécution Silver est absente ou si l'un de ses 11 flux n'est
-- pas OK.
-- ---------------------------------------------------------------------
SELECT assert_true(count(*) = 11 AND sum(CASE WHEN statut = 'OK' THEN 1 ELSE 0 END) = 11) AS silver_conforme
FROM silver.audit
WHERE id_silver = '20261104_1';

-- =====================================================================
-- 2. [DDL] TABLES GOLD : 6 dimensions, 4 faits, 1 table de contrôle
-- IF NOT EXISTS : les dimensions ne sont jamais supprimées, elles portent
-- les clés déjà attribuées.
-- =====================================================================

-- Dimensions
CREATE TABLE IF NOT EXISTS dim_temps (
    id_temps       INT,
    date_complete  TIMESTAMP,
    jour           INT,
    jour_semaine   INT,
    libelle_jour   STRING,
    semaine        INT,
    mois           INT,
    libelle_mois   STRING,
    trimestre      INT,
    annee          INT,
    est_weekend    BOOLEAN
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_localisation (
    id_localisation   INT,
    code_departement  STRING,
    departement       STRING,
    code_region       STRING,
    region            STRING
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_etablissement (
    id_etablissement          INT,
    identifiant_organisation  STRING,
    num_finess_geo            STRING,
    num_finess_juridique      STRING,
    raison_sociale            STRING,
    type_structure            STRING,
    code_commune              STRING,
    commune                   STRING
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_patient (
    id_patient       INT,
    sexe             STRING,
    annee_naissance  INT
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_diagnostic (
    id_diagnostic       INT,
    code_diagnostic     STRING,
    libelle_diagnostic  STRING,
    code_categorie      STRING,
    chapitre_cim10      STRING
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dim_professionnel (
    id_professionnel           INT,
    profession                 STRING,
    code_specialite            STRING,
    specialite                 STRING,
    categorie_professionnelle  STRING
) STORED AS PARQUET;

-- Fait partitionné : annee est déclarée dans PARTITIONED BY, pas dans les colonnes
CREATE TABLE IF NOT EXISTS fait_consultation (
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
) PARTITIONED BY (annee INT) STORED AS PARQUET;

-- Faits non partitionnés : annee est une colonne ordinaire.
-- DROP puis CREATE : IF NOT EXISTS garderait une ancienne version
-- partitionnée et l'INSERT échouerait. Sans risque : un fait est recalculé
-- en entier et ne porte aucune clé à préserver.
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

-- Fait partitionné
CREATE TABLE IF NOT EXISTS fait_deces (
    num_deces        BIGINT,
    id_temps         INT,
    id_localisation  INT,
    sexe             STRING,
    age              INT,
    nb_deces         INT
) PARTITIONED BY (annee INT) STORED AS PARQUET;

-- Contrôle : membres -1 / -2 par fait, une ligne par fait et par exécution
CREATE TABLE IF NOT EXISTS controle_membres (
    id_gold             STRING,
    date_execution      STRING,
    fait                STRING,
    nb_lignes           BIGINT,
    patient_m1          BIGINT,
    diagnostic_m1       BIGINT,
    etablissement_m1    BIGINT,
    etablissement_m2    BIGINT,
    temps_m1            BIGINT,
    localisation_m1     BIGINT,
    localisation_m2     BIGINT,
    professionnel_m1    BIGINT,
    age_null            BIGINT
) STORED AS PARQUET;

-- =====================================================================
-- 3. [DML] CHARGEMENT DES DIMENSIONS
-- INSERT OVERWRITE remplace la dimension, mais les clés déjà attribuées sont
-- relues et conservées. Localisation, établissement et diagnostic sont
-- l'union de 4 parties :
--   membres spéciaux (-1, -2)
--   entités connues   : clé conservée, attributs mis à jour
--   nouvelles entités : plus grande clé + 1, dans l'ordre de la clé naturelle
--   entités disparues de la source : ligne gardée, la clé reste valable
-- « FROM (SELECT max(1) ...) o » sert seulement à produire une ligne unique.
-- =====================================================================

-- ---------------------------------------------------------------------
-- [DML] DIM_TEMPS : calendrier généré, sans table source
-- Du 01/01/1880 au 31/12/2030, soit 55 152 jours, plus le membre -1.
-- id_temps = AAAAMMJJ. jour_semaine : 1 = lundi ... 7 = dimanche.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dim_temps
SELECT u.id_temps, u.date_complete, u.jour, u.jour_semaine, u.libelle_jour, u.semaine,
       u.mois, u.libelle_mois, u.trimestre, u.annee, u.est_weekend
FROM (
    SELECT -1 AS id_temps, CAST(NULL AS TIMESTAMP) AS date_complete, CAST(NULL AS INT) AS jour,
           CAST(NULL AS INT) AS jour_semaine, CAST(NULL AS STRING) AS libelle_jour, CAST(NULL AS INT) AS semaine,
           CAST(NULL AS INT) AS mois, CAST(NULL AS STRING) AS libelle_mois, CAST(NULL AS INT) AS trimestre,
           CAST(NULL AS INT) AS annee, CAST(NULL AS BOOLEAN) AS est_weekend
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT CAST(regexp_replace(c.d, '-', '') AS INT) AS id_temps,
           CAST(concat(c.d, ' 00:00:00') AS TIMESTAMP) AS date_complete,
           day(c.d) AS jour,
           c.js AS jour_semaine,
           CASE c.js WHEN 1 THEN 'lundi' WHEN 2 THEN 'mardi' WHEN 3 THEN 'mercredi' WHEN 4 THEN 'jeudi'
                     WHEN 5 THEN 'vendredi' WHEN 6 THEN 'samedi' ELSE 'dimanche' END AS libelle_jour,
           weekofyear(c.d) AS semaine,
           month(c.d) AS mois,
           CASE month(c.d) WHEN 1 THEN 'janvier' WHEN 2 THEN 'février' WHEN 3 THEN 'mars' WHEN 4 THEN 'avril'
                           WHEN 5 THEN 'mai' WHEN 6 THEN 'juin' WHEN 7 THEN 'juillet' WHEN 8 THEN 'août'
                           WHEN 9 THEN 'septembre' WHEN 10 THEN 'octobre' WHEN 11 THEN 'novembre'
                           ELSE 'décembre' END AS libelle_mois,
           CAST((month(c.d) + 2) DIV 3 AS INT) AS trimestre,
           year(c.d) AS annee,
           c.js >= 6 AS est_weekend
    FROM (
        SELECT g.d, CAST(pmod(datediff(g.d, '1900-01-01'), 7) + 1 AS INT) AS js
        FROM (
            SELECT date_add('1880-01-01', p.pos) AS d
            FROM (SELECT max(1) AS x FROM silver.localisation) o
            LATERAL VIEW posexplode(split(space(55151), ' ')) p AS pos, val
        ) g
    ) c
) u;

-- ---------------------------------------------------------------------
-- [DML] DIM_LOCALISATION : un département par ligne
-- Clé naturelle : code_departement. Source : silver.localisation.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dim_localisation
SELECT u.id_localisation, u.code_departement, u.departement, u.code_region, u.region
FROM (
    SELECT -1 AS id_localisation, '-1' AS code_departement, 'Inconnu' AS departement,
           '-1' AS code_region, 'Inconnue' AS region
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT -2 AS id_localisation, '-2' AS code_departement, 'Plusieurs départements' AS departement,
           '-2' AS code_region, 'Plusieurs Régions' AS region
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT d.id_localisation AS id_localisation, s.code_departement AS code_departement,
           s.departement AS departement, s.code_region AS code_region, s.region AS region
    FROM silver.localisation s
    JOIN dim_localisation d ON d.code_departement = s.code_departement
    UNION ALL
    SELECT CAST(m.cle_max + ROW_NUMBER() OVER (ORDER BY n.code_departement) AS INT) AS id_localisation,
           n.code_departement AS code_departement, n.departement AS departement,
           n.code_region AS code_region, n.region AS region
    FROM (
        SELECT s.code_departement, s.departement, s.code_region, s.region
        FROM silver.localisation s
        LEFT JOIN dim_localisation d ON d.code_departement = s.code_departement
        WHERE d.code_departement IS NULL
    ) n
    CROSS JOIN (SELECT nvl(max(id_localisation), 0) AS cle_max FROM dim_localisation) m
    UNION ALL
    SELECT d.id_localisation AS id_localisation, d.code_departement AS code_departement,
           d.departement AS departement, d.code_region AS code_region, d.region AS region
    FROM dim_localisation d
    LEFT JOIN silver.localisation s ON s.code_departement = d.code_departement
    WHERE s.code_departement IS NULL AND d.id_localisation > 0
) u;

-- ---------------------------------------------------------------------
-- [DML] DIM_ETABLISSEMENT
-- Clé naturelle : identifiant_organisation. Source : silver.etablissement.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dim_etablissement
SELECT u.id_etablissement, u.identifiant_organisation, u.num_finess_geo, u.num_finess_juridique,
       u.raison_sociale, u.type_structure, u.code_commune, u.commune
FROM (
    SELECT -1 AS id_etablissement, 'INCONNU' AS identifiant_organisation,
           CAST(NULL AS STRING) AS num_finess_geo, CAST(NULL AS STRING) AS num_finess_juridique,
           'Établissement inconnu' AS raison_sociale, 'Inconnu' AS type_structure,
           CAST(NULL AS STRING) AS code_commune, CAST(NULL AS STRING) AS commune
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT -2 AS id_etablissement, 'PLUSIEURS' AS identifiant_organisation,
           CAST(NULL AS STRING) AS num_finess_geo, CAST(NULL AS STRING) AS num_finess_juridique,
           'Plusieurs établissements possibles' AS raison_sociale, 'Plusieurs' AS type_structure,
           CAST(NULL AS STRING) AS code_commune, CAST(NULL AS STRING) AS commune
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT d.id_etablissement AS id_etablissement, s.identifiant_organisation AS identifiant_organisation,
           s.num_finess_geo AS num_finess_geo, s.num_finess_juridique AS num_finess_juridique,
           s.raison_sociale AS raison_sociale, s.type_structure AS type_structure,
           s.code_commune AS code_commune, s.commune AS commune
    FROM silver.etablissement s
    JOIN dim_etablissement d ON d.identifiant_organisation = s.identifiant_organisation
    UNION ALL
    SELECT CAST(m.cle_max + ROW_NUMBER() OVER (ORDER BY n.identifiant_organisation) AS INT) AS id_etablissement,
           n.identifiant_organisation AS identifiant_organisation,
           n.num_finess_geo AS num_finess_geo, n.num_finess_juridique AS num_finess_juridique,
           n.raison_sociale AS raison_sociale, n.type_structure AS type_structure,
           n.code_commune AS code_commune, n.commune AS commune
    FROM (
        SELECT s.identifiant_organisation, s.num_finess_geo, s.num_finess_juridique,
               s.raison_sociale, s.type_structure, s.code_commune, s.commune
        FROM silver.etablissement s
        LEFT JOIN dim_etablissement d ON d.identifiant_organisation = s.identifiant_organisation
        WHERE d.identifiant_organisation IS NULL
    ) n
    CROSS JOIN (SELECT nvl(max(id_etablissement), 0) AS cle_max FROM dim_etablissement) m
    UNION ALL
    SELECT d.id_etablissement AS id_etablissement, d.identifiant_organisation AS identifiant_organisation,
           d.num_finess_geo AS num_finess_geo, d.num_finess_juridique AS num_finess_juridique,
           d.raison_sociale AS raison_sociale, d.type_structure AS type_structure,
           d.code_commune AS code_commune, d.commune AS commune
    FROM dim_etablissement d
    LEFT JOIN silver.etablissement s ON s.identifiant_organisation = d.identifiant_organisation
    WHERE s.identifiant_organisation IS NULL AND d.id_etablissement > 0
) u;

-- ---------------------------------------------------------------------
-- [DML] DIM_PATIENT
-- Clés reprises de silver.cle_patient. Chaque clé garde sa ligne, même si le
-- patient a disparu de la source : un fait ne pointe jamais dans le vide.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dim_patient
SELECT u.id_patient, u.sexe, u.annee_naissance
FROM (
    SELECT -1 AS id_patient, 'I' AS sexe, CAST(NULL AS INT) AS annee_naissance
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT c.id_patient AS id_patient, nvl(p.sexe, 'I') AS sexe, p.annee_naissance AS annee_naissance
    FROM silver.cle_patient c
    LEFT JOIN silver.patient p ON p.id_patient_pseudo = c.id_patient_pseudo
) u;

-- ---------------------------------------------------------------------
-- DIM_DIAGNOSTIC : chapitre CIM-10 déduit de la plage du code de catégorie.
-- Catégorie hors de toute plage : rejetée, absente de la dimension.
-- ---------------------------------------------------------------------

-- [DDL+DML] Table temporaire t_dim_diagnostic
DROP TABLE IF EXISTS t_dim_diagnostic;
CREATE TEMPORARY TABLE t_dim_diagnostic AS
SELECT code_diagnostic, libelle_diagnostic, code_categorie, id_execution,
       CASE
         WHEN code_categorie BETWEEN 'A00' AND 'B99' THEN "I – Certaines maladies infectieuses et parasitaires"
         WHEN code_categorie BETWEEN 'C00' AND 'D48' THEN "II – Tumeurs"
         WHEN code_categorie BETWEEN 'D50' AND 'D89' THEN "III – Maladies du sang et des organes hématopoïétiques et certains troubles du système immunitaire"
         WHEN code_categorie BETWEEN 'E00' AND 'E90' THEN "IV – Maladies endocriniennes, nutritionnelles et métaboliques"
         WHEN code_categorie BETWEEN 'F00' AND 'F99' THEN "V – Troubles mentaux et du comportement"
         WHEN code_categorie BETWEEN 'G00' AND 'G99' THEN "VI – Maladies du système nerveux"
         WHEN code_categorie BETWEEN 'H00' AND 'H59' THEN "VII – Maladies de l'œil et de ses annexes"
         WHEN code_categorie BETWEEN 'H60' AND 'H95' THEN "VIII – Maladies de l'oreille et de l'apophyse mastoïde"
         WHEN code_categorie BETWEEN 'I00' AND 'I99' THEN "IX – Maladies de l'appareil circulatoire"
         WHEN code_categorie BETWEEN 'J00' AND 'J99' THEN "X – Maladies de l'appareil respiratoire"
         WHEN code_categorie BETWEEN 'K00' AND 'K93' THEN "XI – Maladies de l'appareil digestif"
         WHEN code_categorie BETWEEN 'L00' AND 'L99' THEN "XII – Maladies de la peau et du tissu cellulaire sous-cutané"
         WHEN code_categorie BETWEEN 'M00' AND 'M99' THEN "XIII – Maladies du système ostéo-articulaire, des muscles et du tissu conjonctif"
         WHEN code_categorie BETWEEN 'N00' AND 'N99' THEN "XIV – Maladies de l'appareil génito-urinaire"
         WHEN code_categorie BETWEEN 'O00' AND 'O99' THEN "XV – Grossesse, accouchement et puerpéralité"
         WHEN code_categorie BETWEEN 'P00' AND 'P96' THEN "XVI – Certaines affections dont l'origine se situe dans la période périnatale"
         WHEN code_categorie BETWEEN 'Q00' AND 'Q99' THEN "XVII – Malformations congénitales et anomalies chromosomiques"
         WHEN code_categorie BETWEEN 'R00' AND 'R99' THEN "XVIII – Symptômes, signes et résultats anormaux d'examens cliniques et de laboratoire, non classés ailleurs"
         WHEN code_categorie BETWEEN 'S00' AND 'T98' THEN "XIX – Lésions traumatiques, empoisonnements et certaines autres conséquences de causes externes"
         WHEN code_categorie BETWEEN 'V01' AND 'Y98' THEN "XX – Causes externes de morbidité et de mortalité"
         WHEN code_categorie BETWEEN 'Z00' AND 'Z99' THEN "XXI – Facteurs influant sur l'état de santé et motifs de recours aux services de santé"
         WHEN code_categorie BETWEEN 'U00' AND 'U99' THEN "XXII – Codes d'utilisation particulière"
       END AS chapitre_cim10
FROM silver.diagnostic;

-- [DML] Rejets : catégorie hors de toute plage (chapitre NULL)
INSERT OVERWRITE TABLE silver.rejets PARTITION (flux = 'gold_dim_diagnostic')
SELECT id_execution, '20261105_1', '03_gold_partitioning', 'silver.diagnostic', CAST(NULL AS BIGINT),
       concat_ws('\073', code_diagnostic, libelle_diagnostic),
       'valeur hors domaine', from_unixtime(unix_timestamp())
FROM t_dim_diagnostic
WHERE chapitre_cim10 IS NULL;

-- [DML] Chargement de dim_diagnostic. Clé naturelle : code_diagnostic.
INSERT OVERWRITE TABLE dim_diagnostic
SELECT u.id_diagnostic, u.code_diagnostic, u.libelle_diagnostic, u.code_categorie, u.chapitre_cim10
FROM (
    SELECT -1 AS id_diagnostic, '-1' AS code_diagnostic, 'Diagnostic inconnu' AS libelle_diagnostic,
           'Inconnu' AS code_categorie, 'Inconnu' AS chapitre_cim10
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT d.id_diagnostic AS id_diagnostic, s.code_diagnostic AS code_diagnostic,
           s.libelle_diagnostic AS libelle_diagnostic, s.code_categorie AS code_categorie,
           s.chapitre_cim10 AS chapitre_cim10
    FROM t_dim_diagnostic s
    JOIN dim_diagnostic d ON d.code_diagnostic = s.code_diagnostic
    WHERE s.chapitre_cim10 IS NOT NULL
    UNION ALL
    SELECT CAST(m.cle_max + ROW_NUMBER() OVER (ORDER BY n.code_diagnostic) AS INT) AS id_diagnostic,
           n.code_diagnostic AS code_diagnostic, n.libelle_diagnostic AS libelle_diagnostic,
           n.code_categorie AS code_categorie, n.chapitre_cim10 AS chapitre_cim10
    FROM (
        SELECT s.code_diagnostic, s.libelle_diagnostic, s.code_categorie, s.chapitre_cim10
        FROM t_dim_diagnostic s
        LEFT JOIN dim_diagnostic d ON d.code_diagnostic = s.code_diagnostic
        WHERE d.code_diagnostic IS NULL AND s.chapitre_cim10 IS NOT NULL
    ) n
    CROSS JOIN (SELECT nvl(max(id_diagnostic), 0) AS cle_max FROM dim_diagnostic) m
    UNION ALL
    SELECT d.id_diagnostic AS id_diagnostic, d.code_diagnostic AS code_diagnostic,
           d.libelle_diagnostic AS libelle_diagnostic, d.code_categorie AS code_categorie,
           d.chapitre_cim10 AS chapitre_cim10
    FROM dim_diagnostic d
    LEFT JOIN t_dim_diagnostic s ON s.code_diagnostic = d.code_diagnostic AND s.chapitre_cim10 IS NOT NULL
    WHERE s.code_diagnostic IS NULL AND d.id_diagnostic > 0
) u;

-- ---------------------------------------------------------------------
-- [DML] DIM_PROFESSIONNEL
-- Clés reprises de silver.cle_professionnel, même logique que DIM_PATIENT.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE dim_professionnel
SELECT u.id_professionnel, u.profession, u.code_specialite, u.specialite, u.categorie_professionnelle
FROM (
    SELECT -1 AS id_professionnel, 'Inconnu' AS profession, CAST(NULL AS STRING) AS code_specialite,
           'Inconnue' AS specialite, CAST(NULL AS STRING) AS categorie_professionnelle
    FROM (SELECT max(1) AS x FROM silver.localisation) o
    UNION ALL
    SELECT c.id_professionnel AS id_professionnel, p.profession AS profession,
           p.code_specialite AS code_specialite, p.specialite AS specialite,
           p.categorie_professionnelle AS categorie_professionnelle
    FROM silver.cle_professionnel c
    LEFT JOIN silver.professionnel p ON p.id_prof_pseudo = c.id_prof_pseudo
) u;

-- =====================================================================
-- 4. [DDL+DML] TABLES DE TRAVAIL DES FAITS (temporaires)
-- Elles donnent aux faits leurs clés d'établissement et de localisation.
-- =====================================================================

-- r_etablissement : pour chaque organisation, sa clé et la clé de son
-- département (-1 si le département est introuvable)
DROP TABLE IF EXISTS r_etablissement;
CREATE TEMPORARY TABLE r_etablissement AS
SELECT d.identifiant_organisation, d.id_etablissement, nvl(l.id_localisation, -1) AS id_localisation
FROM dim_etablissement d
LEFT JOIN silver.etablissement e ON e.identifiant_organisation = d.identifiant_organisation
LEFT JOIN dim_localisation l ON l.code_departement = e.code_departement
WHERE d.id_etablissement > 0;

-- r_medecin : établissement et département de chaque médecin (fiche 4.1)
--   établissement : une organisation -> elle, plusieurs -> -2
--   département   : un seul -> lui, plusieurs différents -> -2
--   médecin sans organisation : pas de ligne ici, donc -1 dans le fait
-- count(DISTINCT) et non count(*) : une activité en double ou une ligne sans
-- organisation (exercice libéral) ne doit pas donner -2 à tort.
DROP TABLE IF EXISTS r_medecin;
CREATE TEMPORARY TABLE r_medecin AS
SELECT g.id_prof_pseudo,
       CASE WHEN g.nb_org > 1 THEN -2 ELSE nvl(g.id_etab, -1) END AS id_etablissement,
       CASE WHEN g.loc_min IS NULL THEN -1
            WHEN g.loc_min <> g.loc_max THEN -2
            ELSE g.loc_max END AS id_localisation
FROM (
    SELECT a.id_prof_pseudo,
           count(DISTINCT r.id_etablissement) AS nb_org,
           max(r.id_etablissement) AS id_etab,
           min(CASE WHEN r.id_localisation > 0 THEN r.id_localisation END) AS loc_min,
           max(CASE WHEN r.id_localisation > 0 THEN r.id_localisation END) AS loc_max
    FROM silver.activite a
    LEFT JOIN r_etablissement r ON r.identifiant_organisation = a.identifiant_organisation
    GROUP BY a.id_prof_pseudo
) g;

-- =====================================================================
-- 5. [DML] CHARGEMENT DES FAITS ET DE LEURS REJETS
-- Lignes inexploitables -> silver.rejets (partition gold_<fait>).
-- Clé étrangère : nvl(..., -1) quand la jointure ne trouve rien.
-- Faits partitionnés : PARTITION (annee) prend l'année dans la dernière
-- colonne du SELECT, DISTRIBUTE BY annee confie chaque année à une seule
-- tâche, donc peu de fichiers Parquet ouverts et moins de mémoire.
-- =====================================================================

-- ---------------------------------------------------------------------
-- FAIT_CONSULTATION (partitionné par année)
-- ---------------------------------------------------------------------
-- [DML] Rejets : numéro illisible ou heure de début absente
INSERT OVERWRITE TABLE silver.rejets PARTITION (flux = 'gold_fait_consultation')
SELECT id_execution, '20261105_1', '03_gold_partitioning', 'silver.consultation', CAST(NULL AS BIGINT),
       concat_ws('\073', num_consultation, id_patient_pseudo, id_prof_pseudo, code_diagnostic,
                         date_consultation, heure_debut),
       CASE WHEN CAST(num_consultation AS INT) IS NULL THEN 'format invalide' ELSE 'heure absente' END,
       from_unixtime(unix_timestamp())
FROM silver.consultation
WHERE CAST(num_consultation AS INT) IS NULL OR heure_debut IS NULL;

-- [DML] Chargement du fait.
-- Établissement et localisation viennent du médecin (r_medecin).
-- Créneau : Matin avant 13 h, Après-midi avant 20 h, Soir ensuite.
INSERT OVERWRITE TABLE fait_consultation PARTITION (annee)
SELECT x.num_consultation, x.id_patient, x.id_diagnostic, x.id_etablissement, x.id_temps,
       x.id_localisation, x.id_professionnel, x.age, x.nb_consultation, x.creneau, x.annee
FROM (
    SELECT CAST(c.num_consultation AS INT) AS num_consultation,
           nvl(kp.id_patient, -1) AS id_patient,
           nvl(dd.id_diagnostic, -1) AS id_diagnostic,
           nvl(rm.id_etablissement, -1) AS id_etablissement,
           c.id_temps AS id_temps,
           nvl(rm.id_localisation, -1) AS id_localisation,
           nvl(kr.id_professionnel, -1) AS id_professionnel,
           age_borne(age_revolu(p.date_naissance, c.date_consultation)) AS age,
           1 AS nb_consultation,
           CASE WHEN CAST(split(c.heure_debut, ':')[0] AS INT) < 13 THEN 'Matin'
                WHEN CAST(split(c.heure_debut, ':')[0] AS INT) < 20 THEN 'Après-midi'
                ELSE 'Soir' END AS creneau,
           annee_partition(c.id_temps) AS annee
    FROM silver.consultation c
    LEFT JOIN silver.cle_patient kp ON kp.id_patient_pseudo = c.id_patient_pseudo
    LEFT JOIN silver.patient p ON p.id_patient_pseudo = c.id_patient_pseudo
    LEFT JOIN dim_diagnostic dd ON dd.code_diagnostic = c.code_diagnostic
    LEFT JOIN r_medecin rm ON rm.id_prof_pseudo = c.id_prof_pseudo
    LEFT JOIN silver.cle_professionnel kr ON kr.id_prof_pseudo = c.id_prof_pseudo
    WHERE CAST(c.num_consultation AS INT) IS NOT NULL AND c.heure_debut IS NOT NULL
) x
DISTRIBUTE BY x.annee;

-- ---------------------------------------------------------------------
-- FAIT_HOSPITALISATION (non partitionné)
-- ---------------------------------------------------------------------
-- [DML] Rejets : numéro illisible (la durée a déjà été contrôlée en Silver)
INSERT OVERWRITE TABLE silver.rejets PARTITION (flux = 'gold_fait_hospitalisation')
SELECT id_execution, '20261105_1', '03_gold_partitioning', 'silver.hospitalisation', CAST(NULL AS BIGINT),
       concat_ws('\073', num_hospitalisation, id_patient_pseudo, identifiant_organisation,
                         code_diagnostic, date_entree),
       'format invalide', from_unixtime(unix_timestamp())
FROM silver.hospitalisation
WHERE CAST(num_hospitalisation AS INT) IS NULL;

-- [DML] Chargement du fait.
-- Établissement et localisation viennent de l'organisation (r_etablissement).
INSERT OVERWRITE TABLE fait_hospitalisation
SELECT x.num_hospitalisation, x.id_patient, x.id_diagnostic, x.id_etablissement, x.id_temps,
       x.id_localisation, x.age, x.nb_hospitalisation, x.duree_sejour_jours, x.annee
FROM (
    SELECT CAST(h.num_hospitalisation AS INT) AS num_hospitalisation,
           nvl(kp.id_patient, -1) AS id_patient,
           nvl(dd.id_diagnostic, -1) AS id_diagnostic,
           nvl(re.id_etablissement, -1) AS id_etablissement,
           h.id_temps AS id_temps,
           nvl(re.id_localisation, -1) AS id_localisation,
           age_borne(age_revolu(p.date_naissance, h.date_entree)) AS age,
           1 AS nb_hospitalisation,
           h.duree_sejour_jours AS duree_sejour_jours,
           annee_partition(h.id_temps) AS annee
    FROM silver.hospitalisation h
    LEFT JOIN silver.cle_patient kp ON kp.id_patient_pseudo = h.id_patient_pseudo
    LEFT JOIN silver.patient p ON p.id_patient_pseudo = h.id_patient_pseudo
    LEFT JOIN dim_diagnostic dd ON dd.code_diagnostic = h.code_diagnostic
    LEFT JOIN r_etablissement re ON re.identifiant_organisation = h.identifiant_organisation
    WHERE CAST(h.num_hospitalisation AS INT) IS NOT NULL
) x;

-- ---------------------------------------------------------------------
-- [DML] FAIT_SATISFACTION (non partitionné, aucun rejet)
-- Les notes passent de DOUBLE à DECIMAL(5,2).
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE fait_satisfaction
SELECT x.id_etablissement, x.id_temps, x.id_localisation, x.classement, x.note_satisfaction, x.nb_avis,
       x.score_accueil, x.score_pec_infirmier, x.score_pec_medical, x.score_chambre, x.score_repas,
       x.score_sortie, x.taux_recommandation, x.nb_reco, x.annee
FROM (
    SELECT nvl(re.id_etablissement, -1) AS id_etablissement,
           s.id_temps AS id_temps,
           nvl(re.id_localisation, -1) AS id_localisation,
           s.classement AS classement,
           CAST(s.note_satisfaction AS DECIMAL(5,2)) AS note_satisfaction,
           s.nb_avis AS nb_avis,
           CAST(s.score_accueil AS DECIMAL(5,2)) AS score_accueil,
           CAST(s.score_pecinf AS DECIMAL(5,2)) AS score_pec_infirmier,
           CAST(s.score_pecmed AS DECIMAL(5,2)) AS score_pec_medical,
           CAST(s.score_chambre AS DECIMAL(5,2)) AS score_chambre,
           CAST(s.score_repas AS DECIMAL(5,2)) AS score_repas,
           CAST(s.score_sortie AS DECIMAL(5,2)) AS score_sortie,
           CAST(s.taux_recommandation AS DECIMAL(5,2)) AS taux_recommandation,
           s.nb_reco AS nb_reco,
           annee_partition(s.id_temps) AS annee
    FROM silver.satisfaction s
    LEFT JOIN r_etablissement re ON re.identifiant_organisation = s.identifiant_organisation
) x;

SET mapreduce.job.reduces=30;

-- ---------------------------------------------------------------------
-- [DML] FAIT_DECES (partitionné par année, aucun rejet : département déjà
-- contrôlé en Silver)
-- Âge : années révolues, à défaut année du décès - année de naissance.
-- [CONFIG] SET ci-dessus : 30 réducteurs se partagent les ~140 années, chacun
-- garde peu de fichiers Parquet ouverts. Remis à -1 (choix de Hive) ensuite.
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE fait_deces PARTITION (annee)
SELECT x.num_deces, x.id_temps, x.id_localisation, x.sexe, x.age, x.nb_deces, x.annee
FROM (
    SELECT d.num_deces AS num_deces,
           d.id_temps AS id_temps,
           nvl(l.id_localisation, -1) AS id_localisation,
           d.sexe AS sexe,
           age_borne(coalesce(age_revolu(d.date_naissance, d.date_deces),
                              d.annee_deces - d.annee_naissance)) AS age,
           1 AS nb_deces,
           annee_partition(d.id_temps) AS annee
    FROM silver.deces d
    LEFT JOIN dim_localisation l ON l.code_departement = d.code_departement
) x
DISTRIBUTE BY x.annee;

SET mapreduce.job.reduces=-1;

-- =====================================================================
-- 6. [DML] AUDIT, puis [CONTRÔLE] VÉRIFICATIONS AFFICHÉES
-- =====================================================================

-- [DML] silver.audit : une ligne par flux (INSERT INTO garde l'historique).
--   faits : lues = écrites + rejetées, sinon ECHEC_CONTROLE
--   dimensions : écart négatif possible (entités disparues conservées)
--   plus de 5 % de rejets : ECHEC_SEUIL
INSERT INTO TABLE silver.audit
SELECT '20261105_1', l.id_execution, '03_gold_partitioning', l.flux, from_unixtime(unix_timestamp()),
       l.lues, e.ecrites, nvl(r.rejetees, 0),
       l.lues - e.ecrites - nvl(r.rejetees, 0),
       CAST(0 AS BIGINT),
       round(nvl(r.rejetees, 0) / l.lues * 100, 2),
       CASE WHEN nvl(r.rejetees, 0) > 0.05 * l.lues THEN 'ECHEC_SEUIL'
            WHEN l.flux LIKE 'fait_%' AND l.lues <> e.ecrites + nvl(r.rejetees, 0) THEN 'ECHEC_CONTROLE'
            ELSE 'OK' END
FROM (
              SELECT 'dim_localisation'     AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.localisation
    UNION ALL SELECT 'dim_etablissement'    AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.etablissement
    UNION ALL SELECT 'dim_patient'          AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.patient
    UNION ALL SELECT 'dim_diagnostic'       AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.diagnostic
    UNION ALL SELECT 'dim_professionnel'    AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.professionnel
    UNION ALL SELECT 'fait_consultation'    AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.consultation
    UNION ALL SELECT 'fait_hospitalisation' AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.satisfaction
    UNION ALL SELECT 'fait_deces'           AS flux, max(id_execution) AS id_execution, count(*) AS lues FROM silver.deces
) l
JOIN (
              SELECT 'dim_localisation'     AS flux, count(*) AS ecrites FROM dim_localisation  WHERE id_localisation > 0
    UNION ALL SELECT 'dim_etablissement'    AS flux, count(*) AS ecrites FROM dim_etablissement WHERE id_etablissement > 0
    UNION ALL SELECT 'dim_patient'          AS flux, count(*) AS ecrites FROM dim_patient       WHERE id_patient > 0
    UNION ALL SELECT 'dim_diagnostic'       AS flux, count(*) AS ecrites FROM dim_diagnostic    WHERE id_diagnostic > 0
    UNION ALL SELECT 'dim_professionnel'    AS flux, count(*) AS ecrites FROM dim_professionnel WHERE id_professionnel > 0
    UNION ALL SELECT 'fait_consultation'    AS flux, count(*) AS ecrites FROM fait_consultation
    UNION ALL SELECT 'fait_hospitalisation' AS flux, count(*) AS ecrites FROM fait_hospitalisation
    UNION ALL SELECT 'fait_satisfaction'    AS flux, count(*) AS ecrites FROM fait_satisfaction
    UNION ALL SELECT 'fait_deces'           AS flux, count(*) AS ecrites FROM fait_deces
) e ON e.flux = l.flux
LEFT JOIN (
    SELECT flux, count(*) AS rejetees FROM silver.rejets WHERE flux LIKE 'gold_%' GROUP BY flux
) r ON r.flux = concat('gold_', l.flux);

-- [DML] gold.controle_membres : lignes à -1 / -2 et âges NULL par fait
INSERT INTO TABLE controle_membres
SELECT '20261105_1', from_unixtime(unix_timestamp()), u.fait, u.nb_lignes, u.patient_m1, u.diagnostic_m1,
       u.etablissement_m1, u.etablissement_m2, u.temps_m1, u.localisation_m1, u.localisation_m2,
       u.professionnel_m1, u.age_null
FROM (
    SELECT 'fait_consultation' AS fait, count(*) AS nb_lignes,
           sum(CASE WHEN id_patient = -1 THEN 1 ELSE 0 END) AS patient_m1,
           sum(CASE WHEN id_diagnostic = -1 THEN 1 ELSE 0 END) AS diagnostic_m1,
           sum(CASE WHEN id_etablissement = -1 THEN 1 ELSE 0 END) AS etablissement_m1,
           sum(CASE WHEN id_etablissement = -2 THEN 1 ELSE 0 END) AS etablissement_m2,
           sum(CASE WHEN id_temps = -1 THEN 1 ELSE 0 END) AS temps_m1,
           sum(CASE WHEN id_localisation = -1 THEN 1 ELSE 0 END) AS localisation_m1,
           sum(CASE WHEN id_localisation = -2 THEN 1 ELSE 0 END) AS localisation_m2,
           sum(CASE WHEN id_professionnel = -1 THEN 1 ELSE 0 END) AS professionnel_m1,
           sum(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS age_null
    FROM fait_consultation
    UNION ALL
    SELECT 'fait_hospitalisation' AS fait, count(*) AS nb_lignes,
           sum(CASE WHEN id_patient = -1 THEN 1 ELSE 0 END) AS patient_m1,
           sum(CASE WHEN id_diagnostic = -1 THEN 1 ELSE 0 END) AS diagnostic_m1,
           sum(CASE WHEN id_etablissement = -1 THEN 1 ELSE 0 END) AS etablissement_m1,
           sum(CASE WHEN id_etablissement = -2 THEN 1 ELSE 0 END) AS etablissement_m2,
           sum(CASE WHEN id_temps = -1 THEN 1 ELSE 0 END) AS temps_m1,
           sum(CASE WHEN id_localisation = -1 THEN 1 ELSE 0 END) AS localisation_m1,
           sum(CASE WHEN id_localisation = -2 THEN 1 ELSE 0 END) AS localisation_m2,
           CAST(0 AS BIGINT) AS professionnel_m1,
           sum(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS age_null
    FROM fait_hospitalisation
    UNION ALL
    SELECT 'fait_satisfaction' AS fait, count(*) AS nb_lignes,
           CAST(0 AS BIGINT) AS patient_m1,
           CAST(0 AS BIGINT) AS diagnostic_m1,
           sum(CASE WHEN id_etablissement = -1 THEN 1 ELSE 0 END) AS etablissement_m1,
           sum(CASE WHEN id_etablissement = -2 THEN 1 ELSE 0 END) AS etablissement_m2,
           sum(CASE WHEN id_temps = -1 THEN 1 ELSE 0 END) AS temps_m1,
           sum(CASE WHEN id_localisation = -1 THEN 1 ELSE 0 END) AS localisation_m1,
           sum(CASE WHEN id_localisation = -2 THEN 1 ELSE 0 END) AS localisation_m2,
           CAST(0 AS BIGINT) AS professionnel_m1,
           CAST(0 AS BIGINT) AS age_null
    FROM fait_satisfaction
    UNION ALL
    SELECT 'fait_deces' AS fait, count(*) AS nb_lignes,
           CAST(0 AS BIGINT) AS patient_m1,
           CAST(0 AS BIGINT) AS diagnostic_m1,
           CAST(0 AS BIGINT) AS etablissement_m1,
           CAST(0 AS BIGINT) AS etablissement_m2,
           sum(CASE WHEN id_temps = -1 THEN 1 ELSE 0 END) AS temps_m1,
           sum(CASE WHEN id_localisation = -1 THEN 1 ELSE 0 END) AS localisation_m1,
           sum(CASE WHEN id_localisation = -2 THEN 1 ELSE 0 END) AS localisation_m2,
           CAST(0 AS BIGINT) AS professionnel_m1,
           sum(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS age_null
    FROM fait_deces
) u;

-- ---------------------------------------------------------------------
-- [CONTRÔLE] Vérifications affichées (lecture seule)
-- ---------------------------------------------------------------------
-- Bilan de l'audit par table
SELECT flux, lignes_lues, lignes_ecrites, lignes_rejetees, lignes_doublon, taux_rejet, statut
FROM silver.audit
WHERE id_silver = '20261105_1'
ORDER BY flux;

-- Membres -1 et -2 dans les faits
SELECT fait, nb_lignes, patient_m1, diagnostic_m1, etablissement_m1, etablissement_m2,
       temps_m1, localisation_m1, localisation_m2, professionnel_m1, age_null
FROM controle_membres
WHERE id_gold = '20261105_1'
ORDER BY fait;

-- Calendrier : 55 153 lignes attendues (55 152 jours + le membre -1)
SELECT count(*) AS nb_lignes, min(id_temps) AS id_min, max(id_temps) AS id_max,
       sum(CASE WHEN est_weekend THEN 1 ELSE 0 END) AS jours_weekend
FROM dim_temps;

-- Dates repères : le 15/03/2019 est un vendredi, le 01/01/1880 un jeudi
SELECT id_temps, date_complete, jour_semaine, libelle_jour, semaine, libelle_mois, trimestre, est_weekend
FROM dim_temps
WHERE id_temps IN (20190315, 18800101, 20301231)
ORDER BY id_temps;

-- Dates des faits absentes du calendrier : 0 attendu partout
SELECT c.fait, c.orphelins
FROM (
    SELECT 'fait_consultation' AS fait, count(*) AS orphelins
    FROM fait_consultation f LEFT JOIN dim_temps t ON t.id_temps = f.id_temps WHERE t.id_temps IS NULL
    UNION ALL
    SELECT 'fait_hospitalisation' AS fait, count(*) AS orphelins
    FROM fait_hospitalisation f LEFT JOIN dim_temps t ON t.id_temps = f.id_temps WHERE t.id_temps IS NULL
    UNION ALL
    SELECT 'fait_satisfaction' AS fait, count(*) AS orphelins
    FROM fait_satisfaction f LEFT JOIN dim_temps t ON t.id_temps = f.id_temps WHERE t.id_temps IS NULL
    UNION ALL
    SELECT 'fait_deces' AS fait, count(*) AS orphelins
    FROM fait_deces f LEFT JOIN dim_temps t ON t.id_temps = f.id_temps WHERE t.id_temps IS NULL
) c;

-- Verdict : assert_true fait échouer beeline si un flux n'est pas OK
SELECT assert_true(count(*) = 0) AS gold_conforme
FROM silver.audit
WHERE id_silver = '20261105_1' AND statut <> 'OK';

-- =====================================================================
-- 7. [STATS] STATISTIQUES
-- Les mêmes dans gold, gold_flat et gold_bucketed : le benchmark compare
-- alors le modèle physique, pas les statistiques.
-- Tables partitionnées : statistiques de base, puis de colonnes.
-- =====================================================================
ANALYZE TABLE dim_temps         COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_localisation  COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_etablissement COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_patient       COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_diagnostic    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE dim_professionnel COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_consultation    PARTITION (annee) COMPUTE STATISTICS;
ANALYZE TABLE fait_consultation    PARTITION (annee) COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_hospitalisation COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_satisfaction    COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE fait_deces           PARTITION (annee) COMPUTE STATISTICS;
ANALYZE TABLE fait_deces           PARTITION (annee) COMPUTE STATISTICS FOR COLUMNS;
