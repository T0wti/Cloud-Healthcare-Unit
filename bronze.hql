-- =====================================================================
-- Projet CHU - Étape 1 : bases Hive et tables externes sur le Bronze
-- Exécution : beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 01_bronze.hql
--
-- Les tables pointent sur les fichiers déposés par les jobs Talend J10 à J15 :
-- séparateur ';', sans en-tête, sans guillemets, UTF-8.
-- Toutes les colonnes sont en STRING : le typage se fait au passage en Silver.
-- Tables EXTERNAL : un DROP TABLE ne supprime pas les fichiers de HDFS.
-- =====================================================================

-- /silver et /gold doivent exister et appartenir à cloudera (comme /bronze) :
--   sudo -u hdfs hdfs dfs -mkdir -p /silver /gold
--   sudo -u hdfs hdfs dfs -chown cloudera:cloudera /silver /gold
-- La base bronze n'a pas d'emplacement propre : ses tables externes pointent sur /bronze.
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Staging brut : copie des extractions Talend';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Staging nettoyé : typé, dédoublonné, normalisé' LOCATION '/silver';
CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Entrepôt décisionnel : constellation, Parquet'  LOCATION '/gold';

USE bronze;

-- ---------------------------------------------------------------------
-- Source 1 : PostgreSQL (job J10_ING_POSTGRES)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS patient;
CREATE EXTERNAL TABLE patient (
    id_patient_pseudo  STRING COMMENT 'HMAC-SHA256 de Id_patient',
    sexe               STRING,
    date_naissance     STRING COMMENT 'Format source M/J/AAAA',
    id_execution       STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/postgres/patient';

DROP TABLE IF EXISTS consultation;
CREATE EXTERNAL TABLE consultation (
    num_consultation   STRING,
    id_patient_pseudo  STRING,
    id_prof_pseudo     STRING,
    code_diag          STRING,
    date_consultation  STRING COMMENT 'Format source AAAA-MM-JJ',
    heure_debut        STRING,
    id_execution       STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/postgres/consultation';

DROP TABLE IF EXISTS professionnel;
CREATE EXTERNAL TABLE professionnel (
    id_prof_pseudo             STRING COMMENT 'HMAC-SHA256 de Identifiant',
    profession                 STRING,
    categorie_professionnelle  STRING,
    code_specialite            STRING,
    id_execution               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/postgres/professionnel';

DROP TABLE IF EXISTS specialites;
CREATE EXTERNAL TABLE specialites (
    code_specialite  STRING,
    specialite       STRING,
    id_execution     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/postgres/specialites';

DROP TABLE IF EXISTS diagnostics;
CREATE EXTERNAL TABLE diagnostics (
    code_diag     STRING COMMENT 'Code CIM-10',
    diagnostic    STRING,
    id_execution  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/postgres/diagnostics';

-- ---------------------------------------------------------------------
-- Source 1b : Hospitalisations.csv (job J11_ING_HOSPITALISATIONS)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS hospitalisations;
CREATE EXTERNAL TABLE hospitalisations (
    num_hospitalisation       STRING,
    id_patient_pseudo         STRING,
    identifiant_organisation  STRING,
    code_diagnostic           STRING,
    date_entree               STRING COMMENT 'Format source JJ/MM/AAAA',
    jour_hospitalisation      STRING COMMENT 'Durée du séjour en jours',
    id_execution              STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/hospitalisations';

-- ---------------------------------------------------------------------
-- Source 2 : référentiel FINESS (job J12_ING_REFERENTIEL)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS etablissement;
CREATE EXTERNAL TABLE etablissement (
    identifiant_organisation        STRING,
    finess_etablissement_juridique  STRING,
    raison_sociale_site             STRING,
    code_commune                    STRING,
    commune                         STRING,
    code_postal                     STRING,
    id_execution                    STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/referentiel/etablissement';

DROP TABLE IF EXISTS activite;
CREATE EXTERNAL TABLE activite (
    id_prof_pseudo            STRING,
    identifiant_organisation  STRING,
    id_execution              STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/referentiel/activite';

-- ---------------------------------------------------------------------
-- Source 3 : e-Satis HAS (job J13_ING_ESATIS)
-- Une seule table sur le dossier : esatis_2017.csv, esatis_2019.csv et
-- esatis_2020.csv ont les mêmes colonnes, annee_recueil les distingue.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS esatis;
CREATE EXTERNAL TABLE esatis (
    id_execution                STRING,
    finess                      STRING,
    finess_geo                  STRING,
    rs_finess_geo               STRING,
    region                      STRING,
    classement                  STRING,
    nb_rep_score_all_rea_ajust  STRING,
    score_all_rea_ajust         STRING COMMENT 'Virgule décimale possible (2019)',
    score_accueil_rea_ajust     STRING,
    score_pecinf_rea_ajust      STRING,
    score_pecmed_rea_ajust      STRING,
    score_chambre_rea_ajust     STRING,
    score_repas_rea_ajust       STRING,
    score_sortie_rea_ajust      STRING,
    taux_reco_brut              STRING COMMENT 'Virgule décimale possible (2019)',
    nb_reco_brut                STRING,
    annee_recueil               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/esatis';

-- ---------------------------------------------------------------------
-- Source 4 : décès INSEE (job J14_ING_DECES)
-- Talend n'extrait aucune identité (ni nom, ni prénoms, ni lieu de naissance).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS deces;
CREATE EXTERNAL TABLE deces (
    id_execution         STRING,
    sexe                 STRING,
    date_naissance       STRING,
    date_deces           STRING COMMENT 'Format source AAAA-MM-JJ',
    code_lieu_deces      STRING COMMENT 'Code commune INSEE',
    numero_acte_deces    STRING COMMENT 'Numéroté par commune et par année'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/deces';

-- ---------------------------------------------------------------------
-- Source 5 : COG INSEE (job J15_ING_COG)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS cog_departement;
CREATE EXTERNAL TABLE cog_departement (
    dep           STRING,
    reg           STRING,
    libelle       STRING,
    id_execution  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/cog/departement';

DROP TABLE IF EXISTS cog_region;
CREATE EXTERNAL TABLE cog_region (
    reg           STRING,
    libelle       STRING,
    id_execution  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/bronze/cog/region';

-- ---------------------------------------------------------------------
-- Contrôles : nombre de lignes par table (à comparer aux sources)
-- ---------------------------------------------------------------------
SELECT 'patient'          AS table_bronze, COUNT(*) AS nb_lignes FROM patient
UNION ALL SELECT 'consultation' AS table_bronze,     COUNT(*) AS nb_lignes FROM consultation
UNION ALL SELECT 'professionnel' AS table_bronze,    COUNT(*) AS nb_lignes FROM professionnel
UNION ALL SELECT 'specialites' AS table_bronze,      COUNT(*) AS nb_lignes FROM specialites
UNION ALL SELECT 'diagnostics' AS table_bronze,      COUNT(*) AS nb_lignes FROM diagnostics
UNION ALL SELECT 'hospitalisations' AS table_bronze, COUNT(*) AS nb_lignes FROM hospitalisations
UNION ALL SELECT 'etablissement' AS table_bronze,    COUNT(*) AS nb_lignes FROM etablissement
UNION ALL SELECT 'activite' AS table_bronze,         COUNT(*) AS nb_lignes FROM activite
UNION ALL SELECT 'esatis' AS table_bronze,           COUNT(*) AS nb_lignes FROM esatis
UNION ALL SELECT 'deces' AS table_bronze,            COUNT(*) AS nb_lignes FROM deces
UNION ALL SELECT 'cog_departement' AS table_bronze,  COUNT(*) AS nb_lignes FROM cog_departement
UNION ALL SELECT 'cog_region' AS table_bronze,       COUNT(*) AS nb_lignes FROM cog_region;

-- Lignes mal découpées : un ';' dans un texte décale les colonnes.
-- La dernière colonne (id_execution ou code) doit toujours être remplie.
SELECT 'etablissement' AS table_bronze, COUNT(*) AS lignes_suspectes FROM etablissement WHERE id_execution IS NULL OR id_execution = ''
UNION ALL SELECT 'hospitalisations' AS table_bronze, COUNT(*) AS lignes_suspectes FROM hospitalisations WHERE id_execution IS NULL OR id_execution = ''
UNION ALL SELECT 'deces' AS table_bronze,            COUNT(*) AS lignes_suspectes FROM deces WHERE numero_acte_deces IS NULL
UNION ALL SELECT 'esatis' AS table_bronze,           COUNT(*) AS lignes_suspectes FROM esatis WHERE annee_recueil IS NULL OR annee_recueil = '';

SELECT annee_recueil, COUNT(*) AS nb_etablissements FROM esatis GROUP BY annee_recueil;
