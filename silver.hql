SET hive.mapred.mode=nonstrict;
SET hive.auto.convert.join=true;
SET parquet.compression=SNAPPY;
SET parquet.block.size=33554432;
SET hive.execution.engine=spark;
SET spark.executor.instances=1;
SET spark.executor.cores=1;
SET spark.executor.memory=1g;
SET spark.yarn.executor.memoryOverhead=384;
SET spark.driver.memory=512m;
SET spark.yarn.driver.memoryOverhead=384;
-- =====================================================================
-- Les SET restent en tête, sans commentaire avant eux : sinon Hue les
-- envoie au parseur SQL et Hive répond « missing KW_ROLE ».
-- Moteur : Hive on Spark (Hive -> Configuration -> Spark On YARN Service
-- = Spark dans Cloudera Manager). Pour revenir à MapReduce, remplacer les
-- six SET spark par : SET hive.execution.engine=mr;
--
-- Projet CHU - Étape 2 : du Bronze au Silver
-- Exécution : éditeur Hive de Hue, ou
--   beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 02_silver.hql
-- Identifiant d'exécution (id_silver) : '20261102_1', écrit en dur.
-- À changer à chaque nouvelle exécution (remplacer partout) pour que
-- l'audit et le contrôle du seuil ne mélangent pas deux exécutions.
--
-- Règles appliquées (livrable, fiches des jobs 2.2 à 2.7) :
--   typage      : dates selon le format de chaque source, virgule décimale -> point
--   normalisation : textes sans espaces de bord, chaîne vide = absente,
--                 FINESS sur 9 caractères, code commune sur 5, département déduit
--   unicité     : doublon exact supprimé, même clé + contenu différent -> rejet
--   rejets      : silver.rejets (une partition par flux), jamais de suppression silencieuse
--   audit       : silver.audit, lues = écrites + rejetées + doublons + ignorées
--   seuil       : plus de 5 % de rejets sur un flux -> le script échoue en fin
--                 d'exécution, le Gold ne doit pas être lancé.
--
-- Le Silver est recalculé entièrement à partir du Bronze à chaque exécution.
-- Les dates sont gardées en chaîne AAAA-MM-JJ (Hive 1.1 n'a pas de DATE en
-- Parquet) et accompagnées de leur id_temps AAAAMMJJ, -1 si absente ou invalide.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Fonctions de nettoyage (valables pour la session)
-- ---------------------------------------------------------------------
-- Texte : espaces de bord retirés, chaîne vide -> NULL
CREATE TEMPORARY MACRO txt(s STRING)
    CASE WHEN trim(s) = '' THEN NULL ELSE trim(s) END;

-- Date AAAA-MM-JJ réelle (le 30/02 est refusé), sinon NULL
CREATE TEMPORARY MACRO iso_valide(s STRING)
    CASE WHEN s RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          AND from_unixtime(unix_timestamp(s, 'yyyy-MM-dd'), 'yyyy-MM-dd') = s
         THEN s END;

-- JJ/MM/AAAA -> AAAA-MM-JJ (Hospitalisations.csv)
CREATE TEMPORARY MACRO jma_vers_iso(s STRING)
    CASE WHEN trim(s) RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
         THEN concat(split(trim(s), '/')[2], '-',
                     lpad(split(trim(s), '/')[1], 2, '0'), '-',
                     lpad(split(trim(s), '/')[0], 2, '0')) END;

-- M/J/AAAA -> AAAA-MM-JJ (PostgreSQL Patient)
CREATE TEMPORARY MACRO mja_vers_iso(s STRING)
    CASE WHEN trim(s) RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
         THEN concat(split(trim(s), '/')[2], '-',
                     lpad(split(trim(s), '/')[0], 2, '0'), '-',
                     lpad(split(trim(s), '/')[1], 2, '0')) END;

-- AAAA-MM-JJ -> AAAAMMJJ, -1 si absente
CREATE TEMPORARY MACRO vers_id_temps(iso STRING)
    CASE WHEN iso IS NULL THEN -1 ELSE CAST(regexp_replace(iso, '-', '') AS INT) END;

-- Code commune ou postal sur 5 caractères (zéro de tête perdu par Excel)
CREATE TEMPORARY MACRO code5(c STRING)
    CASE WHEN upper(trim(c)) RLIKE '^[0-9][0-9AB][0-9]{2,3}$'
         THEN lpad(upper(trim(c)), 5, '0') END;

-- Département d'un code commune (règle 2.4)
CREATE TEMPORARY MACRO dep_commune(c STRING)
    CASE WHEN c IS NULL THEN NULL
         WHEN c LIKE '99%' THEN '99'
         WHEN c LIKE '97%' OR c LIKE '98%' THEN substr(c, 1, 3)
         WHEN upper(substr(c, 1, 2)) IN ('2A', '2B') THEN upper(substr(c, 1, 2))
         ELSE substr(c, 1, 2) END;

-- Département d'un code postal (à défaut de code commune)
CREATE TEMPORARY MACRO dep_postal(cp STRING)
    CASE WHEN cp IS NULL THEN NULL
         WHEN cp LIKE '97%' OR cp LIKE '98%' THEN substr(cp, 1, 3)
         WHEN cp LIKE '20%' THEN CASE WHEN cp < '20200' THEN '2A' ELSE '2B' END
         ELSE substr(cp, 1, 2) END;

-- Code CIM-10 : majuscules, sans espace
CREATE TEMPORARY MACRO code_cim(s STRING)
    CASE WHEN regexp_replace(upper(s), '\\s', '') = '' THEN NULL
         ELSE regexp_replace(upper(s), '\\s', '') END;

-- Nombre avec virgule ou point décimal, NULL si vide ou illisible
CREATE TEMPORARY MACRO num(s STRING)
    CAST(regexp_replace(trim(s), ',', '.') AS DOUBLE);

-- 1 si le texte est présent mais n'est pas un nombre
CREATE TEMPORARY MACRO num_ko(s STRING)
    CASE WHEN trim(s) <> '' AND CAST(regexp_replace(trim(s), ',', '.') AS DOUBLE) IS NULL
         THEN 1 ELSE 0 END;

-- 1 si une note est hors 0 à 100
CREATE TEMPORARY MACRO hors_0_100(x DOUBLE)
    CASE WHEN x < 0 OR x > 100 THEN 1 ELSE 0 END;

USE silver;

-- ---------------------------------------------------------------------
-- Tables Silver (Parquet, recalculées à chaque exécution)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS localisation;
CREATE TABLE localisation (
    code_departement  STRING,
    departement       STRING,
    code_region       STRING,
    region            STRING,
    origine           STRING COMMENT 'COG ou ajout (20, COM, 99)',
    id_execution      STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS specialite;
CREATE TABLE specialite (
    code_specialite  STRING,
    specialite       STRING,
    id_execution     STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS diagnostic;
CREATE TABLE diagnostic (
    code_diagnostic     STRING,
    libelle_diagnostic  STRING,
    code_categorie      STRING COMMENT '3 premiers caractères',
    id_execution        STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS professionnel;
CREATE TABLE professionnel (
    id_prof_pseudo             STRING,
    profession                 STRING,
    categorie_professionnelle  STRING,
    code_specialite            STRING,
    specialite                 STRING,
    id_execution               STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    id_patient_pseudo  STRING,
    sexe               STRING COMMENT 'F, M ou I',
    date_naissance     STRING COMMENT "AAAA-MM-JJ, sert au calcul de l'âge",
    annee_naissance    INT,
    id_execution       STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS consultation;
CREATE TABLE consultation (
    num_consultation   STRING,
    id_patient_pseudo  STRING,
    id_prof_pseudo     STRING,
    code_diagnostic    STRING,
    date_consultation  STRING,
    id_temps           INT,
    heure_debut        STRING,
    id_execution       STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS hospitalisation;
CREATE TABLE hospitalisation (
    num_hospitalisation       STRING,
    id_patient_pseudo         STRING,
    identifiant_organisation  STRING,
    code_diagnostic           STRING,
    date_entree               STRING,
    id_temps                  INT,
    duree_sejour_jours        INT,
    id_execution              STRING
) STORED AS PARQUET;

DROP TABLE IF EXISTS satisfaction;
CREATE TABLE satisfaction (
    identifiant_organisation  STRING COMMENT 'F + finess_geo',
    finess_geo                STRING,
    num_finess_juridique      STRING,
    raison_sociale            STRING,
    region                    STRING,
    annee                     INT,
    id_temps                  INT COMMENT 'AAAA0101',
    classement                STRING,
    note_satisfaction         DOUBLE,
    nb_avis                   INT,
    score_accueil             DOUBLE,
    score_pecinf              DOUBLE,
    score_pecmed              DOUBLE,
    score_chambre             DOUBLE,
    score_repas               DOUBLE,
    score_sortie              DOUBLE,
    taux_recommandation       DOUBLE,
    nb_reco                   INT,
    id_execution              STRING
) STORED AS PARQUET;

-- Sert aussi de table de rattachement établissement -> département
DROP TABLE IF EXISTS etablissement;
CREATE TABLE etablissement (
    identifiant_organisation  STRING,
    type_structure            STRING,
    num_finess_geo            STRING,
    num_finess_juridique      STRING,
    raison_sociale            STRING,
    code_commune              STRING,
    commune                   STRING,
    code_postal               STRING,
    code_departement          STRING COMMENT 'NULL si introuvable dans le COG',
    origine                   STRING COMMENT 'referentiel ou esatis',
    id_execution              STRING
) STORED AS PARQUET;

-- Couples (médecin, organisation) dédoublonnés
DROP TABLE IF EXISTS activite;
CREATE TABLE activite (
    id_prof_pseudo            STRING,
    identifiant_organisation  STRING,
    id_execution              STRING
) STORED AS PARQUET;

-- Sans identité : Talend n'extrait ni nom, ni prénoms, ni lieu de naissance
DROP TABLE IF EXISTS deces;
CREATE TABLE deces (
    num_deces         BIGINT,
    sexe              STRING COMMENT 'M, F ou I',
    date_naissance    STRING COMMENT 'AAAA-MM-JJ, NULL si incomplète',
    annee_naissance   INT    COMMENT "Pour l'âge quand la date est incomplète",
    date_deces        STRING,
    id_temps          INT,
    code_lieu_deces   STRING,
    code_departement  STRING COMMENT 'NULL si introuvable dans le COG',
    id_execution      STRING
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS rejets (
    id_execution  STRING,
    id_silver     STRING,
    job           STRING,
    fichier       STRING,
    pos_octet     BIGINT COMMENT 'Position de la ligne dans le fichier Bronze',
    contenu       STRING,
    motif         STRING,
    date_rejet    STRING
) PARTITIONED BY (flux STRING) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS audit (
    id_silver        STRING,
    id_execution     STRING,
    job              STRING,
    flux             STRING,
    date_execution   STRING,
    lignes_lues      BIGINT,
    lignes_ecrites   BIGINT,
    lignes_rejetees  BIGINT,
    lignes_doublon   BIGINT,
    lignes_ignorees  BIGINT,
    taux_rejet       DOUBLE COMMENT 'En %',
    statut           STRING COMMENT 'OK ou ECHEC_SEUIL'
) STORED AS PARQUET;

-- =====================================================================
-- Méthode commune à chaque flux :
--   t_<flux> : lignes typées et normalisées, motif de rejet éventuel,
--              empreinte = contenu hors clé
--   d_<flux> : lignes valides, numérotées par clé ; emp_min <> emp_max
--              signale une même clé avec des contenus différents
-- =====================================================================

-- ---------------------------------------------------------------------
-- LOCALISATION (COG départements + régions)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_localisation;
CREATE TEMPORARY TABLE t_localisation AS
SELECT d.id_execution, d.fichier, d.pos_octet, d.contenu,
       d.code_departement, d.departement, d.code_region,
       r.region,
       CASE WHEN d.code_departement IS NULL THEN 'cle absente'
            WHEN d.departement IS NULL      THEN 'libelle absent'
            WHEN r.region IS NULL           THEN 'region introuvable' END AS motif,
       concat_ws('|', nvl(d.departement, ''), nvl(d.code_region, ''), nvl(r.region, '')) AS empreinte
FROM (
    -- Les fichiers INSEE sont entre guillemets : ils sont retirés ici
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', dep, reg, libelle) AS contenu,
           upper(txt(regexp_replace(dep, '"', ''))) AS code_departement,
           txt(regexp_replace(libelle, '"', ''))    AS departement,
           txt(regexp_replace(reg, '"', ''))        AS code_region
    FROM bronze.cog_departement
) d
LEFT JOIN (
    SELECT txt(regexp_replace(reg, '"', ''))     AS code_region,
           txt(regexp_replace(libelle, '"', '')) AS region
    FROM bronze.cog_region
) r ON r.code_region = d.code_region;

DROP TABLE IF EXISTS d_localisation;
CREATE TEMPORARY TABLE d_localisation AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY code_departement ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY code_departement) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY code_departement) AS emp_max
FROM t_localisation t
WHERE t.motif IS NULL;

-- Valeurs absentes du COG : Corse (ancien code), collectivités d'outre-mer, étranger
INSERT OVERWRITE TABLE localisation
SELECT u.code_departement, u.departement, u.code_region, u.region, u.origine, u.id_execution
FROM (
    SELECT code_departement, departement, code_region, region, 'COG' AS origine, id_execution
    FROM d_localisation
    WHERE emp_min = emp_max AND rn = 1
    UNION ALL
    SELECT s.code_departement, s.departement, s.code_region, s.region, 'ajout' AS origine, dual.id_execution
    FROM (SELECT max(id_execution) AS id_execution FROM bronze.cog_departement) dual
    LATERAL VIEW stack(9,
        '20',  'Corse (ancien code)',                          '94',  'Corse',
        '975', 'Saint-Pierre-et-Miquelon',                     'COM', "Collectivités d'outre-mer",
        '977', 'Saint-Barthélemy',                             'COM', "Collectivités d'outre-mer",
        '978', 'Saint-Martin',                                 'COM', "Collectivités d'outre-mer",
        '984', 'Terres australes et antarctiques françaises',  'COM', "Collectivités d'outre-mer",
        '986', 'Wallis-et-Futuna',                             'COM', "Collectivités d'outre-mer",
        '987', 'Polynésie française',                          'COM', "Collectivités d'outre-mer",
        '988', 'Nouvelle-Calédonie',                           'COM', "Collectivités d'outre-mer",
        '99',  'Étranger',                                     '99',  'Étranger'
    ) s AS code_departement, departement, code_region, region
) u;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'localisation')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_localisation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_localisation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- SPECIALITE
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_specialite;
CREATE TEMPORARY TABLE t_specialite AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws(';', code_specialite, specialite) AS contenu,
       txt(code_specialite) AS code_specialite,
       txt(specialite)      AS specialite,
       CASE WHEN txt(code_specialite) IS NULL THEN 'cle absente' END AS motif,
       nvl(txt(specialite), '') AS empreinte
FROM bronze.specialites;

DROP TABLE IF EXISTS d_specialite;
CREATE TEMPORARY TABLE d_specialite AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY code_specialite ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY code_specialite) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY code_specialite) AS emp_max
FROM t_specialite t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE specialite
SELECT code_specialite, specialite, id_execution
FROM d_specialite
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'specialite')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_specialite WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_specialite WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- DIAGNOSTIC (CIM-10)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_diagnostic;
CREATE TEMPORARY TABLE t_diagnostic AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws(';', code_diag, diagnostic) AS contenu,
       code_cim(code_diag)               AS code_diagnostic,
       txt(diagnostic)                   AS libelle_diagnostic,
       substr(code_cim(code_diag), 1, 3) AS code_categorie,
       CASE WHEN code_cim(code_diag) IS NULL THEN 'cle absente' END AS motif,
       nvl(txt(diagnostic), '') AS empreinte
FROM bronze.diagnostics;

DROP TABLE IF EXISTS d_diagnostic;
CREATE TEMPORARY TABLE d_diagnostic AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY code_diagnostic ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY code_diagnostic) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY code_diagnostic) AS emp_max
FROM t_diagnostic t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE diagnostic
SELECT code_diagnostic, libelle_diagnostic, code_categorie, id_execution
FROM d_diagnostic
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'diagnostic')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_diagnostic WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_diagnostic WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- PROFESSIONNEL (+ libellé de spécialité)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_professionnel;
CREATE TEMPORARY TABLE t_professionnel AS
SELECT p.id_execution, p.fichier, p.pos_octet, p.contenu,
       p.id_prof_pseudo, p.profession, p.categorie_professionnelle, p.code_specialite,
       s.specialite,
       p.motif,
       concat_ws('|', nvl(p.profession, ''), nvl(p.categorie_professionnelle, ''),
                      nvl(p.code_specialite, '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', id_prof_pseudo, profession, categorie_professionnelle, code_specialite) AS contenu,
           txt(id_prof_pseudo)            AS id_prof_pseudo,
           txt(profession)                AS profession,
           txt(categorie_professionnelle) AS categorie_professionnelle,
           txt(code_specialite)           AS code_specialite,
           CASE WHEN txt(id_prof_pseudo) IS NULL THEN 'cle absente' END AS motif
    FROM bronze.professionnel
) p
LEFT JOIN specialite s ON s.code_specialite = p.code_specialite;

DROP TABLE IF EXISTS d_professionnel;
CREATE TEMPORARY TABLE d_professionnel AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY id_prof_pseudo ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY id_prof_pseudo) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY id_prof_pseudo) AS emp_max
FROM t_professionnel t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE professionnel
SELECT id_prof_pseudo, profession, categorie_professionnelle, code_specialite, specialite, id_execution
FROM d_professionnel
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'professionnel')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_professionnel WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_professionnel WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- PATIENT
-- Date de naissance M/J/AAAA d'après le livrable ; AAAA-MM-JJ aussi accepté
-- au cas où PostgreSQL la renvoie dans son format natif.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_patient;
CREATE TEMPORARY TABLE t_patient AS
SELECT id_execution, fichier, pos_octet, contenu, id_patient_pseudo, sexe, date_naissance, motif,
       concat_ws('|', sexe, nvl(date_naissance, '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', id_patient_pseudo, sexe, date_naissance) AS contenu,
           txt(id_patient_pseudo) AS id_patient_pseudo,
           CASE lower(trim(sexe)) WHEN 'female' THEN 'F' WHEN 'male' THEN 'M' ELSE 'I' END AS sexe,
           iso_valide(coalesce(mja_vers_iso(date_naissance), trim(date_naissance))) AS date_naissance,
           CASE WHEN txt(id_patient_pseudo) IS NULL THEN 'cle absente' END AS motif
    FROM bronze.patient
) p;

DROP TABLE IF EXISTS d_patient;
CREATE TEMPORARY TABLE d_patient AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY id_patient_pseudo ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY id_patient_pseudo) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY id_patient_pseudo) AS emp_max
FROM t_patient t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE patient
SELECT id_patient_pseudo, sexe, date_naissance,
       CAST(substr(date_naissance, 1, 4) AS INT) AS annee_naissance,
       id_execution
FROM d_patient
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'patient')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_patient WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_patient WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- CONSULTATION (date AAAA-MM-JJ)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_consultation;
CREATE TEMPORARY TABLE t_consultation AS
SELECT id_execution, fichier, pos_octet, contenu,
       num_consultation, id_patient_pseudo, id_prof_pseudo, code_diagnostic,
       date_consultation, heure_debut, motif,
       concat_ws('|', nvl(id_patient_pseudo, ''), nvl(id_prof_pseudo, ''), nvl(code_diagnostic, ''),
                      nvl(date_consultation, ''), nvl(heure_debut, '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', num_consultation, id_patient_pseudo, id_prof_pseudo, code_diag,
                          date_consultation, heure_debut) AS contenu,
           txt(num_consultation)  AS num_consultation,
           txt(id_patient_pseudo) AS id_patient_pseudo,
           txt(id_prof_pseudo)    AS id_prof_pseudo,
           code_cim(code_diag)    AS code_diagnostic,
           iso_valide(trim(date_consultation)) AS date_consultation,
           CASE WHEN trim(heure_debut) RLIKE '^[0-9]{1,2}:[0-9]{2}' THEN trim(heure_debut) END AS heure_debut,
           CASE WHEN txt(num_consultation) IS NULL THEN 'cle absente' END AS motif
    FROM bronze.consultation
) c;

DROP TABLE IF EXISTS d_consultation;
CREATE TEMPORARY TABLE d_consultation AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY num_consultation ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY num_consultation) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY num_consultation) AS emp_max
FROM t_consultation t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE consultation
SELECT num_consultation, id_patient_pseudo, id_prof_pseudo, code_diagnostic,
       date_consultation, vers_id_temps(date_consultation) AS id_temps,
       heure_debut, id_execution
FROM d_consultation
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'consultation')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_consultation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_consultation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- HOSPITALISATION (date JJ/MM/AAAA, durée 1 à 365 jours)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_hospitalisation;
CREATE TEMPORARY TABLE t_hospitalisation AS
SELECT id_execution, fichier, pos_octet, contenu,
       num_hospitalisation, id_patient_pseudo, identifiant_organisation, code_diagnostic,
       date_entree, duree_sejour_jours,
       CASE WHEN num_hospitalisation IS NULL THEN 'cle absente'
            WHEN duree_sejour_jours IS NULL  THEN 'format invalide'
            WHEN duree_sejour_jours NOT BETWEEN 1 AND 365 THEN 'valeur hors domaine' END AS motif,
       concat_ws('|', nvl(id_patient_pseudo, ''), nvl(identifiant_organisation, ''), nvl(code_diagnostic, ''),
                      nvl(date_entree, ''), nvl(CAST(duree_sejour_jours AS STRING), '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', num_hospitalisation, id_patient_pseudo, identifiant_organisation,
                          code_diagnostic, date_entree, jour_hospitalisation) AS contenu,
           txt(num_hospitalisation)         AS num_hospitalisation,
           txt(id_patient_pseudo)           AS id_patient_pseudo,
           upper(txt(identifiant_organisation)) AS identifiant_organisation,
           code_cim(code_diagnostic)        AS code_diagnostic,
           iso_valide(jma_vers_iso(date_entree)) AS date_entree,
           CAST(txt(jour_hospitalisation) AS INT) AS duree_sejour_jours
    FROM bronze.hospitalisations
) h;

DROP TABLE IF EXISTS d_hospitalisation;
CREATE TEMPORARY TABLE d_hospitalisation AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY num_hospitalisation ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY num_hospitalisation) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY num_hospitalisation) AS emp_max
FROM t_hospitalisation t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE hospitalisation
SELECT num_hospitalisation, id_patient_pseudo, identifiant_organisation, code_diagnostic,
       date_entree, vers_id_temps(date_entree) AS id_temps,
       duree_sejour_jours, id_execution
FROM d_hospitalisation
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'hospitalisation')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_hospitalisation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_hospitalisation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- SATISFACTION (e-Satis 2017, 2019, 2020)
-- Vide (classes DI, NR, NV) -> NULL ; texte non numérique -> rejet ;
-- note hors 0 à 100 -> rejet. 2017 : taux et nb_reco absents -> NULL.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_satisfaction;
CREATE TEMPORARY TABLE t_satisfaction AS
SELECT id_execution, fichier, pos_octet, contenu,
       finess_geo, num_finess_juridique, raison_sociale, region, annee, classement,
       note_satisfaction, CAST(nb_avis AS INT) AS nb_avis,
       score_accueil, score_pecinf, score_pecmed, score_chambre, score_repas, score_sortie,
       taux_recommandation, CAST(nb_reco AS INT) AS nb_reco,
       CASE WHEN finess_geo IS NULL THEN 'cle absente'
            WHEN annee IS NULL      THEN 'format invalide'
            WHEN nb_num_ko > 0      THEN 'format invalide'
            WHEN hors_0_100(note_satisfaction) + hors_0_100(score_accueil) + hors_0_100(score_pecinf)
               + hors_0_100(score_pecmed) + hors_0_100(score_chambre) + hors_0_100(score_repas)
               + hors_0_100(score_sortie) + hors_0_100(taux_recommandation) > 0
                                    THEN 'valeur hors domaine' END AS motif,
       concat_ws('|', nvl(num_finess_juridique, ''), nvl(raison_sociale, ''), nvl(region, ''),
                      nvl(classement, ''), nvl(CAST(note_satisfaction AS STRING), ''),
                      nvl(CAST(nb_avis AS STRING), ''), nvl(CAST(score_accueil AS STRING), ''),
                      nvl(CAST(score_pecinf AS STRING), ''), nvl(CAST(score_pecmed AS STRING), ''),
                      nvl(CAST(score_chambre AS STRING), ''), nvl(CAST(score_repas AS STRING), ''),
                      nvl(CAST(score_sortie AS STRING), ''), nvl(CAST(taux_recommandation AS STRING), ''),
                      nvl(CAST(nb_reco AS STRING), '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', finess, finess_geo, rs_finess_geo, region, classement,
                          nb_rep_score_all_rea_ajust, score_all_rea_ajust, score_accueil_rea_ajust,
                          score_pecinf_rea_ajust, score_pecmed_rea_ajust, score_chambre_rea_ajust,
                          score_repas_rea_ajust, score_sortie_rea_ajust, taux_reco_brut,
                          nb_reco_brut, annee_recueil) AS contenu,
           CASE WHEN txt(finess_geo) IS NOT NULL THEN lpad(upper(txt(finess_geo)), 9, '0') END AS finess_geo,
           CASE WHEN txt(finess)     IS NOT NULL THEN lpad(upper(txt(finess)), 9, '0') END     AS num_finess_juridique,
           txt(rs_finess_geo)                   AS raison_sociale,
           txt(region)                          AS region,
           CAST(txt(annee_recueil) AS INT)      AS annee,
           txt(classement)                      AS classement,
           num(score_all_rea_ajust)             AS note_satisfaction,
           num(nb_rep_score_all_rea_ajust)      AS nb_avis,
           num(score_accueil_rea_ajust)         AS score_accueil,
           num(score_pecinf_rea_ajust)          AS score_pecinf,
           num(score_pecmed_rea_ajust)          AS score_pecmed,
           num(score_chambre_rea_ajust)         AS score_chambre,
           num(score_repas_rea_ajust)           AS score_repas,
           num(score_sortie_rea_ajust)          AS score_sortie,
           num(taux_reco_brut)                  AS taux_recommandation,
           num(nb_reco_brut)                    AS nb_reco,
           num_ko(nb_rep_score_all_rea_ajust) + num_ko(score_all_rea_ajust)
             + num_ko(score_accueil_rea_ajust) + num_ko(score_pecinf_rea_ajust)
             + num_ko(score_pecmed_rea_ajust) + num_ko(score_chambre_rea_ajust)
             + num_ko(score_repas_rea_ajust) + num_ko(score_sortie_rea_ajust)
             + num_ko(taux_reco_brut) + num_ko(nb_reco_brut) AS nb_num_ko
    FROM bronze.esatis
) e;

-- Clé d'unicité : (établissement, année)
DROP TABLE IF EXISTS d_satisfaction;
CREATE TEMPORARY TABLE d_satisfaction AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY finess_geo, annee ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY finess_geo, annee) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY finess_geo, annee) AS emp_max
FROM t_satisfaction t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE satisfaction
SELECT concat('F', finess_geo) AS identifiant_organisation,
       finess_geo, num_finess_juridique, raison_sociale, region,
       annee, annee * 10000 + 101 AS id_temps,
       classement, note_satisfaction, nb_avis,
       score_accueil, score_pecinf, score_pecmed, score_chambre, score_repas, score_sortie,
       taux_recommandation, nb_reco, id_execution
FROM d_satisfaction
WHERE emp_min = emp_max AND rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'satisfaction')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_satisfaction WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_satisfaction WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- ETABLISSEMENT (référentiel FINESS, puis établissements e-Satis absents)
-- Département : code commune, à défaut code postal, contrôlé dans le COG.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_etablissement;
CREATE TEMPORARY TABLE t_etablissement AS
SELECT id_execution, fichier, pos_octet, contenu, identifiant_organisation,
       CASE substr(identifiant_organisation, 1, 1)
            WHEN 'F' THEN 'Établissement FINESS'
            WHEN 'C' THEN 'Cabinet SIRET'
            WHEN 'R' THEN 'Cabinet RPPS' END AS type_structure,
       CASE WHEN identifiant_organisation LIKE 'F%' THEN substr(identifiant_organisation, 2) END AS num_finess_geo,
       CASE WHEN finess_juridique IS NOT NULL THEN lpad(finess_juridique, 9, '0') END AS num_finess_juridique,
       coalesce(raison_sociale, identifiant_organisation) AS raison_sociale,
       code_commune, commune, code_postal,
       coalesce(dep_commune(code_commune), dep_postal(code_postal)) AS dep_calcule,
       CASE WHEN identifiant_organisation IS NULL THEN 'cle absente'
            WHEN substr(identifiant_organisation, 1, 1) NOT IN ('F', 'C', 'R') THEN 'valeur hors domaine' END AS motif,
       concat_ws('|', nvl(finess_juridique, ''), nvl(raison_sociale, ''), nvl(code_commune, ''),
                      nvl(commune, ''), nvl(code_postal, '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', identifiant_organisation, finess_etablissement_juridique, raison_sociale_site,
                          code_commune, commune, code_postal) AS contenu,
           upper(txt(identifiant_organisation))  AS identifiant_organisation,
           txt(finess_etablissement_juridique)   AS finess_juridique,
           txt(raison_sociale_site)              AS raison_sociale,
           code5(code_commune)                   AS code_commune,
           txt(commune)                          AS commune,
           code5(code_postal)                    AS code_postal
    FROM bronze.etablissement
) e;

DROP TABLE IF EXISTS d_etablissement;
CREATE TEMPORARY TABLE d_etablissement AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY identifiant_organisation ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY identifiant_organisation) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY identifiant_organisation) AS emp_max
FROM t_etablissement t
WHERE t.motif IS NULL;

INSERT OVERWRITE TABLE etablissement
SELECT d.identifiant_organisation, d.type_structure, d.num_finess_geo, d.num_finess_juridique,
       d.raison_sociale, d.code_commune, d.commune, d.code_postal,
       l.code_departement, 'referentiel', d.id_execution
FROM d_etablissement d
LEFT JOIN localisation l ON l.code_departement = d.dep_calcule
WHERE d.emp_min = d.emp_max AND d.rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'etablissement')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_etablissement WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_etablissement WHERE emp_min <> emp_max
) r;

-- Établissements notés par e-Satis mais absents du référentiel :
-- dernière année connue ; département = début du FINESS géographique,
-- gardé seulement s'il est dans la région indiquée par e-Satis.
DROP TABLE IF EXISTS t_etab_esatis;
CREATE TEMPORARY TABLE t_etab_esatis AS
SELECT s.identifiant_organisation,
       'Établissement FINESS'                              AS type_structure,
       s.finess_geo                                        AS num_finess_geo,
       s.num_finess_juridique,
       coalesce(s.raison_sociale, s.identifiant_organisation) AS raison_sociale,
       CAST(NULL AS STRING)                                AS code_commune,
       CAST(NULL AS STRING)                                AS commune,
       CAST(NULL AS STRING)                                AS code_postal,
       CASE WHEN lower(l.region) = lower(s.region) THEN l.code_departement END AS code_departement,
       'esatis'                                            AS origine,
       s.id_execution
FROM (
    SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x.identifiant_organisation ORDER BY x.annee DESC) AS rn
    FROM satisfaction x
) s
LEFT JOIN etablissement e ON e.identifiant_organisation = s.identifiant_organisation
LEFT JOIN localisation l  ON l.code_departement = dep_commune(s.finess_geo)
WHERE s.rn = 1 AND e.identifiant_organisation IS NULL;

INSERT INTO TABLE etablissement
SELECT * FROM t_etab_esatis;

-- ---------------------------------------------------------------------
-- ACTIVITE : couples (médecin, organisation) dédoublonnés
-- Une activité sans organisation (exercice libéral non rattaché) n'est pas
-- une erreur : elle est ignorée et comptée à part dans l'audit.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_activite;
CREATE TEMPORARY TABLE t_activite AS
SELECT id_execution, fichier, pos_octet, contenu, id_prof_pseudo, identifiant_organisation,
       CASE WHEN id_prof_pseudo IS NULL THEN 'cle absente'
            WHEN identifiant_organisation IS NOT NULL
             AND substr(identifiant_organisation, 1, 1) NOT IN ('F', 'C', 'R') THEN 'valeur hors domaine' END AS motif
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws(';', id_prof_pseudo, identifiant_organisation) AS contenu,
           txt(id_prof_pseudo)                  AS id_prof_pseudo,
           upper(txt(identifiant_organisation)) AS identifiant_organisation
    FROM bronze.activite
) a;

INSERT OVERWRITE TABLE activite
SELECT id_prof_pseudo, identifiant_organisation, max(id_execution)
FROM t_activite
WHERE motif IS NULL AND identifiant_organisation IS NOT NULL
GROUP BY id_prof_pseudo, identifiant_organisation;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'activite')
SELECT id_execution, '20261102_1', '02_silver', fichier, pos_octet, contenu, motif,
       from_unixtime(unix_timestamp())
FROM t_activite
WHERE motif IS NOT NULL;

-- ---------------------------------------------------------------------
-- DECES (25 millions de lignes)
-- Aucune identité n'est extraite par Talend. Clé de dédoublonnage : acte de
-- décès = (commune du décès, date du décès, numéro d'acte). Le numéro seul
-- ne suffit pas : chaque mairie numérote ses actes à partir de 1 chaque année.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_deces;
CREATE TEMPORARY TABLE t_deces AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws(';', sexe, date_naissance, date_deces, code_lieu_deces, numero_acte_deces) AS contenu,
       CASE trim(sexe) WHEN '1' THEN 'M' WHEN '2' THEN 'F' ELSE 'I' END AS sexe,
       iso_valide(trim(date_naissance)) AS date_naissance,
       CASE WHEN trim(date_naissance) RLIKE '^[0-9]{4}' AND substr(trim(date_naissance), 1, 4) <> '0000'
            THEN CAST(substr(trim(date_naissance), 1, 4) AS INT) END AS annee_naissance,
       iso_valide(trim(date_deces)) AS date_deces,
       code5(code_lieu_deces)       AS code_lieu_deces,
       concat_ws('|', upper(nvl(txt(code_lieu_deces), '')), nvl(trim(date_deces), ''),
                      upper(txt(numero_acte_deces))) AS cle,
       concat_ws('|', nvl(trim(sexe), ''), nvl(trim(date_naissance), '')) AS empreinte,
       CASE WHEN numero_acte_deces IS NULL     THEN 'nombre de colonnes incorrect'
            WHEN txt(numero_acte_deces) IS NULL THEN 'cle absente' END AS motif
FROM bronze.deces;

DROP TABLE IF EXISTS d_deces;
CREATE TEMPORARY TABLE d_deces AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY cle ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY cle) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY cle) AS emp_max
FROM t_deces t
WHERE t.motif IS NULL;

-- num_deces attribué après dédoublonnage, dans l'ordre des dates de décès
INSERT OVERWRITE TABLE deces
SELECT ROW_NUMBER() OVER (ORDER BY d.id_temps, d.cle) AS num_deces,
       d.sexe, d.date_naissance, d.annee_naissance, d.date_deces, d.id_temps,
       d.code_lieu_deces, l.code_departement, d.id_execution
FROM (
    SELECT x.*, vers_id_temps(x.date_deces) AS id_temps
    FROM d_deces x
    WHERE x.emp_min = x.emp_max AND x.rn = 1
) d
LEFT JOIN localisation l ON l.code_departement = dep_commune(d.code_lieu_deces);

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'deces')
SELECT r.id_execution, '20261102_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_deces WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_deces WHERE emp_min <> emp_max
) r;

-- =====================================================================
-- AUDIT : lues = écrites + rejetées + doublons + ignorées
-- =====================================================================
INSERT INTO TABLE audit
SELECT '20261102_1', l.id_execution, '02_silver', l.flux, from_unixtime(unix_timestamp()),
       l.lues, e.ecrites, nvl(r.rejetees, 0),
       l.lues - e.ecrites - nvl(r.rejetees, 0) - l.ignorees,
       l.ignorees,
       round(nvl(r.rejetees, 0) / l.lues * 100, 2),
       CASE WHEN nvl(r.rejetees, 0) > 0.05 * l.lues THEN 'ECHEC_SEUIL' ELSE 'OK' END
FROM (
              SELECT 'localisation'    AS flux, max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_localisation
    UNION ALL SELECT 'specialite' AS flux,      max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_specialite
    UNION ALL SELECT 'diagnostic' AS flux,      max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_diagnostic
    UNION ALL SELECT 'professionnel' AS flux,   max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_professionnel
    UNION ALL SELECT 'patient' AS flux,         max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_patient
    UNION ALL SELECT 'consultation' AS flux,    max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_consultation
    UNION ALL SELECT 'hospitalisation' AS flux, max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_hospitalisation
    UNION ALL SELECT 'satisfaction' AS flux,    max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_satisfaction
    UNION ALL SELECT 'etablissement' AS flux,   max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_etablissement
    UNION ALL SELECT 'activite' AS flux,        max(id_execution) AS id_execution, count(*) AS lues,
                     CAST(sum(CASE WHEN motif IS NULL AND identifiant_organisation IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS ignorees
                     FROM t_activite
    UNION ALL SELECT 'deces' AS flux,           max(id_execution) AS id_execution, count(*) AS lues, CAST(0 AS BIGINT) AS ignorees FROM t_deces
) l
JOIN (
              SELECT 'localisation'    AS flux, count(*) AS ecrites FROM localisation WHERE origine = 'COG'
    UNION ALL SELECT 'specialite' AS flux,      count(*) AS ecrites FROM specialite
    UNION ALL SELECT 'diagnostic' AS flux,      count(*) AS ecrites FROM diagnostic
    UNION ALL SELECT 'professionnel' AS flux,   count(*) AS ecrites FROM professionnel
    UNION ALL SELECT 'patient' AS flux,         count(*) AS ecrites FROM patient
    UNION ALL SELECT 'consultation' AS flux,    count(*) AS ecrites FROM consultation
    UNION ALL SELECT 'hospitalisation' AS flux, count(*) AS ecrites FROM hospitalisation
    UNION ALL SELECT 'satisfaction' AS flux,    count(*) AS ecrites FROM satisfaction
    UNION ALL SELECT 'etablissement' AS flux,   count(*) AS ecrites FROM etablissement WHERE origine = 'referentiel'
    UNION ALL SELECT 'activite' AS flux,        count(*) AS ecrites FROM activite
    UNION ALL SELECT 'deces' AS flux,           count(*) AS ecrites FROM deces
) e ON e.flux = l.flux
LEFT JOIN (
    SELECT flux, count(*) AS rejetees FROM rejets GROUP BY flux
) r ON r.flux = l.flux;

-- ---------------------------------------------------------------------
-- Contrôles affichés
-- ---------------------------------------------------------------------
SELECT flux, lignes_lues, lignes_ecrites, lignes_rejetees, lignes_doublon, lignes_ignorees,
       taux_rejet, statut
FROM audit
WHERE id_silver = '20261102_1'
ORDER BY flux;

SELECT flux, motif, count(*) AS nb
FROM rejets
GROUP BY flux, motif
ORDER BY flux, motif;

SELECT origine, count(*) AS nb_etablissements,
       sum(CASE WHEN code_departement IS NULL THEN 1 ELSE 0 END) AS sans_departement
FROM etablissement
GROUP BY origine;

-- Seuil de 5 % : fait échouer beeline (code retour non nul) pour bloquer le Gold
SELECT assert_true(count(*) = 0) AS seuil_rejets_respecte
FROM audit
WHERE id_silver = '20261102_1' AND statut <> 'OK';
