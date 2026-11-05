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
-- PROJET CHU - ÉTAPE 2 : DU BRONZE AU SILVER
-- =====================================================================
-- Réglages ci-dessus (ils doivent rester en tête, sans commentaire avant
-- eux, sinon Hue les envoie au parseur SQL et Hive répond KW_ROLE)
--   nonstrict      : autorise le CROSS JOIN de l'audit et des clés
--   convert.join   : petites tables jointes en mémoire (COG, spécialités)
--   parquet.*      : compression Snappy, blocs de 32 Mo pour la VM
--   spark.*        : Hive on Spark dimensionné pour les 3 Go de YARN
--                    (driver 1 Go + exécuteur 1,5 Go). Pour revenir à
--                    MapReduce : remplacer ces lignes par
--                    SET hive.execution.engine=mr
--
-- Exécution recommandée (en terminal sur la VM, sans passer par Hue)
--   nohup hive -f ~/02_silver.hql > ~/silver.log 2>&1 &
--   tail -f ~/silver.log
--
-- Identifiant de cette exécution : '20261104_1', écrit en dur dans les
-- rejets, l'audit et le contrôle final. Le remplacer partout avant
-- chaque nouvelle exécution, sinon l'audit mélange deux lancements.
--
-- Règles appliquées (livrable, sections 2.2 à 2.7)
--   Typage        dates converties selon le format de chaque source,
--                 virgule décimale remplacée par un point
--   Normalisation espaces de bord retirés, chaîne vide = valeur absente,
--                 FINESS sur 9 caractères, code commune sur 5,
--                 département déduit du code commune (règle 2.4)
--   Unicité       chaque flux est contrôlé sur sa clé. Un doublon exact
--                 est supprimé, une même clé avec un contenu différent
--                 part en rejet
--   Rejets        silver.rejets, une partition par flux, avec le motif.
--                 Aucune ligne n'est supprimée en silence
--   Audit         silver.audit, avec le contrôle
--                 lues = écrites + rejetées + doublons + ignorées
--   Seuil         plus de 5 % de rejets sur un flux fait échouer le
--                 script en fin d'exécution : le Gold ne doit pas suivre
--
-- Ce qui est recalculé et ce qui est conservé
--   Les tables de données sont supprimées puis reconstruites à partir du
--   Bronze à chaque exécution. Les tables cle_patient, cle_professionnel,
--   rejets et audit sont créées une seule fois et conservées.
--
-- Dates
--   Hive 1.1 ne sait pas écrire de DATE en Parquet. Les dates sont donc
--   gardées en texte AAAA-MM-JJ, avec leur id_temps AAAAMMJJ (clé de
--   DIM_TEMPS), qui vaut -1 si la date est absente ou invalide.
--
-- Compatibilité avec le client hive en ligne de commande
--   Ce client découpe le script à chaque point-virgule, même entre
--   guillemets. Le séparateur des contenus rejetés est donc écrit '\073',
--   code octal du point-virgule, et les commentaires n'en contiennent pas.
-- =====================================================================

-- =====================================================================
-- MACROS DE NETTOYAGE
-- Fonctions réutilisées par tous les flux, valables pour la session.
-- =====================================================================

-- Texte : espaces de début et de fin retirés, chaîne vide -> NULL
CREATE TEMPORARY MACRO txt(s STRING)
    CASE WHEN trim(s) = '' THEN NULL ELSE trim(s) END;

-- Date AAAA-MM-JJ réelle, sinon NULL
-- La conversion aller-retour refuse les dates impossibles comme le
-- 30 février, que Java aurait sinon reportées au 2 mars.
CREATE TEMPORARY MACRO iso_valide(s STRING)
    CASE WHEN s RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          AND from_unixtime(unix_timestamp(s, 'yyyy-MM-dd'), 'yyyy-MM-dd') = s
         THEN s END;

-- JJ/MM/AAAA -> AAAA-MM-JJ (format de Hospitalisations.csv)
CREATE TEMPORARY MACRO jma_vers_iso(s STRING)
    CASE WHEN trim(s) RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
         THEN concat(split(trim(s), '/')[2], '-',
                     lpad(split(trim(s), '/')[1], 2, '0'), '-',
                     lpad(split(trim(s), '/')[0], 2, '0')) END;

-- M/J/AAAA -> AAAA-MM-JJ (format de la table PostgreSQL Patient)
CREATE TEMPORARY MACRO mja_vers_iso(s STRING)
    CASE WHEN trim(s) RLIKE '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
         THEN concat(split(trim(s), '/')[2], '-',
                     lpad(split(trim(s), '/')[0], 2, '0'), '-',
                     lpad(split(trim(s), '/')[1], 2, '0')) END;

-- AAAA-MM-JJ -> id_temps AAAAMMJJ, -1 si la date est absente
CREATE TEMPORARY MACRO vers_id_temps(iso STRING)
    CASE WHEN iso IS NULL THEN -1 ELSE CAST(regexp_replace(iso, '-', '') AS INT) END;

-- Code commune ou code postal ramené à 5 caractères
-- Excel a parfois retiré le zéro de tête (1451 au lieu de 01451).
-- Les codes corses (2A004) sont acceptés, toute autre forme donne NULL.
CREATE TEMPORARY MACRO code5(c STRING)
    CASE WHEN upper(trim(c)) RLIKE '^[0-9][0-9AB][0-9]{2,3}$'
         THEN lpad(upper(trim(c)), 5, '0') END;

-- Département d'un code commune INSEE (règle 2.4 du livrable)
--   99...       -> 99 (étranger)
--   97... 98... -> 3 premiers caractères (outre-mer)
--   2A... 2B... -> 2A ou 2B (Corse)
--   sinon       -> 2 premiers caractères
--   00...       -> NULL, pour laisser le code postal prendre le relais
CREATE TEMPORARY MACRO dep_commune(c STRING)
    CASE WHEN c IS NULL OR c LIKE '00%' THEN NULL
         WHEN c LIKE '99%' THEN '99'
         WHEN c LIKE '97%' OR c LIKE '98%' THEN substr(c, 1, 3)
         WHEN upper(substr(c, 1, 2)) IN ('2A', '2B') THEN upper(substr(c, 1, 2))
         ELSE substr(c, 1, 2) END;

-- Département d'un code postal, utilisé à défaut de code commune
-- Corse : 20000 à 20199 pour la Corse-du-Sud, au-delà pour la Haute-Corse.
CREATE TEMPORARY MACRO dep_postal(cp STRING)
    CASE WHEN cp IS NULL THEN NULL
         WHEN cp LIKE '97%' OR cp LIKE '98%' THEN substr(cp, 1, 3)
         WHEN cp LIKE '20%' THEN CASE WHEN cp < '20200' THEN '2A' ELSE '2B' END
         ELSE substr(cp, 1, 2) END;

-- Département d'un numéro FINESS (établissements ajoutés depuis e-Satis)
-- Les FINESS d'outre-mer commencent par 9701 à 9704 et 9805, ce qui ne
-- correspond pas directement aux codes du COG (971 à 974 et 976).
CREATE TEMPORARY MACRO dep_finess(f STRING)
    CASE substr(f, 1, 4)
         WHEN '9701' THEN '971' WHEN '9702' THEN '972' WHEN '9703' THEN '973'
         WHEN '9704' THEN '974' WHEN '9805' THEN '976'
         ELSE CASE WHEN upper(substr(f, 1, 2)) IN ('2A', '2B') THEN upper(substr(f, 1, 2))
                   ELSE substr(f, 1, 2) END END;

-- Nom de région comparable entre e-Satis et le COG
-- Minuscules, sans accent, sans espace ni tiret : « Ile de France » et
-- « Île-de-France » donnent tous deux iledefrance.
CREATE TEMPORARY MACRO cle_region(s STRING)
    regexp_replace(translate(lower(s), 'àâäéèêëîïôöùûüç', 'aaaeeeeiioouuuc'), '[^a-z]', '');

-- Code CIM-10 : majuscules, sans aucun espace, NULL si vide
CREATE TEMPORARY MACRO code_cim(s STRING)
    CASE WHEN regexp_replace(upper(s), '\\s', '') = '' THEN NULL
         ELSE regexp_replace(upper(s), '\\s', '') END;

-- Nombre écrit avec une virgule ou un point décimal
-- NULL si la valeur est vide ou illisible.
CREATE TEMPORARY MACRO num(s STRING)
    CAST(regexp_replace(trim(s), ',', '.') AS DOUBLE);

-- Vaut 1 si un texte est présent mais n'est pas un nombre (format invalide)
CREATE TEMPORARY MACRO num_ko(s STRING)
    CASE WHEN trim(s) <> '' AND CAST(regexp_replace(trim(s), ',', '.') AS DOUBLE) IS NULL
         THEN 1 ELSE 0 END;

-- Vaut 1 si une note e-Satis sort de l'intervalle 0 à 100
CREATE TEMPORARY MACRO hors_0_100(x DOUBLE)
    CASE WHEN x < 0 OR x > 100 THEN 1 ELSE 0 END;

USE silver;

-- =====================================================================
-- STRUCTURE DES TABLES SILVER (Parquet)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Tables de données : supprimées et recréées à chaque exécution
-- ---------------------------------------------------------------------

-- Départements avec leur région (base de DIM_LOCALISATION)
DROP TABLE IF EXISTS localisation;
CREATE TABLE localisation (
    code_departement  STRING,
    departement       STRING,
    code_region       STRING,
    region            STRING,
    origine           STRING COMMENT 'COG ou ajout (20, COM, 99)',
    id_execution      STRING
) STORED AS PARQUET;

-- Spécialités médicales (libellé repris dans professionnel)
DROP TABLE IF EXISTS specialite;
CREATE TABLE specialite (
    code_specialite  STRING,
    specialite       STRING,
    id_execution     STRING
) STORED AS PARQUET;

-- Diagnostics CIM-10 (base de DIM_DIAGNOSTIC)
DROP TABLE IF EXISTS diagnostic;
CREATE TABLE diagnostic (
    code_diagnostic     STRING,
    libelle_diagnostic  STRING,
    code_categorie      STRING COMMENT '3 premiers caractères du code',
    id_execution        STRING
) STORED AS PARQUET;

-- Professionnels de santé (base de DIM_PROFESSIONNEL)
DROP TABLE IF EXISTS professionnel;
CREATE TABLE professionnel (
    id_prof_pseudo             STRING,
    profession                 STRING,
    categorie_professionnelle  STRING,
    code_specialite            STRING,
    specialite                 STRING COMMENT 'NULL si le code est absent du référentiel',
    id_execution               STRING
) STORED AS PARQUET;

-- Patients (base de DIM_PATIENT)
-- La date de naissance complète reste en Silver : le Gold n'en garde
-- que l'année, mais elle sert au calcul de l'âge dans les faits.
DROP TABLE IF EXISTS patient;
CREATE TABLE patient (
    id_patient_pseudo  STRING,
    sexe               STRING COMMENT 'F, M ou I',
    date_naissance     STRING COMMENT "AAAA-MM-JJ, sert au calcul de l'âge",
    annee_naissance    INT,
    id_execution       STRING
) STORED AS PARQUET;

-- Consultations (base de FAIT_CONSULTATION)
DROP TABLE IF EXISTS consultation;
CREATE TABLE consultation (
    num_consultation   STRING,
    id_patient_pseudo  STRING,
    id_prof_pseudo     STRING,
    code_diagnostic    STRING,
    date_consultation  STRING,
    id_temps           INT,
    heure_debut        STRING COMMENT 'NULL si illisible, le Gold en déduit le créneau',
    id_execution       STRING
) STORED AS PARQUET;

-- Séjours hospitaliers (base de FAIT_HOSPITALISATION)
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

-- Résultats e-Satis, un établissement et une année par ligne
-- (base de FAIT_SATISFACTION)
DROP TABLE IF EXISTS satisfaction;
CREATE TABLE satisfaction (
    identifiant_organisation  STRING COMMENT 'F + finess_geo',
    finess_geo                STRING,
    num_finess_juridique      STRING,
    raison_sociale            STRING,
    region                    STRING,
    annee                     INT,
    id_temps                  INT COMMENT "AAAA0101 : 1er janvier de l'année de recueil",
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

-- Organisations (base de DIM_ETABLISSEMENT)
-- Sert aussi de table de rattachement établissement -> département pour
-- les faits. Contient le référentiel, plus les établissements notés par
-- e-Satis qui en sont absents.
DROP TABLE IF EXISTS etablissement;
CREATE TABLE etablissement (
    identifiant_organisation  STRING,
    type_structure            STRING,
    num_finess_geo            STRING COMMENT 'NULL pour les cabinets et les identifiants F longs',
    num_finess_juridique      STRING,
    raison_sociale            STRING,
    code_commune              STRING,
    commune                   STRING,
    code_postal               STRING,
    code_departement          STRING COMMENT 'NULL si introuvable dans le COG',
    origine                   STRING COMMENT 'referentiel ou esatis',
    id_execution              STRING
) STORED AS PARQUET;

-- Couples (professionnel, organisation) dédoublonnés
-- Permet au Gold de rattacher chaque consultation à un établissement :
-- aucune organisation -> -1, une seule -> elle, plusieurs -> -2.
DROP TABLE IF EXISTS activite;
CREATE TABLE activite (
    id_prof_pseudo            STRING,
    identifiant_organisation  STRING,
    id_execution              STRING
) STORED AS PARQUET;

-- Décès dédoublonnés, sans identité (base de FAIT_DECES)
DROP TABLE IF EXISTS deces;
CREATE TABLE deces (
    num_deces         BIGINT COMMENT 'AAAAMMJJ x 100000 + rang dans la journée',
    sexe              STRING COMMENT 'M, F ou I',
    date_naissance    STRING COMMENT 'AAAA-MM-JJ, NULL si incomplète',
    annee_naissance   INT    COMMENT "Pour l'âge quand la date est incomplète",
    date_deces        STRING COMMENT 'AAAA-MM-JJ, NULL si incomplète',
    annee_deces       INT    COMMENT 'Gardée quand la date est incomplète (AAAA ou AAAA-MM)',
    id_temps          INT,
    code_lieu_deces   STRING,
    code_departement  STRING COMMENT 'NULL si introuvable dans le COG',
    id_execution      STRING
) STORED AS PARQUET;

-- ---------------------------------------------------------------------
-- Tables permanentes : créées une seule fois, jamais supprimées
-- ---------------------------------------------------------------------

-- Correspondances pseudonyme -> clé de substitution (fiches 3.4 et 3.6)
-- Une clé attribuée ne change plus : seuls les nouveaux pseudonymes sont
-- ajoutés, en fin de script. Le Gold lit ces clés pour DIM_PATIENT,
-- DIM_PROFESSIONNEL et les faits. Attention : un DROP DATABASE silver
-- CASCADE les effacerait et toutes les clés seraient renumérotées.
CREATE TABLE IF NOT EXISTS cle_patient (
    id_patient_pseudo  STRING,
    id_patient         INT
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS cle_professionnel (
    id_prof_pseudo    STRING,
    id_professionnel  INT
) STORED AS PARQUET;

-- Lignes écartées, une partition par flux (section 2.7)
-- pos_octet donne la position de la ligne dans le fichier Bronze, et non
-- son numéro : Hive ne connaît pas les numéros de ligne d'un fichier.
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

-- Journal des exécutions, une ligne par flux et par lancement (section 2.8)
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
-- MÉTHODE COMMUNE À CHAQUE FLUX
--
-- 1. t_<flux> (table temporaire) : une ligne par ligne du Bronze, avec
--    les valeurs nettoyées, le motif de rejet éventuel (NULL si la ligne
--    est valide), et deux colonnes techniques :
--      fichier, pos_octet, contenu : pour retrouver la ligne d'origine
--      empreinte : concaténation des valeurs hors clé
--
-- 2. d_<flux> (table temporaire) : lignes valides seulement, avec
--      rn       : numéro de la ligne parmi celles de même clé
--      emp_min et emp_max : plus petite et plus grande empreinte de la clé
--    Si emp_min = emp_max, toutes les lignes de la clé sont identiques :
--    on garde la première (rn = 1), les autres sont des doublons exacts.
--    Si emp_min <> emp_max, la clé a plusieurs contenus : toutes ses
--    lignes partent en rejet, faute de savoir laquelle est la bonne.
--
-- 3. INSERT OVERWRITE de la table Silver et de la partition de rejets.
--
-- Les tables temporaires disparaissent à la fin de la session. Le DROP
-- placé avant chacune permet de relancer le script dans la même session.
-- =====================================================================

-- ---------------------------------------------------------------------
-- FLUX LOCALISATION : départements du COG avec leur région
-- Clé : code_departement
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
-- Les guillemets éventuels des fichiers INSEE sont retirés par sécurité
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws('\073', dep, reg, libelle) AS contenu,
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

-- Départements du COG, complétés par les codes absents du COG mais
-- présents dans les autres sources (fiche 3.2) :
--   20         ancien code de la Corse, rattaché à la région 94
--   975 à 988  collectivités d'outre-mer, région COM
--   99         étranger
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_localisation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_localisation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX SPECIALITE : référentiel des spécialités
-- Clé : code_specialite
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_specialite;
CREATE TEMPORARY TABLE t_specialite AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws('\073', code_specialite, specialite) AS contenu,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_specialite WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_specialite WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX DIAGNOSTIC : référentiel CIM-10
-- Clé : code_diagnostic, normalisé en majuscules sans espace (fiche 3.5).
-- La catégorie (3 premiers caractères) est préparée ici, le chapitre
-- CIM-10 sera calculé dans le Gold à partir de cette catégorie.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_diagnostic;
CREATE TEMPORARY TABLE t_diagnostic AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws('\073', code_diag, diagnostic) AS contenu,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_diagnostic WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_diagnostic WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX PROFESSIONNEL : professionnels de santé
-- Clé : id_prof_pseudo
-- Le libellé de spécialité est repris de silver.specialite, construit
-- juste au-dessus. Un code absent du référentiel donne NULL (fiche 3.6).
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
           concat_ws('\073', id_prof_pseudo, profession, categorie_professionnelle, code_specialite) AS contenu,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_professionnel WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_professionnel WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX PATIENT
-- Clé : id_patient_pseudo
-- Sexe : female -> F, male -> M, toute autre valeur -> I (fiche 3.4).
-- Date de naissance : M/J/AAAA d'après le livrable. Le format AAAA-MM-JJ
-- est aussi accepté, au cas où PostgreSQL renvoie son format natif.
-- Une date illisible donne NULL sans rejeter le patient.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_patient;
CREATE TEMPORARY TABLE t_patient AS
SELECT id_execution, fichier, pos_octet, contenu, id_patient_pseudo, sexe, date_naissance, motif,
       concat_ws('|', sexe, nvl(date_naissance, '')) AS empreinte
FROM (
    SELECT id_execution,
           INPUT__FILE__NAME           AS fichier,
           BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
           concat_ws('\073', id_patient_pseudo, sexe, date_naissance) AS contenu,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_patient WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_patient WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX CONSULTATION
-- Clé : num_consultation
-- Date au format AAAA-MM-JJ. Une date invalide donne id_temps = -1 et une
-- heure illisible donne NULL : la consultation est gardée dans les deux
-- cas, seule l'absence du numéro entraîne un rejet.
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
           concat_ws('\073', num_consultation, id_patient_pseudo, id_prof_pseudo, code_diag,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_consultation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_consultation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX HOSPITALISATION
-- Clé : num_hospitalisation
-- Date d'entrée au format JJ/MM/AAAA. Durée du séjour obligatoire :
-- vide ou illisible -> rejet pour format invalide, hors 1 à 365 jours ->
-- rejet pour valeur hors domaine (fiche 4.2).
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
           concat_ws('\073', num_hospitalisation, id_patient_pseudo, identifiant_organisation,
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_hospitalisation WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_hospitalisation WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX SATISFACTION : résultats e-Satis 2017, 2019 et 2020
-- Clé : (finess_geo, annee)
-- Règles de la fiche 4.3 :
--   valeur vide (classes DI, NR, NV)   -> NULL, la ligne est gardée
--   texte présent mais non numérique   -> rejet pour format invalide
--   note ou taux hors 0 à 100          -> rejet pour valeur hors domaine
--   taux et nb_reco absents en 2017    -> NULL
-- La sous-requête convertit les valeurs, la requête principale en déduit
-- le motif de rejet.
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
           concat_ws('\073', finess, finess_geo, rs_finess_geo, region, classement,
                          nb_rep_score_all_rea_ajust, score_all_rea_ajust, score_accueil_rea_ajust,
                          score_pecinf_rea_ajust, score_pecmed_rea_ajust, score_chambre_rea_ajust,
                          score_repas_rea_ajust, score_sortie_rea_ajust, taux_reco_brut,
                          nb_reco_brut, annee_recueil) AS contenu,
-- FINESS ramenés à 9 caractères (zéro de tête perdu par Excel)
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
-- Nombre de valeurs numériques illisibles sur la ligne
           num_ko(nb_rep_score_all_rea_ajust) + num_ko(score_all_rea_ajust)
             + num_ko(score_accueil_rea_ajust) + num_ko(score_pecinf_rea_ajust)
             + num_ko(score_pecmed_rea_ajust) + num_ko(score_chambre_rea_ajust)
             + num_ko(score_repas_rea_ajust) + num_ko(score_sortie_rea_ajust)
             + num_ko(taux_reco_brut) + num_ko(nb_reco_brut) AS nb_num_ko
    FROM bronze.esatis
) e;

DROP TABLE IF EXISTS d_satisfaction;
CREATE TEMPORARY TABLE d_satisfaction AS
SELECT t.*,
       ROW_NUMBER() OVER (PARTITION BY finess_geo, annee ORDER BY fichier, pos_octet) AS rn,
       MIN(empreinte) OVER (PARTITION BY finess_geo, annee) AS emp_min,
       MAX(empreinte) OVER (PARTITION BY finess_geo, annee) AS emp_max
FROM t_satisfaction t
WHERE t.motif IS NULL;

-- L'identifiant d'organisation est reconstruit comme dans le référentiel
-- (F + FINESS géographique), et la date du recueil est fixée au 1er janvier.
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
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_satisfaction WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_satisfaction WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- FLUX ETABLISSEMENT : référentiel des organisations
-- Clé : identifiant_organisation, qui doit commencer par F, C ou R
-- (fiche 3.3). Le département vient du code commune, à défaut du code
-- postal, et n'est gardé que s'il existe dans silver.localisation.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_etablissement;
CREATE TEMPORARY TABLE t_etablissement AS
SELECT id_execution, fichier, pos_octet, contenu, identifiant_organisation,
       CASE substr(identifiant_organisation, 1, 1)
            WHEN 'F' THEN 'Établissement FINESS'
            WHEN 'C' THEN 'Cabinet SIRET'
            WHEN 'R' THEN 'Cabinet RPPS' END AS type_structure,
-- Numéro FINESS géographique : seulement pour F suivi de 9 caractères.
-- Les 21 332 identifiants F de 18 caractères ne sont pas un FINESS.
       CASE WHEN identifiant_organisation LIKE 'F%' AND length(identifiant_organisation) = 10
            THEN substr(identifiant_organisation, 2) END AS num_finess_geo,
       CASE WHEN finess_juridique IS NOT NULL THEN lpad(finess_juridique, 9, '0') END AS num_finess_juridique,
-- Raison sociale vide -> identifiant de l'organisation (fiche 3.3)
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
           concat_ws('\073', identifiant_organisation, finess_etablissement_juridique, raison_sociale_site,
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

-- La jointure sur localisation laisse le département à NULL s'il n'existe
-- pas dans le COG. Le Gold le remplacera par le membre -1.
INSERT OVERWRITE TABLE etablissement
SELECT d.identifiant_organisation, d.type_structure, d.num_finess_geo, d.num_finess_juridique,
       d.raison_sociale, d.code_commune, d.commune, d.code_postal,
       l.code_departement, 'referentiel', d.id_execution
FROM d_etablissement d
LEFT JOIN localisation l ON l.code_departement = d.dep_calcule
WHERE d.emp_min = d.emp_max AND d.rn = 1;

INSERT OVERWRITE TABLE rejets PARTITION (flux = 'etablissement')
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_etablissement WHERE motif IS NOT NULL
    UNION ALL
    SELECT id_execution, fichier, pos_octet, contenu, 'meme cle contenu different' AS motif
    FROM d_etablissement WHERE emp_min <> emp_max
) r;

-- ---------------------------------------------------------------------
-- Ajout des établissements notés par e-Satis mais absents du référentiel
-- (fiche 3.3), pour que chaque ligne de FAIT_SATISFACTION trouve son
-- établissement.
--   Nom et FINESS juridique : ceux de la dernière année de recueil.
--   Département : déduit du FINESS géographique, puis contrôlé avec la
--   région indiquée par e-Satis. En cas de désaccord, le département
--   reste à NULL (membre -1 dans le Gold) plutôt que d'être faux.
--   e-Satis écrit PACA pour Provence-Alpes-Côte d'Azur, et regroupe
--   La Réunion et Mayotte sous Océan Indien.
-- ---------------------------------------------------------------------
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
       CASE WHEN cle_region(l.region) = cle_region(s.region)
              OR (cle_region(s.region) = 'paca'        AND cle_region(l.region) = 'provencealpescotedazur')
              OR (cle_region(s.region) = 'oceanindien' AND cle_region(l.region) IN ('lareunion', 'mayotte'))
            THEN l.code_departement END AS code_departement,
       'esatis'                                            AS origine,
       s.id_execution
FROM (
    SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x.identifiant_organisation ORDER BY x.annee DESC) AS rn
    FROM satisfaction x
) s
LEFT JOIN etablissement e ON e.identifiant_organisation = s.identifiant_organisation
LEFT JOIN localisation l  ON l.code_departement = dep_finess(s.finess_geo)
WHERE s.rn = 1 AND e.identifiant_organisation IS NULL;

INSERT INTO TABLE etablissement
SELECT * FROM t_etab_esatis;

-- ---------------------------------------------------------------------
-- FLUX ACTIVITE : rattachement professionnel -> organisation
-- Clé : le couple (id_prof_pseudo, identifiant_organisation). Le contenu
-- se résume à la clé, un doublon est donc forcément exact.
-- Une activité sans organisation (exercice libéral non rattaché) n'est
-- pas une erreur : elle est ignorée et comptée à part dans l'audit.
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
           concat_ws('\073', id_prof_pseudo, identifiant_organisation) AS contenu,
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
SELECT id_execution, '20261104_1', '02_silver', fichier, pos_octet, contenu, motif,
       from_unixtime(unix_timestamp())
FROM t_activite
WHERE motif IS NOT NULL;

-- ---------------------------------------------------------------------
-- FLUX DECES : fichier INSEE, 25 millions de lignes
-- Clé : l'acte de décès = (commune du décès, date du décès, numéro
-- d'acte). Le numéro seul ne suffit pas : chaque mairie recommence à 1
-- chaque année. Aucune identité n'est extraite par Talend.
-- Règles de la fiche 4.4 :
--   sexe 1 -> M, 2 -> F, autre -> I
--   date incomplète (AAAA ou AAAA-MM) -> date NULL, mais l'année est
--   gardée dans annee_naissance et annee_deces pour l'âge et la partition
--   ligne sans ses 6 colonnes -> rejet, numéro d'acte vide -> rejet
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS t_deces;
CREATE TEMPORARY TABLE t_deces AS
SELECT id_execution,
       INPUT__FILE__NAME           AS fichier,
       BLOCK__OFFSET__INSIDE__FILE AS pos_octet,
       concat_ws('\073', sexe, date_naissance, date_deces, code_lieu_deces, numero_acte_deces) AS contenu,
       CASE trim(sexe) WHEN '1' THEN 'M' WHEN '2' THEN 'F' ELSE 'I' END AS sexe,
       iso_valide(trim(date_naissance)) AS date_naissance,
       CASE WHEN trim(date_naissance) RLIKE '^[0-9]{4}' AND substr(trim(date_naissance), 1, 4) <> '0000'
            THEN CAST(substr(trim(date_naissance), 1, 4) AS INT) END AS annee_naissance,
       iso_valide(trim(date_deces)) AS date_deces,
       CASE WHEN trim(date_deces) RLIKE '^[0-9]{4}' AND substr(trim(date_deces), 1, 4) <> '0000'
            THEN CAST(substr(trim(date_deces), 1, 4) AS INT) END AS annee_deces,
       code5(code_lieu_deces)       AS code_lieu_deces,
       concat_ws('|', upper(nvl(txt(code_lieu_deces), '')), nvl(trim(date_deces), ''),
                      upper(txt(numero_acte_deces))) AS cle,
       concat_ws('|', nvl(trim(sexe), ''), nvl(trim(date_naissance), '')) AS empreinte,
       CASE WHEN numero_acte_deces IS NULL     THEN 'nombre de colonnes incorrect'
            WHEN txt(numero_acte_deces) IS NULL THEN 'cle absente' END AS motif
FROM bronze.deces;

-- Dédoublonnage par GROUP BY au lieu des fonctions de fenêtre des autres
-- flux : sur 25 millions de lignes, les fenêtres dépassent la mémoire de
-- la VM. Le résultat est le même. Dans une clé sans conflit
-- (emp_min = emp_max), toutes les lignes sont identiques, et min() redonne
-- donc leurs valeurs.
DROP TABLE IF EXISTS d_deces;
DROP TABLE IF EXISTS g_deces;
CREATE TEMPORARY TABLE g_deces AS
SELECT cle,
       min(empreinte)       AS emp_min,
       max(empreinte)       AS emp_max,
       min(sexe)            AS sexe,
       min(date_naissance)  AS date_naissance,
       min(annee_naissance) AS annee_naissance,
       min(date_deces)      AS date_deces,
       min(annee_deces)     AS annee_deces,
       min(code_lieu_deces) AS code_lieu_deces,
       max(id_execution)    AS id_execution
FROM t_deces
WHERE motif IS NULL
GROUP BY cle;

-- num_deces, attribué après dédoublonnage (fiche 4.4)
--   Valeur : AAAAMMJJ du décès x 100000 + rang du décès dans la journée,
--   ou 0 x 100000 + rang pour les dates incomplètes. Exemple : 42e décès
--   du 15/03/2019 -> 2019031500042.
--   Unique et calculé sans tri global des 25 millions de lignes, mais pas
--   stable : si un décès d'une journée apparaît ou disparaît au chargement
--   suivant, les rangs de cette journée changent. Type BIGINT, trop grand
--   pour un INT.
INSERT OVERWRITE TABLE deces
SELECT CAST(CASE WHEN d.id_temps = -1 THEN 0 ELSE d.id_temps END AS BIGINT) * 100000
         + ROW_NUMBER() OVER (PARTITION BY d.id_temps ORDER BY d.cle) AS num_deces,
       d.sexe, d.date_naissance, d.annee_naissance, d.date_deces, d.annee_deces, d.id_temps,
       d.code_lieu_deces, l.code_departement, d.id_execution
FROM (
    SELECT g.*, vers_id_temps(g.date_deces) AS id_temps
    FROM g_deces g
    WHERE g.emp_min = g.emp_max
) d
LEFT JOIN localisation l ON l.code_departement = dep_commune(d.code_lieu_deces);

-- Rejets : lignes invalides, et toutes les lignes des actes en conflit
-- (même acte avec un sexe ou une date de naissance différents)
INSERT OVERWRITE TABLE rejets PARTITION (flux = 'deces')
SELECT r.id_execution, '20261104_1', '02_silver', r.fichier, r.pos_octet, r.contenu, r.motif,
       from_unixtime(unix_timestamp())
FROM (
    SELECT id_execution, fichier, pos_octet, contenu, motif FROM t_deces WHERE motif IS NOT NULL
    UNION ALL
    SELECT t.id_execution, t.fichier, t.pos_octet, t.contenu, 'meme cle contenu different' AS motif
    FROM t_deces t
    JOIN (SELECT cle FROM g_deces WHERE emp_min <> emp_max) c ON c.cle = t.cle
    WHERE t.motif IS NULL
) r;

-- =====================================================================
-- CORRESPONDANCES PSEUDONYME -> CLÉ (règle 2.3, fiches 3.4 et 3.6)
-- Un pseudonyme déjà présent garde sa clé. Un nouveau pseudonyme reçoit
-- la plus grande clé existante + 1, dans l'ordre des pseudonymes (et non
-- des identifiants d'origine, pour ne rien révéler de ceux-ci).
-- Au premier chargement, la table est vide et la numérotation part de 1.
-- =====================================================================
INSERT INTO TABLE cle_patient
SELECT n.id_patient_pseudo,
       CAST(m.cle_max + ROW_NUMBER() OVER (ORDER BY n.id_patient_pseudo) AS INT) AS id_patient
FROM (
    SELECT p.id_patient_pseudo
    FROM patient p
    LEFT JOIN cle_patient c ON c.id_patient_pseudo = p.id_patient_pseudo
    WHERE c.id_patient_pseudo IS NULL
) n
CROSS JOIN (SELECT nvl(max(id_patient), 0) AS cle_max FROM cle_patient) m;

INSERT INTO TABLE cle_professionnel
SELECT n.id_prof_pseudo,
       CAST(m.cle_max + ROW_NUMBER() OVER (ORDER BY n.id_prof_pseudo) AS INT) AS id_professionnel
FROM (
    SELECT p.id_prof_pseudo
    FROM professionnel p
    LEFT JOIN cle_professionnel c ON c.id_prof_pseudo = p.id_prof_pseudo
    WHERE c.id_prof_pseudo IS NULL
) n
CROSS JOIN (SELECT nvl(max(id_professionnel), 0) AS cle_max FROM cle_professionnel) m;

-- =====================================================================
-- AUDIT (section 2.8)
-- Une ligne par flux pour cette exécution.
--   lues      : lignes du Bronze (tables t_<flux>)
--   écrites   : lignes de la table Silver. Pour la localisation et les
--               établissements, seules les lignes issues de la source
--               sont comptées, sans les ajouts (COM, e-Satis)
--   rejetées  : lignes de la partition de rejets du flux
--   ignorées  : activités sans organisation
--   doublons  : déduits, lues - écrites - rejetées - ignorées
-- Statut ECHEC_SEUIL si plus de 5 % des lignes lues sont rejetées.
-- =====================================================================
INSERT INTO TABLE audit
SELECT '20261104_1', l.id_execution, '02_silver', l.flux, from_unixtime(unix_timestamp()),
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

-- =====================================================================
-- CONTRÔLES AFFICHÉS EN FIN D'EXÉCUTION
-- =====================================================================

-- Contrôle 1 : bilan de l'exécution, un flux par ligne
SELECT flux, lignes_lues, lignes_ecrites, lignes_rejetees, lignes_doublon, lignes_ignorees,
       taux_rejet, statut
FROM audit
WHERE id_silver = '20261104_1'
ORDER BY flux;

-- Contrôle 2 : rejets par flux et par motif
SELECT flux, motif, count(*) AS nb
FROM rejets
GROUP BY flux, motif
ORDER BY flux, motif;

-- Contrôle 3 : établissements sans département (membre -1 dans le Gold)
SELECT origine, count(*) AS nb_etablissements,
       sum(CASE WHEN code_departement IS NULL THEN 1 ELSE 0 END) AS sans_departement
FROM etablissement
GROUP BY origine;

-- Contrôle 4 : seuil de 5 % de rejets
-- assert_true provoque une erreur, et donc un code retour non nul, si un
-- flux de cette exécution est en ECHEC_SEUIL. L'orchestration ne doit
-- alors pas lancer le Gold. Résultat NULL = tout est conforme.
SELECT assert_true(count(*) = 0) AS seuil_rejets_respecte
FROM audit
WHERE id_silver = '20261104_1' AND statut <> 'OK';
