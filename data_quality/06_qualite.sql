-- =====================================================================
-- Projet CHU - Étape 6 : rapport de qualité des données intégrées
-- Exécution : impala-shell -i localhost:21000 -f 06_qualite.sql > 06.log 2>&1
--
-- Lecture seule : aucune requête ne modifie les données.
-- Source des chiffres de la partie VI du Livrable 2 :
--   Complétude par attribut (Silver, puis membres -1 et -2 du Gold)
--   Cohérence référentielle et codes FINESS
--   Doublons et unicité des clés
-- Complétude = count(colonne) / count(*) : count(colonne) ne compte que
-- les valeurs non NULL, et le Silver remplace les chaînes vides par NULL.
-- L'unicité des numéros de décès se vérifie dans Hive (06b_unicite_deces.hql) :
-- Impala manque de mémoire sur 24,8 millions de valeurs distinctes.
-- =====================================================================

-- 0. Métadonnées : le Silver et le Gold ont été recréés par Hive
INVALIDATE METADATA;

-- 1. Exécutions présentes dans l'audit (identifiant à reporter en 16)
SELECT id_silver, job, max(date_execution) AS date_execution, count(*) AS nb_flux
FROM silver.audit
GROUP BY id_silver, job
ORDER BY date_execution DESC
LIMIT 6;

-- =====================================================================
-- VI.2 COMPLÉTUDE PAR ATTRIBUT
-- =====================================================================

-- 2. Patients (sexe I = inconnu)
SELECT count(*) AS nb_lignes,
       CAST(100 * sum(CASE WHEN sexe <> 'I' THEN 1 ELSE 0 END) / count(*) AS DECIMAL(5,2)) AS sexe_pct,
       CAST(100 * count(date_naissance) / count(*) AS DECIMAL(5,2)) AS date_naissance_pct
FROM silver.patient;

-- 3. Professionnels
SELECT count(*) AS nb_lignes,
       CAST(100 * count(profession) / count(*) AS DECIMAL(5,2)) AS profession_pct,
       CAST(100 * count(categorie_professionnelle) / count(*) AS DECIMAL(5,2)) AS categorie_pct,
       CAST(100 * count(code_specialite) / count(*) AS DECIMAL(5,2)) AS code_specialite_pct,
       CAST(100 * count(specialite) / count(*) AS DECIMAL(5,2)) AS specialite_pct
FROM silver.professionnel;

-- 4. Consultations
SELECT count(*) AS nb_lignes,
       CAST(100 * count(id_patient_pseudo) / count(*) AS DECIMAL(5,2)) AS patient_pct,
       CAST(100 * count(id_prof_pseudo) / count(*) AS DECIMAL(5,2)) AS professionnel_pct,
       CAST(100 * count(code_diagnostic) / count(*) AS DECIMAL(5,2)) AS diagnostic_pct,
       CAST(100 * count(date_consultation) / count(*) AS DECIMAL(5,2)) AS date_pct,
       CAST(100 * count(heure_debut) / count(*) AS DECIMAL(5,2)) AS heure_pct
FROM silver.consultation;

-- 5. Hospitalisations
SELECT count(*) AS nb_lignes,
       CAST(100 * count(id_patient_pseudo) / count(*) AS DECIMAL(5,2)) AS patient_pct,
       CAST(100 * count(identifiant_organisation) / count(*) AS DECIMAL(5,2)) AS organisation_pct,
       CAST(100 * count(code_diagnostic) / count(*) AS DECIMAL(5,2)) AS diagnostic_pct,
       CAST(100 * count(date_entree) / count(*) AS DECIMAL(5,2)) AS date_entree_pct,
       CAST(100 * count(duree_sejour_jours) / count(*) AS DECIMAL(5,2)) AS duree_pct
FROM silver.hospitalisation;

-- 6. Décès : date complète et année mesurées à part
-- (une date incomplète AAAA ou AAAA-MM garde son année)
SELECT count(*) AS nb_lignes,
       CAST(100 * sum(CASE WHEN sexe <> 'I' THEN 1 ELSE 0 END) / count(*) AS DECIMAL(5,2)) AS sexe_pct,
       CAST(100 * count(date_naissance) / count(*) AS DECIMAL(5,2)) AS naissance_complete_pct,
       CAST(100 * count(annee_naissance) / count(*) AS DECIMAL(5,2)) AS annee_naissance_pct,
       CAST(100 * count(date_deces) / count(*) AS DECIMAL(5,2)) AS deces_complet_pct,
       CAST(100 * count(annee_deces) / count(*) AS DECIMAL(5,2)) AS annee_deces_pct,
       CAST(100 * count(code_lieu_deces) / count(*) AS DECIMAL(5,2)) AS lieu_pct,
       CAST(100 * count(code_departement) / count(*) AS DECIMAL(5,2)) AS departement_pct
FROM silver.deces;

-- 7. Établissements par type : identifiants et localisation
-- Le FINESS ne concerne que les organisations F (absence structurelle ailleurs)
SELECT type_structure, origine, count(*) AS nb_lignes,
       CAST(100 * count(num_finess_geo) / count(*) AS DECIMAL(5,2)) AS finess_geo_pct,
       CAST(100 * count(num_finess_juridique) / count(*) AS DECIMAL(5,2)) AS finess_juridique_pct,
       CAST(100 * count(code_commune) / count(*) AS DECIMAL(5,2)) AS code_commune_pct,
       CAST(100 * count(code_departement) / count(*) AS DECIMAL(5,2)) AS departement_pct
FROM silver.etablissement
GROUP BY type_structure, origine
ORDER BY type_structure, origine;

-- 7b. Établissements par type : attributs descriptifs
-- Une raison sociale vide est remplacée par l'identifiant (fiche 3.3) :
-- on compte donc celles réellement fournies par la source
SELECT type_structure, origine, count(*) AS nb_lignes,
       sum(CASE WHEN raison_sociale <> identifiant_organisation THEN 1 ELSE 0 END) AS raison_sociale_source,
       count(commune) AS commune,
       count(code_postal) AS code_postal
FROM silver.etablissement
GROUP BY type_structure, origine
ORDER BY type_structure, origine;

-- 8. Satisfaction : note par année et par classe
-- Les notes vides doivent correspondre exactement aux classes DI, NR et NV
SELECT annee, classement, count(*) AS nb_lignes, count(note_satisfaction) AS avec_note
FROM silver.satisfaction
GROUP BY annee, classement
ORDER BY annee, classement;

-- 9. Satisfaction : taux de recommandation et nombre d'avis par année
-- Le taux n'existe pas en 2017 (colonnes absentes du fichier)
SELECT annee, count(*) AS nb_lignes,
       CAST(100 * count(taux_recommandation) / count(*) AS DECIMAL(5,2)) AS taux_reco_pct,
       CAST(100 * count(nb_avis) / count(*) AS DECIMAL(5,2)) AS nb_avis_pct
FROM silver.satisfaction
GROUP BY annee
ORDER BY annee;

-- 9b. Satisfaction : tous les attributs, classes A à D contre DI, NR et NV
SELECT annee,
       CASE WHEN upper(classement) IN ('DI', 'NR', 'NV') THEN 'DI/NR/NV' ELSE 'A a D' END AS classes,
       count(*) AS lignes,
       count(note_satisfaction) AS note, count(nb_avis) AS nb_avis,
       count(score_accueil) AS accueil, count(score_pecinf) AS pec_infirmier,
       count(score_pecmed) AS pec_medicale, count(score_chambre) AS chambre,
       count(score_repas) AS repas, count(score_sortie) AS sortie,
       count(taux_recommandation) AS taux_reco, count(nb_reco) AS nb_reco,
       count(raison_sociale) AS raison_sociale, count(region) AS region,
       count(num_finess_juridique) AS finess_juridique
FROM silver.satisfaction
GROUP BY 1, 2
ORDER BY 1, 2;

-- 9c. Référentiels : diagnostics, spécialités, départements et régions
SELECT count(*) AS lignes, count(code_diagnostic) AS code,
       count(libelle_diagnostic) AS libelle, count(code_categorie) AS categorie
FROM silver.diagnostic;

SELECT count(*) AS lignes, count(code_specialite) AS code, count(specialite) AS libelle
FROM silver.specialite;

SELECT origine, count(*) AS lignes, count(departement) AS departement,
       count(code_region) AS code_region, count(region) AS region
FROM silver.localisation
GROUP BY origine;

-- 9d. Diagnostics sans chapitre CIM-10 (rejetés par le Gold), 0 attendu
SELECT count(*) AS diagnostics_sans_chapitre
FROM silver.rejets
WHERE flux = 'gold_dim_diagnostic';

-- 9e. Clés : une ligne sans clé est rejetée en Silver, d'où 100 % dans les tables
SELECT flux, count(*) AS lignes_sans_cle
FROM silver.rejets
WHERE motif = 'cle absente'
GROUP BY flux
ORDER BY flux;

-- 10. Complétude côté Gold : membres -1 et -2 de chaque fait, dernier chargement
SELECT fait, nb_lignes, patient_m1, diagnostic_m1, etablissement_m1, etablissement_m2,
       temps_m1, localisation_m1, localisation_m2, professionnel_m1, age_null, date_execution
FROM gold.controle_membres
ORDER BY date_execution DESC, fait
LIMIT 4;

-- =====================================================================
-- VI.3 COHÉRENCE RÉFÉRENTIELLE ET CODES FINESS
-- =====================================================================

-- 11. Format des FINESS : 9 caractères, 2A ou 2B admis en Corse, 0 invalide attendu
SELECT count(*) AS nb_finess_geo,
       sum(CASE WHEN NOT (num_finess_geo RLIKE '^[0-9][0-9AB][0-9]{7}$') THEN 1 ELSE 0 END) AS geo_invalides,
       sum(CASE WHEN num_finess_juridique IS NOT NULL
                 AND NOT (num_finess_juridique RLIKE '^[0-9][0-9AB][0-9]{7}$') THEN 1 ELSE 0 END) AS juridique_invalides
FROM silver.etablissement
WHERE num_finess_geo IS NOT NULL;

-- 12. FINESS et département de la commune, en métropole, 0 incohérent attendu
SELECT count(*) AS nb_compares,
       sum(CASE WHEN substr(num_finess_geo, 1, 2) <> code_departement THEN 1 ELSE 0 END) AS incoherents
FROM silver.etablissement
WHERE origine = 'referentiel' AND num_finess_geo IS NOT NULL AND code_departement IS NOT NULL
  AND substr(num_finess_geo, 1, 2) NOT IN ('97', '98');

-- 13. Établissements notés par e-Satis présents dans le référentiel, par année
SELECT s.annee, count(*) AS nb_resultats,
       sum(CASE WHEN e.origine = 'referentiel' THEN 1 ELSE 0 END) AS dans_referentiel,
       sum(CASE WHEN e.origine = 'esatis' THEN 1 ELSE 0 END) AS ajoutes_depuis_esatis
FROM silver.satisfaction s
JOIN silver.etablissement e ON e.identifiant_organisation = s.identifiant_organisation
GROUP BY s.annee
ORDER BY s.annee;

-- 14. Liens hospitalisation et activité vers établissements et professionnels
SELECT 'hospitalisation -> etablissement' AS lien, count(*) AS nb, count(e.identifiant_organisation) AS trouves
FROM silver.hospitalisation h LEFT JOIN silver.etablissement e ON e.identifiant_organisation = h.identifiant_organisation
UNION ALL
SELECT 'activite -> etablissement', count(*), count(e.identifiant_organisation)
FROM silver.activite a LEFT JOIN silver.etablissement e ON e.identifiant_organisation = a.identifiant_organisation
UNION ALL
SELECT 'activite -> professionnel', count(*), count(p.id_prof_pseudo)
FROM silver.activite a LEFT JOIN silver.professionnel p ON p.id_prof_pseudo = a.id_prof_pseudo;

-- 15. Liens des consultations vers patients, professionnels et diagnostics
SELECT count(*) AS nb, count(p.id_patient_pseudo) AS patient_trouve,
       count(pr.id_prof_pseudo) AS professionnel_trouve, count(d.code_diagnostic) AS diagnostic_trouve
FROM silver.consultation c
LEFT JOIN silver.patient p ON p.id_patient_pseudo = c.id_patient_pseudo
LEFT JOIN silver.professionnel pr ON pr.id_prof_pseudo = c.id_prof_pseudo
LEFT JOIN silver.diagnostic d ON d.code_diagnostic = c.code_diagnostic;

-- =====================================================================
-- VI.4 DOUBLONS ET UNICITÉ
-- =====================================================================

-- 16. Bilan par flux : remplacer 20261104_1 par l'identifiant Silver trouvé en 1
SELECT flux, lignes_lues, lignes_ecrites, lignes_doublon, lignes_rejetees, lignes_ignorees, taux_rejet, statut
FROM silver.audit
WHERE id_silver = '20261104_1'
ORDER BY flux;

-- 17. Motifs des rejets par flux
SELECT flux, motif, count(*) AS nb
FROM silver.rejets
GROUP BY flux, motif
ORDER BY flux, nb DESC;

-- 18. Unicité des clés des consultations et des hospitalisations
-- (nb doit être égal à distincts, décès vérifiés dans 06b_unicite_deces.hql)
SELECT 'fait_consultation' AS fait, count(*) AS nb, count(DISTINCT num_consultation) AS distincts
FROM gold.fait_consultation;

SELECT 'fait_hospitalisation' AS fait, count(*) AS nb, count(DISTINCT num_hospitalisation) AS distincts
FROM gold.fait_hospitalisation;
