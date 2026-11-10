# Cloud Healthcare Unit

Scripts d'implémentation physique de l'entrepôt de données décisionnel (EDD) du groupe CHU : chargement Lakehouse Bronze / Silver / Gold sous Hive, rapport de qualité et benchmark des modèles physiques.

Le dépôt ne contient que les scripts (Livrable 2). Les rapports, les jobs Talend et le tableau de bord sont livrés à part.

## Environnement

- VM Cloudera (CDH) : HDFS, YARN (3 Go, 4 vcores), Hive (`localhost:10000`, utilisateur `cloudera`), Impala (`localhost:21000`)
- Extraction : les jobs Talend J10 à J15 déposent dans `/bronze` des fichiers séparés par `;`, sans en-tête, en UTF-8
- Stockage : Silver et Gold en Parquet Snappy, chargés avec Hive on Spark

## Contenu

| Script | Moteur | Rôle |
|---|---|---|
| [bronze/01_bronze.hql](bronze/01_bronze.hql) | Hive | Bases `bronze`, `silver` et `gold`, tables externes sur les fichiers Talend (colonnes STRING) |
| [silver/02_silver.hql](silver/02_silver.hql) | Hive | Typage, normalisation, dédoublonnage, rejets (`silver.rejets`) et audit (`silver.audit`) des 11 flux |
| [gold/03_gold_partitioning.hql](gold/03_gold_partitioning.hql) | Hive | Constellation Gold (6 dimensions, 4 faits), scénario **v1** partitionné |
| [gold/03a_gold_flat.hql](gold/03a_gold_flat.hql) | Hive | Même modèle sans partition ni bucket, scénario **v0** de référence |
| [gold/03b_gold_bucketed.hql](gold/03b_gold_bucketed.hql) | Hive | v1 + buckets, scénario **v2** |
| [gold/04_impala_metadata.sql](gold/04_impala_metadata.sql) | Impala | `INVALIDATE METADATA`, `COMPUTE STATS`, preuve de l'élagage de partitions |
| [benchmark/05_benchmark.sh](benchmark/05_benchmark.sh) | Bash + Hive | 5 requêtes × 3 modèles × 3 mesures, résultats en CSV |
| [data_quality/06_qualite.sql](data_quality/06_qualite.sql) | Impala | Complétude par attribut, cohérence référentielle (FINESS), doublons |
| [data_quality/06b_unicite_deces.hql](data_quality/06b_unicite_deces.hql) | Hive | Unicité des numéros de décès (trop lourd pour Impala sur la VM) |

## Sources intégrées

| Source | Tables Bronze |
|---|---|
| 1. PostgreSQL opérationnel, plus `Hospitalisations.csv` | `patient`, `consultation`, `professionnel`, `specialites`, `diagnostics`, `hospitalisations` |
| 2. Référentiel des établissements (FINESS) | `etablissement`, `activite` |
| 3. Satisfaction e-Satis HAS (2017, 2019, 2020) | `esatis` |
| 4. Décès INSEE | `deces` |
| Complément : COG INSEE | `cog_departement`, `cog_region` |

## Modèle Gold

- **Faits** : `fait_consultation`, `fait_hospitalisation`, `fait_deces`, `fait_satisfaction`
- **Dimensions** : `dim_temps` (1880 à 2030), `dim_localisation` (département, région), `dim_etablissement`, `dim_patient`, `dim_diagnostic` (chapitres CIM-10), `dim_professionnel`
- Membres spéciaux `-1` (inconnu) et `-2` (plusieurs) : une clé étrangère n'est jamais NULL
- Les clés de substitution sont conservées d'une exécution à l'autre, les faits sont recalculés en entier

## Optimisation physique

| Scénario | Base | Stockage |
|---|---|---|
| v0 | `gold_flat` | Ni partition ni bucket |
| v1 | `gold_partitioning` | `fait_deces` (24,8 M lignes) et `fait_consultation` (1 M) partitionnés par `annee` |
| v2 | `gold_bucketed` | v1 + `fait_consultation` et `dim_professionnel` en 4 buckets triés sur `id_professionnel` (sort-merge bucket join) |

`fait_hospitalisation` et `fait_satisfaction` (quelques milliers de lignes) sont identiques dans les trois bases.

Requêtes du benchmark :

| Requête | Besoin | Effet mesuré |
|---|---|---|
| Q1 | Consultations par établissement, 2019-2020 | Élagage de partitions |
| Q2 | Taux d'hospitalisation par sexe et tranche d'âge | Témoin (bruit de mesure) |
| Q3 | Décès et âge moyen par région, 2019 | Élagage sur 25 M lignes |
| Q4 | Satisfaction pondérée par région, 2020 | Témoin (coût fixe) |
| Q5 | Consultations par spécialité, 2019-2020 | Jointure sur la clé des buckets |

## Exécution

À lancer dans l'ordre, chaque étape une fois la précédente terminée :

```bash
HIVE="beeline -u jdbc:hive2://localhost:10000 -n cloudera"
IMPALA="impala-shell -i localhost:21000"

$HIVE -f bronze/01_bronze.hql
$HIVE -f silver/02_silver.hql
$HIVE -f gold/03_gold_partitioning.hql
$HIVE -f gold/03a_gold_flat.hql
$HIVE -f gold/03b_gold_bucketed.hql
$IMPALA -f gold/04_impala_metadata.sql
bash benchmark/05_benchmark.sh               # QUERIES="Q1 Q5" pour n'en lancer que certaines
$IMPALA -f data_quality/06_qualite.sql
$HIVE -f data_quality/06b_unicite_deces.hql
```

Les étapes Silver, Gold et benchmark sont longues : les lancer avec `nohup ... > fichier.log 2>&1 &` comme indiqué dans leur en-tête. Le benchmark écrit `benchmark_results_<date>.csv` dans le répertoire courant et ses journaux dans `/tmp/hive_bench_logs/<date>/`.

## Points d'attention

- **Identifiants d'exécution en dur** : `20261104_1` (Silver) et `20261105_1` (Gold). Les remplacer partout dans `02_silver.hql`, `03_gold_partitioning.hql` et `06_qualite.sql` (requête 16) avant une nouvelle exécution, sinon l'audit mélange deux lancements.
- **Arrêts contrôlés** : `assert_true` interrompt le Silver si un flux dépasse 5 % de rejets, et le Gold refuse de démarrer si le Silver n'est pas conforme.
- **Beeline (CDH)** : aucun commentaire juste avant un `SET`, sinon il n'est plus reconnu.
- **Impala** : relancer `04_impala_metadata.sql` après chaque exécution de 03, 03a ou 03b. Impala ignore les buckets écrits par Hive, c'est pourquoi le benchmark tourne dans Hive (MapReduce).

## Données personnelles (RGPD)

- Patients et professionnels sont pseudonymisés par Talend (HMAC-SHA256) avant leur arrivée dans le Bronze.
- Le Silver remplace les pseudonymes par des clés entières (`cle_patient`, `cle_professionnel`).
- `dim_patient` ne garde que le sexe et l'année de naissance, et aucune identité n'est extraite du fichier des décès.
