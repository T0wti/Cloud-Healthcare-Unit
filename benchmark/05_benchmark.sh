#!/bin/bash
# =====================================================================
# Projet CHU - Benchmark des 3 modèles physiques du Gold
#   3 modèles x 5 requêtes x 3 mesures (+ 1 échauffement non compté)
#
# Nature du SQL  : ni DDL ni DML. Ce script ne crée et ne charge aucune
#                  table : il n'envoie à Hive que des lectures (SELECT,
#                  EXPLAIN, SHOW TABLES).
#                  Création des tables (DDL) et chargement des données (DML) :
#                  03_gold_partitioning.hql, 03a_gold_flat.hql, 03b_gold_bucketed.hql.
#
# Exécution sur la VM : cd ~/chu_scripts && nohup bash 05_benchmark.sh > bench.out 2>&1 &
# Suivi              : tail -f ~/chu_scripts/bench.out
# Prérequis          : 03_gold_partitioning.hql, 03a_gold_flat.hql, 03b_gold_bucketed.hql
#                      exécutés (statistiques Hive comprises). 04_impala_metadata.sql
#                      n'est pas nécessaire ici : tout passe par Hive.
#
# Modèles (mêmes tables, mêmes données, seul le stockage change) :
#   gold_flat      v0 : sans partition ni bucket
#   gold           v1 : fait_deces et fait_consultation partitionnés par année
#   gold_bucketed  v2 : v1 + fait_consultation et dim_professionnel en buckets
#                  triés sur id_professionnel (4 buckets)
#   fait_hospitalisation et fait_satisfaction sont identiques dans les trois
#   modèles (trop petites pour être partitionnées ou bucketées).
#
# Moteurs : tables chargées avec Hive on Spark, requêtes mesurées avec
#   MapReduce, le même pour les trois modèles (les compteurs CPU et HDFS
#   lus plus bas sont ceux de MapReduce).
#
# Temps mesuré   : celui que beeline affiche pour la requête
#                  ("N rows selected (X seconds)"), sans le démarrage de la
#                  JVM ni la connexion.
# Valeur retenue : médiane des 3 mesures (la moyenne est donnée pour info).
#                  Le passage d'échauffement n'est pas compté : le premier
#                  passage lit le disque, les suivants le cache.
# Échec          : requête en erreur ou au-delà de QUERY_TIMEOUT, notée
#                  ECHEC, jamais chronométrée.
# Contrôles      : plan de jointure (EXPLAIN) et empreinte md5 du résultat,
#                  comparée d'un modèle à l'autre.
# Sorties        : benchmark_results_<date>.csv (une ligne par requête et par
#                  modèle) et, dans /tmp/hive_bench_logs/<date>/, le plan, les
#                  résultats et les messages Hive de chaque passage.
# =====================================================================

# ---------------------------------------------------------------------
# Paramètres
# ---------------------------------------------------------------------
BEELINE_URL="jdbc:hive2://localhost:10000"
HIVE_USER="cloudera"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUTPUT_CSV="benchmark_results_${RUN_ID}.csv"
LOG_DIR="/tmp/hive_bench_logs/${RUN_ID}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-1800}"   # secondes par requête ; au-delà : ECHEC
# Requêtes à lancer (toutes par défaut). Exemple pour n'en relancer que deux :
#   QUERIES="Q1 Q5" nohup bash 05_benchmark.sh > bench.out 2>&1 &
QUERIES="${QUERIES:-Q1 Q2 Q3 Q4 Q5}"

# Bases comparées, et tables que chacune doit contenir (vérifié avant de lancer)
MODELS=("gold_flat" "gold" "gold_bucketed")
TABLES=(fait_consultation fait_hospitalisation fait_deces fait_satisfaction
        dim_etablissement dim_patient dim_localisation dim_professionnel)

# ---------------------------------------------------------------------
# Configuration Hive, identique pour les 3 modèles (passée à chaque beeline)
# ---------------------------------------------------------------------
# - Moteur MapReduce. YARN dispose de 3 Go et 4 vcores : ApplicationMaster
#   1 Go + 2 tâches de 1 Go.
# - nonstrict : le mode strict refuserait Q2 à Q5 (ORDER BY sans LIMIT) et
#   Q1 (CROSS JOIN).
# - compute.query.using.stats=false : les count(*) sont réellement calculés,
#   jamais lus dans les statistiques du métastore.
# - Seuil du map join abaissé à 2 Mo (au lieu de 30) : sous ce seuil, Hive
#   charge la dimension en mémoire dans une tâche locale (MapredLocalTask)
#   dont la JVM est petite. Le seuil porte sur la taille Parquet COMPRESSÉE,
#   bien plus petite qu'en mémoire :
#     dim_etablissement  416 702 lignes  10,70 Mo sur disque
#     dim_professionnel 1 048 576 lignes   6,32 Mo sur disque
#     dim_temps, dim_patient, dim_diagnostic, dim_localisation : <= 552 Ko
#   À 30 Mo puis 10 Mo, dim_professionnel passait encore en map join et la
#   tâche mourait : « OutOfMemoryError: Java heap space », return code 137
#   (Q1 gold_flat au 1er passage, Q5 gold_flat et gold aux deux passages).
#   À 2 Mo, les 4 petites dimensions restent en map join, les 2 grosses
#   passent en jointure classique (shuffle). Q1 à Q4 gardent le même plan
#   qu'à 10 Mo : seule Q5 change.
# - Jointures sur buckets (bucketmapjoin, sortmerge) : sans effet tant que les
#   deux tables ne sont pas en buckets triés sur la clé de jointure, donc
#   uniquement actives sur fait_consultation / dim_professionnel de
#   gold_bucketed (Q5).
CONF=(
  --hiveconf hive.execution.engine=mr
  --hiveconf hive.mapred.mode=nonstrict
  --hiveconf hive.compute.query.using.stats=false
  --hiveconf hive.auto.convert.join=true
  --hiveconf hive.auto.convert.join.noconditionaltask=true
  --hiveconf hive.auto.convert.join.noconditionaltask.size=2000000
  --hiveconf hive.mapjoin.smalltable.filesize=2000000
  --hiveconf hive.optimize.bucketmapjoin=true
  --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true
  --hiveconf hive.auto.convert.sortmerge.join=true
  --hiveconf hive.auto.convert.sortmerge.join.to.mapjoin=false
  --hiveconf hive.map.aggr=true
  --hiveconf hive.map.aggr.hash.percentmemory=0.5
  --hiveconf yarn.app.mapreduce.am.resource.mb=1024
  --hiveconf yarn.app.mapreduce.am.command-opts=-Xmx820m
  --hiveconf mapreduce.map.memory.mb=1024
  --hiveconf mapreduce.reduce.memory.mb=1024
  --hiveconf mapreduce.map.java.opts=-Xmx820m
  --hiveconf mapreduce.reduce.java.opts=-Xmx820m
  --hiveconf mapreduce.map.cpu.vcores=1
  --hiveconf mapreduce.reduce.cpu.vcores=1
  --hiveconf hive.exec.reducers.max=4
)

# ---------------------------------------------------------------------
# Les 5 requêtes : des SELECT uniquement (lecture seule, ni DDL ni DML).
# __DB__ est remplacé par chaque modèle.
# Une par table de faits, plus une jointure sur la clé des buckets.
# Chacune isole un effet différent du modèle physique :
#   Q1  Direction  : consultations par établissement, 2019-2020.
#       Élagage des partitions (2 années lues) et jointure avec la dimension
#       la plus volumineuse sur disque (dim_etablissement).
#   Q2  Praticiens : taux d'hospitalisation par sexe et tranche d'âge.
#       REQUÊTE TÉMOIN : fait_hospitalisation et dim_patient sont stockées
#       de la même façon dans les trois modèles. Les temps doivent être
#       égaux : leur écart donne le bruit de la mesure.
#   Q3  Direction  : décès et âge moyen par région, 2019.
#       Une année sur plusieurs décennies, 25 M lignes : effet maximal
#       de l'élagage des partitions.
#   Q4  Direction  : satisfaction pondérée par région, 2020.
#       REQUÊTE TÉMOIN : petite table (~3 500 lignes), identique dans les
#       trois modèles. Elle mesure le coût fixe d'une requête.
#   Q5  Praticiens : consultations par spécialité, 2019-2020.
#       Jointure fait_consultation / dim_professionnel (1 M x 1 M) sur
#       id_professionnel, la clé des buckets : sort-merge bucket join
#       possible dans gold_bucketed seulement.
# Q1 a un critère de départage (id) après le tri : avec LIMIT 20, deux
# ex æquo pourraient sinon sortir dans un ordre différent selon le modèle.
# ---------------------------------------------------------------------
Q1="WITH f AS (SELECT id_etablissement FROM __DB__.fait_consultation WHERE annee BETWEEN 2019 AND 2020 AND id_etablissement > 0), t AS (SELECT count(*) AS total FROM f) SELECT e.id_etablissement, e.raison_sociale, e.type_structure, count(*) AS nb_consultations, CAST(100 * count(*) / max(t.total) AS DECIMAL(5,2)) AS taux_pct FROM f JOIN __DB__.dim_etablissement e ON e.id_etablissement = f.id_etablissement CROSS JOIN t GROUP BY e.id_etablissement, e.raison_sociale, e.type_structure ORDER BY nb_consultations DESC, e.id_etablissement LIMIT 20;"

Q2="WITH pat AS (SELECT id_patient, sexe, CASE WHEN annee_naissance IS NULL THEN 'Inconnu' WHEN 2020 - annee_naissance < 18 THEN '0-17' WHEN 2020 - annee_naissance < 40 THEN '18-39' WHEN 2020 - annee_naissance < 65 THEN '40-64' ELSE '65+' END AS tranche FROM __DB__.dim_patient WHERE id_patient > 0), hosp AS (SELECT DISTINCT id_patient FROM __DB__.fait_hospitalisation) SELECT pat.sexe, pat.tranche, count(*) AS nb_patients, count(hosp.id_patient) AS nb_hospitalises, CAST(100 * count(hosp.id_patient) / count(*) AS DECIMAL(5,2)) AS taux_pct FROM pat LEFT JOIN hosp ON hosp.id_patient = pat.id_patient GROUP BY pat.sexe, pat.tranche ORDER BY pat.sexe, pat.tranche;"

Q3="SELECT l.region, count(*) AS nb_deces, CAST(avg(f.age) AS DECIMAL(4,1)) AS age_moyen FROM __DB__.fait_deces f JOIN __DB__.dim_localisation l ON l.id_localisation = f.id_localisation WHERE f.annee = 2019 GROUP BY l.region ORDER BY nb_deces DESC;"

Q4="SELECT l.region, CAST(sum(f.note_satisfaction * f.nb_avis) / sum(f.nb_avis) AS DECIMAL(5,2)) AS note_ponderee, sum(f.nb_avis) AS nb_avis, count(*) AS nb_etablissements FROM __DB__.fait_satisfaction f JOIN __DB__.dim_localisation l ON l.id_localisation = f.id_localisation WHERE f.annee = 2020 AND f.note_satisfaction IS NOT NULL GROUP BY l.region ORDER BY note_ponderee DESC;"

Q5="SELECT pr.specialite, count(*) AS nb_consultations, count(DISTINCT f.id_patient) AS nb_patients FROM __DB__.fait_consultation f JOIN __DB__.dim_professionnel pr ON pr.id_professionnel = f.id_professionnel WHERE f.annee BETWEEN 2019 AND 2020 GROUP BY pr.specialite ORDER BY nb_consultations DESC, pr.specialite;"

# ---------------------------------------------------------------------
# Fonctions
# ---------------------------------------------------------------------

# Arrête les jobs YARN restés actifs après un dépassement de délai
# (la VM est dédiée au benchmark pendant son exécution).
kill_yarn_jobs() {
    local app
    for app in $(yarn application -list -appStates ACCEPTED,RUNNING 2>/dev/null |
                 awk '$1 ~ /^application_/ { print $1 }'); do
        yarn application -kill "$app" > /dev/null 2>&1
    done
}

# Lance une requête : résultats dans $2, messages de Hive dans $3.
# Affiche le temps en secondes, ou ECHEC.
run_once() {
    local sql="$1" out="$2" log="$3" rc t
    timeout "$QUERY_TIMEOUT" beeline -u "$BEELINE_URL" -n "$HIVE_USER" "${CONF[@]}" \
        --outputformat=tsv2 --showHeader=true -e "$sql" > "$out" 2> "$log"
    rc=$?
    if [ "$rc" -eq 124 ]; then
        echo "TIMEOUT : requete arretee apres ${QUERY_TIMEOUT} s" >> "$log"
        kill_yarn_jobs
        echo "ECHEC"
        return
    fi
    # Le séparateur décimal suit la langue de la VM ("12.3" ou "12,3").
    t=$(cat "$log" "$out" | grep -oE "rows? selected \([0-9.,]+ seconds\)" | tail -n 1 |
        grep -oE "[0-9][0-9.,]*" | tr ',' '.')
    if [ "$rc" -ne 0 ] || grep -q "^Error:" "$log" || [ -z "$t" ]; then
        echo "ECHEC"
    else
        echo "$t"
    fi
}

# Enregistre le plan (EXPLAIN) dans $2 et affiche les jointures qu'il contient :
# smb (sort-merge bucket), mapjoin (table de hachage en mémoire),
# shuffle (jointure classique avec redistribution des données).
# Si le plan prévoit une jointure en mémoire avec repli en shuffle, les deux
# apparaissent : le fichier du plan fait foi.
explain_join() {
    local sql="$1" file="$2" types=""
    timeout 300 beeline -u "$BEELINE_URL" -n "$HIVE_USER" "${CONF[@]}" --silent=true \
        --outputformat=tsv2 --showHeader=false -e "EXPLAIN $sql" > "$file" 2>&1
    if grep -q "^Error:" "$file" || [ ! -s "$file" ]; then
        echo "?"
        return
    fi
    grep -q "Sorted Merge Bucket Map Join Operator" "$file" && types="smb"
    grep -v "Sorted Merge Bucket" "$file" | grep -q "Map Join Operator" && types="${types:+$types+}mapjoin"
    grep -v "Map Join Operator" "$file" | grep -q "Join Operator" && types="${types:+$types+}shuffle"
    echo "${types:-aucune}"
}

# Temps CPU total des jobs MapReduce, en secondes
# (le format de Hive va de "230 msec" à "1 minutes 5 seconds 230 msec").
cpu_seconds() {
    grep -h "Total MapReduce CPU Time Spent:" "$1" | tail -n 1 | sed 's/.*Spent: //' |
    awk '{ s = 0
           for (i = 1; i < NF; i++) {
               u = $(i + 1)
               if (u ~ /^days?$/)         s += $i * 86400
               else if (u ~ /^hours?$/)   s += $i * 3600
               else if (u ~ /^minutes?$/) s += $i * 60
               else if (u ~ /^seconds?$/) s += $i
               else if (u ~ /^msec$/)     s += $i / 1000
           }
           printf "%.2f", s }'
}

# Volume lu sur HDFS par tous les jobs MapReduce de la requête, en Mo
# (les tables chargées en mémoire par la tâche locale d'un mapjoin n'y sont pas).
hdfs_read_mb() {
    grep -oh "HDFS Read: [0-9]*" "$1" | awk '{ s += $3 } END { printf "%.2f", s / 1048576 }'
}

# Empreinte du résultat, pour vérifier que les 3 modèles renvoient la même chose
result_hash() {
    grep -vE "rows? selected" "$1" | LC_ALL=C sort | md5sum | cut -c1-32
}

# Mesure une requête sur les 3 modèles et écrit une ligne de CSV par modèle :
# plan (EXPLAIN), 1 échauffement, 3 passages chronométrés, puis médiane,
# moyenne, volume HDFS lu, temps CPU et empreinte du résultat (ceux du
# 3e passage). Le premier modèle sert de référence pour l'empreinte.
run_benchmark_query() {
    local q_num="$1" q_desc="$2" q_sql="$3"
    local ref_hash="" db sql base t run statut med avg hdfs_mb cpu_sec hash resultat jointure
    local times

    # Requête non demandée dans QUERIES : ignorée
    [[ " $QUERIES " == *" $q_num "* ]] || return 0

    echo ""
    echo "=========================================================================="
    echo " REQUETE $q_num : $q_desc"
    echo "=========================================================================="

    for db in "${MODELS[@]}"; do
        sql="${q_sql//__DB__/$db}"
        base="$LOG_DIR/${q_num}_${db}"
        times=("" "" "")
        med=""; avg=""; hdfs_mb=""; cpu_sec=""; resultat=""; statut="OK"

        echo -n "  -> Modele [$db] : "

        jointure=$(explain_join "$sql" "${base}_explain.txt")
        echo -n "plan ${jointure} | "

        t=$(run_once "$sql" "${base}_run0.out" "${base}_run0.log")
        if [ "$t" = "ECHEC" ]; then
            statut="ECHEC"
            echo "ECHEC des l'echauffement (voir ${base}_run0.log)"
        else
            echo -n "echauffement ${t}s | "
            for run in 1 2 3; do
                t=$(run_once "$sql" "${base}_run${run}.out" "${base}_run${run}.log")
                times[$((run - 1))]="$t"
                echo -n "Run $run: ${t}s | "
                [ "$t" = "ECHEC" ] && statut="ECHEC"
            done

            if [ "$statut" = "ECHEC" ]; then
                echo "=> ECHEC (voir ${base}_run*.log)"
            else
                med=$(printf "%s\n" "${times[@]}" | LC_ALL=C sort -n | sed -n 2p)
                avg=$(awk "BEGIN { printf \"%.2f\", (${times[0]} + ${times[1]} + ${times[2]}) / 3 }")
                hdfs_mb=$(hdfs_read_mb "${base}_run3.log")
                cpu_sec=$(cpu_seconds "${base}_run3.log")
                [ -z "$cpu_sec" ] && cpu_sec="N/A"

                hash=$(result_hash "${base}_run3.out")
                if [ -z "$ref_hash" ]; then
                    ref_hash="$hash"; resultat="reference"
                elif [ "$hash" = "$ref_hash" ]; then
                    resultat="identique"
                else
                    resultat="DIFFERENT"
                fi
                echo "=> MEDIANE: ${med}s (resultat ${resultat})"
            fi
        fi

        echo "${q_num},\"${q_desc}\",${db},${times[0]},${times[1]},${times[2]},${med},${avg},${hdfs_mb},${cpu_sec},${jointure},${resultat},${statut}" >> "$OUTPUT_CSV"
    done
}

# ---------------------------------------------------------------------
# Vérifications préalables
# ---------------------------------------------------------------------
for cmd in beeline timeout yarn md5sum; do
    if ! command -v "$cmd" > /dev/null; then
        echo "ERREUR : commande $cmd introuvable. Benchmark arrete."
        exit 1
    fi
done

# Chaque modèle doit contenir les tables interrogées
for db in "${MODELS[@]}"; do
    if ! have=$(beeline -u "$BEELINE_URL" -n "$HIVE_USER" --silent=true --showHeader=false \
                --outputformat=tsv2 -e "SHOW TABLES IN $db;" 2>/dev/null); then
        echo "ERREUR : HiveServer2 ne repond pas, ou la base $db n'existe pas. Benchmark arrete."
        exit 1
    fi
    for tbl in "${TABLES[@]}"; do
        if ! grep -qx "$tbl" <<< "$have"; then
            echo "ERREUR : la table $db.$tbl n'existe pas. Benchmark arrete."
            exit 1
        fi
    done
done

# ---------------------------------------------------------------------
# Lancement
# ---------------------------------------------------------------------
mkdir -p "$LOG_DIR"
printf "%s %s\n" "${CONF[@]}" > "$LOG_DIR/configuration.txt"

echo "Requete,Description,Modele,Run1_s,Run2_s,Run3_s,Mediane_s,Moyenne_s,HDFS_Read_MB,CPU_Time_s,Jointure,Resultat,Statut" > "$OUTPUT_CSV"

echo "=========================================================================="
echo "    BENCHMARK CHU : 3 MODELES x 5 REQUETES x 3 MESURES (+1 ECHAUFFEMENT)"
echo "    Debut             : $(date '+%d/%m/%Y %H:%M:%S')"
echo "    Requetes          : $QUERIES"
echo "    Fichier de sortie : $OUTPUT_CSV"
echo "    Logs et resultats : $LOG_DIR"
echo "=========================================================================="

run_benchmark_query "Q1" "Consultations par Etablissement (2019-2020)" "$Q1"
run_benchmark_query "Q2" "Hospitalisations par Sexe et Tranche d'Age (temoin)" "$Q2"
run_benchmark_query "Q3" "Mortalite et Age Moyen par Region (2019 - 25M lignes)" "$Q3"
run_benchmark_query "Q4" "Satisfaction et Note Ponderee par Region (2020 - temoin)" "$Q4"
run_benchmark_query "Q5" "Consultations par Specialite (jointure id_professionnel)" "$Q5"

# ---------------------------------------------------------------------
# Bilan : nombre d'échecs et de résultats différents, tableau des médianes
# ---------------------------------------------------------------------
nb_echecs=$(grep -c ",ECHEC$" "$OUTPUT_CSV")
nb_diff=$(grep -c ",DIFFERENT," "$OUTPUT_CSV")

echo ""
echo "=========================================================================="
echo " BENCHMARK TERMINE le $(date '+%d/%m/%Y %H:%M:%S')"
echo " $nb_echecs echec(s), $nb_diff resultat(s) different(s)"
echo " Fichier de synthese : $OUTPUT_CSV"
echo " Logs et resultats    : $LOG_DIR"
echo " En cas de DIFFERENT  : diff <modele1>_run3.out <modele2>_run3.out dans les logs"
echo "=========================================================================="
echo " Medianes (secondes) :"
awk -F, -v models="${MODELS[*]}" '
    NR > 1 {
        v[$1 SUBSEP $3] = ($13 == "OK") ? $7 : "ECHEC"
        if (!($1 in seen)) { seen[$1] = 1; order[++n] = $1 }
    }
    END {
        nm = split(models, m, " ")
        printf "   %-5s", "Req"
        for (j = 1; j <= nm; j++) printf " %14s", m[j]
        printf "\n"
        for (i = 1; i <= n; i++) {
            printf "   %-5s", order[i]
            for (j = 1; j <= nm; j++) printf " %14s", v[order[i] SUBSEP m[j]]
            printf "\n"
        }
    }' "$OUTPUT_CSV"
