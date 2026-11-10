-- =====================================================================
-- Projet CHU - Étape 6b : unicité des numéros de décès
-- Exécution : beeline -u jdbc:hive2://localhost:10000 -n cloudera -f 06b_unicite_deces.hql
-- Lancé dans Hive et non dans Impala : 24,8 millions de valeurs distinctes
-- dépassent la mémoire d'Impala sur la VM (512 Mo), Hive peut écrire sur disque.
-- Résultat attendu : 0 numéro en double.
-- =====================================================================
SELECT count(*) AS numeros_en_double
FROM (
    SELECT num_deces
    FROM gold.fait_deces
    GROUP BY num_deces
    HAVING count(*) > 1
) x;
