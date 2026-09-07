# -*- coding: utf-8 -*-
"""
CHECK DATA TIA (DAAP) — construit DATABASE_ALL_PL_V3 :
filtre, agrège, renomme les postes, pivote POSTE en colonnes (GWP/Comms/Claims),
et calcule un row count par pays.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

arrete     = "2026_04_V2"
TIA_SCHEMA = "tia"

# ═══════════════════════════════════════════════════════════════════════
# 1. Filtre : retirer Type_Insurance in (11,8) sur les PREMIUM
# ═══════════════════════════════════════════════════════════════════════
df = (spark.table(f"{TIA_SCHEMA}.DATABASE_ALL_PL")
      .filter(~F.expr("Type_Insurance IN (11,8) AND POSTE IN ('PREMIUM')"))
      .drop("Type_Insurance"))
df.createOrReplaceTempView("DATABASE_ALL_PL_V3")

# ═══════════════════════════════════════════════════════════════════════
# 2. Agrégation : somme des montants par clé
# ═══════════════════════════════════════════════════════════════════════
df = spark.sql("""
    SELECT country, entity_name, POSTE, gl_period, sum(MONTANT) AS MONTANT
    FROM DATABASE_ALL_PL_V3
    GROUP BY country, entity_name, POSTE, gl_period
""")
df.createOrReplaceTempView("DATABASE_ALL_PL_V3")

# ═══════════════════════════════════════════════════════════════════════
# 3. Renommage des postes (PREMIUM→GWP, COMMISSION→Comms, CLAIM→Claims)
# ═══════════════════════════════════════════════════════════════════════
df = spark.table("DATABASE_ALL_PL_V3").withColumn(
    "POSTE",
    F.when(F.col("POSTE") == "PREMIUM", F.lit("GWP"))
     .when(F.col("POSTE") == "COMMISSION", F.lit("Comms"))
     .when(F.col("POSTE") == "CLAIM", F.lit("Claims"))
     .otherwise(F.col("POSTE")))
df.createOrReplaceTempView("DATABASE_ALL_PL_V3")

# ═══════════════════════════════════════════════════════════════════════
# 4. Dédup (proc sort nodupkey) puis PIVOT (proc transpose id POSTE)
# ═══════════════════════════════════════════════════════════════════════
# Le transpose étale les valeurs de POSTE en colonnes (GWP, Comms, Claims...).
# Le regroupement se fait par (country, entity_name, gl_period) — PAS par POSTE,
# puisque POSTE devient les colonnes.
df = spark.table("DATABASE_ALL_PL_V3").dropDuplicates(
    ["country", "entity_name", "POSTE", "gl_period"])

df = (df.groupBy("country", "entity_name", "gl_period")
        .pivot("POSTE")
        .agg(F.first("MONTANT")))
df.createOrReplaceTempView("DATABASE_ALL_PL_V3")

# ═══════════════════════════════════════════════════════════════════════
# 5. Remplacer les null par 0 sur les colonnes de postes
# ═══════════════════════════════════════════════════════════════════════
# Certaines colonnes peuvent ne pas exister si le poste est absent des données ;
# on ne remplit que celles présentes.
for col in ["Comms", "GWP", "Claims", "PS_PAID", "CLAIM_CED"]:
    if col in df.columns:
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
df.createOrReplaceTempView("DATABASE_ALL_PL_V3")

# ═══════════════════════════════════════════════════════════════════════
# 6. ROW COUNT par pays
# ═══════════════════════════════════════════════════════════════════════
count = spark.sql(f"""
    SELECT countryid_vorig, count(*) AS RowCount
    FROM {TIA_SCHEMA}.daap_level_1_dueonly
    GROUP BY countryid_vorig
""")
count.createOrReplaceTempView("count")

print("Check data TIA terminé.")
