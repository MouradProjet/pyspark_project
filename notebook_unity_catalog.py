# Databricks notebook source
# =============================================================================
# 00_setup/init_catalog.py
# Création du catalog actuariat_dev + schemas (9 groupes) + volumes
# À exécuter UNE SEULE FOIS sur un cluster Unity Catalog (Single user, 16.4 LTS)
# =============================================================================

# COMMAND ----------

# ── Paramètres — à adapter à votre environnement ─────────────────────────────
ADLS_ACCOUNT = "adlsactuariat"        # ⚠️ votre compte de stockage ADLS Gen2
ENV          = "dev"
CATALOG      = f"actuariat_{ENV}"
BASE_PATH    = f"abfss://{ENV}@{ADLS_ACCOUNT}.dfs.core.windows.net"

# Vos 9 groupes de scripts — ⚠️ ajustez les noms exacts
GROUPES = [
    "cqs", "webxl", "gim", "cartographie", "ppna",
    "cbp", "sinistres", "provisions", "reporting",
]

print(f"Catalog cible : {CATALOG}")
print(f"Stockage      : {BASE_PATH}")
print(f"Groupes       : {len(GROUPES)}")

# COMMAND ----------

# ── 0. Vérifications préalables ──────────────────────────────────────────────
# Confirmer que Unity Catalog est accessible
metastore = spark.sql("SELECT current_metastore()").collect()[0][0]
print(f"✅ Metastore détecté : {metastore}")

# Confirmer que l'External Location existe pour ce chemin
print("\nExternal Locations disponibles :")
spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)
# ⚠️ Si aucune External Location ne couvre BASE_PATH, créez-la avant de continuer

# COMMAND ----------

# ── 1. Création du catalog ───────────────────────────────────────────────────
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG}
    MANAGED LOCATION '{BASE_PATH}/catalog/'
    COMMENT 'Environnement {ENV.upper()} - migration SAS actuariat'
""")
print(f"✅ Catalog créé : {CATALOG}")

spark.sql("SHOW CATALOGS LIKE 'actuariat*'").show()

# COMMAND ----------

# ── 2. Création des schemas (1 par groupe + shared) ──────────────────────────
spark.sql(f"USE CATALOG {CATALOG}")

for groupe in GROUPES:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG}.{groupe}
        MANAGED LOCATION '{BASE_PATH}/{groupe}/'
        COMMENT 'Groupe de scripts {groupe.upper()}'
    """)
    print(f"  ✅ Schema : {CATALOG}.{groupe}")

# Schema partagé pour les tables de référence communes
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.shared
    MANAGED LOCATION '{BASE_PATH}/shared/'
    COMMENT 'Tables de reference communes a tous les groupes'
""")
print(f"  ✅ Schema : {CATALOG}.shared")

spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()

# COMMAND ----------

# ── 3. Création des volumes (sources + exports par groupe) ───────────────────
for groupe in GROUPES:
    # Volume pour les fichiers sources (Excel, CSV venant du NAS)
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.{groupe}.sources
        COMMENT 'Fichiers sources Excel/CSV - groupe {groupe.upper()}'
    """)
    # Volume pour les exports Excel générés
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.{groupe}.exports_excel
        COMMENT 'Fichiers Excel generes - groupe {groupe.upper()}'
    """)
    print(f"  ✅ Volumes : {CATALOG}.{groupe}.sources + .exports_excel")

# Volume partagé pour les JARs (installation offline crealytics, sas7bdat)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.shared.libs
    COMMENT 'JARs pour installation offline (spark-excel, spark-sas7bdat)'
""")
print(f"  ✅ Volume : {CATALOG}.shared.libs")

# COMMAND ----------

# ── 4. Vérification finale ───────────────────────────────────────────────────
print("═══ Schemas créés ═══")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()

print("═══ Volumes du groupe CQS ═══")
spark.sql(f"SHOW VOLUMES IN {CATALOG}.cqs").show()

# COMMAND ----------

# ── 5. Test écriture/lecture (validation complète) ───────────────────────────
test_df = spark.createDataFrame([(1, "ok"), (2, "test")], ["id", "valeur"])

# Test table externe sur ADLS Gen2
test_df.write.format("delta").mode("overwrite") \
    .option("path", f"{BASE_PATH}/cqs/test_init/") \
    .saveAsTable(f"{CATALOG}.cqs.test_init")

# Vérifier qu'elle est bien dans Unity Catalog (pas spark_catalog) et sur ADLS
print("═══ Vérification table de test ═══")
spark.sql(f"SHOW CREATE TABLE {CATALOG}.cqs.test_init").show(truncate=False)
spark.sql(f"DESCRIBE DETAIL {CATALOG}.cqs.test_init") \
    .select("name", "location", "format").show(truncate=False)

# Nettoyage
spark.sql(f"DROP TABLE {CATALOG}.cqs.test_init")
print(f"\n🎉 Catalog {CATALOG} opérationnel — prêt pour la migration")
