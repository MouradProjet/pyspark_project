# -*- coding: utf-8 -*-
"""
PARAMÈTRES RULING & RÉASSURANCE
- Import des UEP Rules Production, nettoyage, split Forecast/PS/L, export
- Import de la cartographie réassurance, calcul des taux QP, exports

Note : les PROC EXPORT (écriture Excel) sont traduits en écritures crealytics.
Vérifiez les chemins et le comportement multi-feuilles selon votre environnement.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES (à compléter par l'utilisateur)
# ═══════════════════════════════════════════════════════════════════════
arrete       = "2026_06_Prov"
N            = 2026
version_ricp = "RICP 20260626"
quarter      = "Q22026"

BASE = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP"


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════
def import_excel(fichier, feuille, vue, plage=None):
    """Lit une feuille Excel dans une temp view (crealytics)."""
    addr = f"'{feuille}'!A1" if feuille else "A1"
    df = (spark.read.format("com.crealytics.spark.excel")
          .option("dataAddress", addr)
          .option("header", "true")
          .load(fichier))
    df.createOrReplaceTempView(vue)
    return df


def export_excel(df, outfile, sheet):
    """Écrit un DataFrame dans une feuille Excel (crealytics)."""
    (df.write.format("com.crealytics.spark.excel")
       .option("dataAddress", f"'{sheet}'!A1")
       .option("header", "true")
       .mode("overwrite")
       .save(outfile))


# ═══════════════════════════════════════════════════════════════════════
# 1. IMPORT UEP RULES PRODUCTION
# ═══════════════════════════════════════════════════════════════════════
importation = (f"{BASE}/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling"
               f"/UEP Rules Production (All Countries).xlsx")
uep = import_excel(importation, "Sheet 1", "UEP_RULES_PROD")

# Filtre : retirer les lignes entièrement vides (country+cover+language vides)
uep = uep.filter(~F.expr("COUNTRY = '' AND COVER = '' AND LANGUAGE = ''"))
uep.createOrReplaceTempView("uep_rules_prod")

# Liste distincte des DIM08 (règles)
uep_rules = spark.table("uep_rules_prod").select("DIM08").dropDuplicates(["DIM08"])
uep_rules.createOrReplaceTempView("uep_rules")

# ═══════════════════════════════════════════════════════════════════════
# 2. NETTOYAGE & DÉRIVATION DES DATES
# ═══════════════════════════════════════════════════════════════════════
uep_rules_prod_ = (spark.table("uep_rules_prod")
    # Normalisation des noms de règles
    .withColumn("DIM08",
        F.when(F.col("DIM08") == "r_12", F.lit("r12"))
         .when(F.col("DIM08") == "r_78p", F.lit("r78"))
         .when(F.col("DIM08") == "r_78m", F.lit("r78m"))
         .when(F.col("DIM08") == "(1-v)*r_12+v*r_78p", F.lit("V"))
         .when(F.col("DIM08") == "1.5*r_12-0.5*r_78p", F.lit("r45m"))
         .otherwise(F.col("DIM08")))
    # Conversions numériques (*1 en SAS)
    .withColumn("DIM11_", F.col("DIM11").cast("double"))
    .withColumn("DIM07_", F.col("DIM07").cast("double"))
    # Découpage des dates DIM20 (format jour.mois.an) → date
    .withColumn("DIM20_jour", F.split("DIM20", r"\.").getItem(0))
    .withColumn("DIM20_mois", F.split("DIM20", r"\.").getItem(1))
    .withColumn("DIM20_an",   F.split("DIM20", r"\.").getItem(2))
    # mdy(mois, jour, an) SAS → make_date(an, mois, jour) Spark
    .withColumn("DIM20_new",
        F.make_date(F.col("DIM20_an").cast("int"),
                    F.col("DIM20_mois").cast("int"),
                    F.col("DIM20_jour").cast("int")))
    .withColumn("DIM19_jour", F.split("DIM19", r"\.").getItem(0))
    .withColumn("DIM19_mois", F.split("DIM19", r"\.").getItem(1))
    .withColumn("DIM19_an",   F.split("DIM19", r"\.").getItem(2))
    .withColumn("DIM19_new",
        F.make_date(F.col("DIM19_an").cast("int"),
                    F.col("DIM19_mois").cast("int"),
                    F.col("DIM19_jour").cast("int")))
    .drop("DIM11"))
uep_rules_prod_.createOrReplaceTempView("uep_rules_prod_")

# ═══════════════════════════════════════════════════════════════════════
# 3. RÉORDONNANCEMENT & RENOMMAGE (proc sql)
# ═══════════════════════════════════════════════════════════════════════
uep_rules_prod_2 = spark.sql("""
    SELECT COUNTRY, COVER, TABLE_NAME, LANGUAGE,
           DIM01, DIM02, DIM03, DIM04, DIM05, DIM06,
           DIM07_ AS DIM07, DIM08, DIM09, DIM10,
           DIM11_ AS DIM11, DIM12, DIM13, DIM14, DIM15, DIM16, DIM17, DIM18,
           DIM19_new AS DIM19, DIM20_new AS DIM20,
           START_DATE, END_DATE, SORT_NO, TIMESTAMP, USERID, RECORD_VERSION
    FROM uep_rules_prod_
""")
uep_rules_prod_2.createOrReplaceTempView("uep_rules_prod_2")

# ═══════════════════════════════════════════════════════════════════════
# 4. SPLIT Forecast / PS / L selon DIM09
# ═══════════════════════════════════════════════════════════════════════
Ruling_Forecast = spark.table("uep_rules_prod_2").filter(F.col("DIM09") == "F")
Ruling_Forecast.createOrReplaceTempView("Ruling_Forecast")

Ruling_PS = spark.table("uep_rules_prod_2").filter(F.col("DIM09") == "P")
Ruling_PS.createOrReplaceTempView("Ruling_PS")

Ruling_L = spark.table("uep_rules_prod_2").filter(F.col("DIM09") == "L")
Ruling_L.createOrReplaceTempView("Ruling_L")

# ═══════════════════════════════════════════════════════════════════════
# 5. COMPTAGES (contrôle)
# ═══════════════════════════════════════════════════════════════════════
NBR1 = spark.table("uep_rules_prod").count()
NBR2 = Ruling_Forecast.count()
NBR3 = Ruling_PS.count()
NBR4 = Ruling_L.count()

print(f"NOTE: la table uep_rules_prod contient {NBR1} lignes")
print(f"NOTE: la table Ruling_Forecast contient {NBR2}")
print(f"NOTE: la table Ruling_PS contient {NBR3}")
print(f"NOTE: la table Ruling_L contient {NBR4}")
print(f"NOTE: la table spliter contient {NBR1 - (NBR3 + NBR2 + NBR4)}")   # %eval → calcul Python

# ═══════════════════════════════════════════════════════════════════════
# 6. EXPORT RULING
# ═══════════════════════════════════════════════════════════════════════
chemin_output_ruling = f"{BASE}/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling"
export_excel(Ruling_PS,       f"{chemin_output_ruling}/Ruling_{quarter}.xlsx", "PS")
export_excel(Ruling_Forecast, f"{chemin_output_ruling}/Ruling_{quarter}.xlsx", "Forecast")


# ═══════════════════════════════════════════════════════════════════════
# 7. PARAMÈTRES RÉASSURANCE — import cartographie
# ═══════════════════════════════════════════════════════════════════════
reass = (f"{BASE}/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling"
         f"/{version_ricp}.xlsx")
# IMP_EXCEL2 = import sans SHEET (première feuille)
carto = import_excel(reass, None, "cartographie_reassurance")
# (les attrib label du SAS sont purement documentaires — ignorés)

# ═══════════════════════════════════════════════════════════════════════
# 8. CONVERSIONS NUMÉRIQUES + CALCUL DES CLÉS ET TAUX QP
# ═══════════════════════════════════════════════════════════════════════
carto_1 = (spark.table("cartographie_reassurance")
    .withColumn("DIM06_", F.col("DIM06").cast("double"))
    .withColumn("DIM07_", F.col("DIM07").cast("double"))
    .withColumn("DIM08_", F.col("DIM08").cast("double"))
    .withColumn("DIM18_", F.col("DIM18").cast("double")))
carto_1.createOrReplaceTempView("cartographie_reassurance_1")

carto_2 = spark.sql("""
    SELECT *, COUNTRY AS country, DIM01 AS SCHEME1, DIM02 AS Cover,
           DIM03 AS Original_underwritter,
           DIM06_ AS DIM06, DIM07_ AS DIM07, DIM08_ AS DIM08
    FROM cartographie_reassurance_1
""")
carto_2.createOrReplaceTempView("cartographie_reassurance_2")

carto_3 = (spark.table("cartographie_reassurance_2")
    .withColumn("taille", F.length("SCHEME1"))
    # SCHEME2 = 2 premiers car si len=3, sinon 3 premiers
    .withColumn("SCHEME2",
        F.when(F.length("SCHEME1") == 3, F.expr("substring(SCHEME1,1,2)"))
         .otherwise(F.expr("substring(SCHEME1,1,3)")))
    .withColumn("Product", F.col("SCHEME2"))
    .withColumn("Version", F.col("DIM18"))
    # SCHEME = compress(Product || "." || DIM18)
    .withColumn("SCHEME",
        F.regexp_replace(F.concat(F.col("Product"), F.lit("."), F.col("DIM18")), " ", ""))
    # CLE = compress(Product _ Cover _ COUNTRY _ Original_underwritter)
    .withColumn("CLE",
        F.regexp_replace(
            F.concat_ws("_", F.col("Product"), F.col("Cover"),
                        F.col("COUNTRY"), F.col("Original_underwritter")), " ", ""))
    .withColumn("QP_rei_PREMIUM", F.col("DIM06") / 100)
    .withColumn("QP_rei_COMM",    F.col("DIM07") / 100)
    .withColumn("QP_rei_CLAIM",   F.col("DIM08") / 100)
    .withColumn("gl_type_no", F.col("Original_underwritter").cast("double")))
carto_3.createOrReplaceTempView("cartographie_reassurance_3")

# ═══════════════════════════════════════════════════════════════════════
# 9. TABLES DE SORTIE
# ═══════════════════════════════════════════════════════════════════════
Parametres_Reas_new = spark.sql("""
    SELECT country, Product, Version, SCHEME, CLE, Cover,
           gl_type_no AS Original_underwritter,
           QP_rei_PREMIUM, QP_rei_COMM, QP_rei_CLAIM
    FROM cartographie_reassurance_3
""")
Parametres_Reas_new.createOrReplaceTempView("Parametres_Reas_new")

Entity_Mappingsv2_test = spark.sql("""
    SELECT country, TABLE_NAME, LANGUAGE, SCHEME1 AS SCHEME2, cover,
           DIM03 AS entity_cd, DIM04, DIM05, DIM06, DIM07, DIM08, DIM09, DIM10,
           DIM11, DIM12, DIM13, DIM14, DIM15, DIM16, DIM17, DIM18, DIM19, DIM20,
           TIMESTAMP, USERID, SCHEME AS SCHEME3
    FROM cartographie_reassurance_3
""")
Entity_Mappingsv2_test.createOrReplaceTempView("Entity_Mappingsv2_test")

Export_SAS_2 = spark.sql("""
    SELECT country, TABLE_NAME, LANGUAGE, SCHEME1 AS SCHEME2, Product, Version,
           SCHEME, cover, DIM03 AS entity_cd, DIM04, DIM05, DIM06, DIM07, DIM08,
           DIM09, DIM10, DIM11, DIM12, DIM13, DIM14, DIM15, DIM16, DIM17, DIM18,
           DIM19, DIM20, TIMESTAMP, USERID
    FROM cartographie_reassurance_3
""")
Export_SAS_2.createOrReplaceTempView("Export_SAS_2")

# ═══════════════════════════════════════════════════════════════════════
# 10. EXPORTS RÉASSURANCE
# ═══════════════════════════════════════════════════════════════════════
chemin_output_reass = (f"{BASE}/{arrete}/02_Elements_Techniques/TIA/Arrete reel"
                       f"/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties")

export_excel(carto,                  f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", "Sheet 1")
export_excel(Export_SAS_2,           f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", "Export_SAS_2")
export_excel(Entity_Mappingsv2_test, f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", "Export_SAS")
export_excel(Parametres_Reas_new,    f"{chemin_output_reass}/Reassurance.xlsx", "Parametres_Reas")

print("Paramétrage Ruling & Réassurance terminé.")
