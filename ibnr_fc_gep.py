# -*- coding: utf-8 -*-
"""
IBNR FC SUR LES GEP STOCK — NET
Calcule les PSAP forecast (IBNR) sur les GEP stock, net de réassurance,
par pays, au format BGD. Consolide tous les pays dans
TIA.reserves_bgd_fc_all_net.

Logique par pays :
  - filtre les policy rules de l'année N
  - applique la cession (QP réassurance) → UEP net
  - calcule le UEP forecast (reste jusqu'à décembre) = UEP arrêté - UEP calcul
  - PSAP_FC = UEP_forecast × Loss Ratio (LR)
  - met au format BGD (entity FICL/FACL, RGPT, currency...)
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete          = "2026_04_V2"
N               = 2026
DT_CAL          = "2026-12-31"     # date de calcul (fin d'année)
DT_ARRETE_REEL  = "2026-03-27"     # date d'arrêté réel

OUT_GEP_SCHEMA = "out_gep"
TIA_SCHEMA     = "tia"
LR_SCHEMA      = "lr"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TIA_SCHEMA}")

BASE = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP"

# Mois/année des deux dates (pour les filtres)
MONTH_ARRETE, YEAR_ARRETE = 3, 2026
MONTH_CAL, YEAR_CAL       = 12, 2026

# Codes entités → FICL / FACL
ENTITY_FICL = [101,121,131,141,151,171,301,311,501,671,681,701,711,771,791,801,
               811,821,831,841,851,861,871,881,891,901,911,921,931,941,951,961,
               971,981,991]
ENTITY_FACL = [102,122,132,142,152,172,192,212,302,312,502,682,702,712,772,782,
               792,802,812,821,822,832,842,851,852,861,862,872,882,892,902,912,
               922,931,932,942,951,952,961,962,972,982,992]


def _sheet(fichier, feuille):
    return (spark.read.format("com.crealytics.spark.excel")
            .option("dataAddress", f"'{feuille}'!A1")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("usePlainNumberFormat", "true")
            .load(fichier))


# ═══════════════════════════════════════════════════════════════════════
# IMPORTS & MAPPINGS (une seule fois)
# ═══════════════════════════════════════════════════════════════════════
# Réassurance
reas = _sheet(f"{BASE}/{arrete}/02_Elements_techniques/TIA/Arrete reel/RESERVES"
              f"/ON-SYSTEM/CASES RESERVES/Model Properties/Reassurance.xlsx",
              "Parametres_Reas")
reas.createOrReplaceTempView("reas")

# Mapping cover
mapping_cover = _sheet(f"{BASE}/01_Mappings/Mapping cover TIA.xlsx", "Feuil1")
mapping_cover_distinct = mapping_cover.select("TIA_Cover", "cover").dropDuplicates()
mapping_cover_distinct.createOrReplaceTempView("mapping_cover_distinct")

# Mapping currency
mapping_currency = _sheet(f"{BASE}/01_Mappings/Mapping_currency.xlsx", "Mapping")
mapping_currency.createOrReplaceTempView("mapping_currency")

# Mapping RGPT (depuis carto_tia)
mapping_rgpt = spark.sql(f"""
    SELECT DISTINCT Country, Scheme, partner_sales_name AS RGPT
    FROM {TIA_SCHEMA}.carto_tia
""")
mapping_rgpt.createOrReplaceTempView("mapping_rgpt")

# Mapping typaff / typidean (depuis carto_tia)
mapping_typaff = spark.sql(f"""
    SELECT DISTINCT Country, Scheme, AXA_QS_Involvement,
        CASE WHEN AXA_QS_Involvement = 'DIRECT UNDERWRITER' THEN 'DI'
             WHEN AXA_QS_Involvement = 'UNKNOWN' THEN 'DI'
             WHEN AXA_QS_Involvement = 'COINSURER' THEN 'CO'
             WHEN AXA_QS_Involvement = 'TPA ONLY' THEN 'DI'
             WHEN AXA_QS_Involvement = 'REINSURER' THEN 'RE' END AS typidean,
        CASE WHEN AXA_QS_Involvement = 'DIRECT UNDERWRITER' THEN 0
             WHEN AXA_QS_Involvement = 'UNKNOWN' THEN 0
             WHEN AXA_QS_Involvement = 'COINSURER' THEN 2
             WHEN AXA_QS_Involvement = 'TPA ONLY' THEN 0
             WHEN AXA_QS_Involvement = 'REINSURER' THEN 4 END AS typaff
    FROM {TIA_SCHEMA}.carto_tia
""")
mapping_typaff.createOrReplaceTempView("mapping_typaff")


# ═══════════════════════════════════════════════════════════════════════
# MACRO CALCUL : IBNR forecast net par pays
# ═══════════════════════════════════════════════════════════════════════
def calcul(country, cc):
    # ── 1. Policy rules de l'année N ───────────────────────────────────
    policy = (spark.table(f"{OUT_GEP_SCHEMA}.policy_rule_{country}_bgd")
              .filter(F.year("EXP_PERIOD") == N))

    # ── 2. Cession réassurance (QP) → UEP net ──────────────────────────
    # scheme = compress(PRODUCT.PRODUCT_VERSION)
    policy = policy.withColumn("_scheme_join",
        F.regexp_replace(F.concat(F.col("PRODUCT"), F.lit("."), F.col("PRODUCT_VERSION")), " ", ""))
    policy_net = (policy.alias("t1").join(
        reas.select(
            F.col("country").alias("r_country"), F.col("scheme").alias("r_scheme"),
            F.col("cover").alias("r_cover"),
            F.col("Original_underwritter").alias("r_uw"),
            F.col("QP_rei_PREMIUM").alias("QP")).alias("t2"),
        (F.col("t1.Country") == F.col("t2.r_country")) &
        (F.col("t1._scheme_join") == F.col("t2.r_scheme")) &
        (F.col("t1.cover") == F.col("t2.r_cover")) &
        (F.col("t1.GL_TYPE_NO") == F.col("t2.r_uw")),
        "left")
        .drop("r_country", "r_scheme", "r_cover", "r_uw"))
    # UEP net : si QP existe → UEP × QP, sinon UEP brut
    policy_net = policy_net.withColumn("UEP_TBT_net",
        F.when(F.col("QP").isNotNull(), F.col("UEP_TBT") * F.col("QP"))
         .otherwise(F.col("UEP_TBT")))

    # ── 3. UEP forecast = UEP arrêté - UEP calcul ──────────────────────
    fc = (policy_net.groupBy("country", "cover", "product", "product_version",
                             "policy_line_no", "gl_type_no")
        .agg(
            F.sum(F.when((F.month("EXP_PERIOD") == MONTH_ARRETE) & (F.year("EXP_PERIOD") == YEAR_ARRETE),
                         F.col("UEP_TBT_net")).otherwise(F.lit(0))).alias("UEP_DT_arrete_reel"),
            F.sum(F.when((F.month("EXP_PERIOD") == MONTH_CAL) & (F.year("EXP_PERIOD") == YEAR_CAL),
                         F.col("UEP_TBT_net")).otherwise(F.lit(0))).alias("UEP_DT_cal")))
    fc = fc.withColumn("UEP_for_forecast",
        F.col("UEP_DT_arrete_reel") - F.col("UEP_DT_cal"))
    fc = fc.withColumn("_scheme_join",
        F.regexp_replace(F.concat(F.col("PRODUCT"), F.lit("."), F.col("PRODUCT_VERSION")), " ", ""))

    # ── 4. Jointures : cover, RGPT, LR, currency, typaff ───────────────
    fc = (fc.alias("t1")
        .join(mapping_cover_distinct.alias("t2"),
              F.col("t1.cover") == F.col("t2.TIA_Cover"), "left")
        .join(mapping_rgpt.alias("t3"),
              (F.col("t1.country") == F.col("t3.country")) &
              (F.col("t1._scheme_join") == F.col("t3.Scheme")), "left")
        .join(spark.table(f"{LR_SCHEMA}.indic_reel_all_net").alias("t4"),
              (F.col("t1.country") == F.col("t4.Country")) &
              (F.col("t2.cover") == F.col("t4.cover_name")) &
              (F.col("t3.RGPT") == F.col("t4.RGPT")), "left")
        .join(mapping_currency.alias("t5"),
              F.col("t1.country") == F.col("t5.country"), "left")
        .join(mapping_typaff.alias("t6"),
              (F.col("t1.country") == F.col("t6.country")) &
              (F.col("t1._scheme_join") == F.col("t6.Scheme")), "left")
        .select(F.col("t1.*"),
                F.col("t2.cover").alias("Cover2"),
                F.col("t3.RGPT"),
                F.col("t4.LR"),
                F.col("t5.currency"),
                F.col("t6.typaff")))

    # ── 5. PSAP forecast = UEP_forecast × LR ───────────────────────────
    fc = fc.withColumn("PSAP_FC", F.col("UEP_for_forecast") * F.col("LR"))

    # ── 6. Format BGD ──────────────────────────────────────────────────
    fc = (fc
        .withColumn("Scheme",
            F.regexp_replace(F.concat(F.col("PRODUCT"), F.lit("."), F.col("PRODUCT_VERSION")), " ", ""))
        .withColumn("entity",
            F.when(F.col("gl_type_no").isin(ENTITY_FICL), F.lit("FICL"))
             .when(F.col("gl_type_no").isin(ENTITY_FACL), F.lit("FACL")))
        .withColumn("GEN", F.lit(N))
        .withColumn("SURV", F.lit(N))
        .withColumn("POSTE", F.lit("RESERVES_CLOT")))

    bgd = (fc.groupBy("country", "Scheme",
                      F.col("Cover2").alias("cover_name"), "entity",
                      F.col("typaff").alias("Type_Insurance"),
                      F.col("currency").alias("currency_code"), "SURV", "RGPT", "POSTE")
        .agg(
            F.sum("UEP_for_forecast").alias("UEP_FC"),
            F.mean("LR").alias("LR"),
            F.sum("PSAP_FC").alias("NET_AMT"))
        .withColumn("GEN", F.lit(N)))
    bgd.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.reserves_bgd_fc_{cc}_2_net")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION — pays simples
# ═══════════════════════════════════════════════════════════════════════
PAYS_SIMPLES = [
    ("NETHERLANDS","NL"),("POLAND","PL"),("NORTHERNIRELAND","NI"),("NORWAY","NO"),
    ("FINLAND","FI"),("ESTONIA","EE"),("COLOMBIA","CO"),("PERU","PE"),
    ("LATVIA","LV"),("LITHUANIA","LT"),("MEXICO","MX"),("GREECE","GR"),
    ("GERMANY","DE"),("SPAIN","ES"),("DENMARK","DK"),("TURKEY","TR"),
    ("SWEDEN","SE"),("UK","UK"),("SWITZERLAND","CH"),("IRELAND","IE"),
    ("AUSTRIA","AT"),("BELGIUM","BE"),
]
for country, cc in PAYS_SIMPLES:
    print(f"Calcul {country}...")
    try:
        calcul(country, cc)
    except Exception as e:
        print(f"  ⚠ {country} : {e}")


def _exists(t):
    try:
        spark.table(t); return True
    except Exception:
        return False


def merge_sous_pays(sous_cc, cc_final):
    """Fusionne les sous-tables d'un pays (PT, IT) en une table pays."""
    tables = [f"{TIA_SCHEMA}.reserves_bgd_fc_{s}_2_net" for s in sous_cc]
    tables = [t for t in tables if _exists(t)]
    if not tables:
        return
    merged = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                    [spark.table(t) for t in tables])
    merged.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.reserves_bgd_fc_{cc_final}_2_net")


# ── PORTUGAL (7 sous-entités) ──────────────────────────────────────────
PT_SOUS = [("PORTUGAL1","PT1"),("PORTUGAL2","PT2"),("PORTUGAL31","PT31"),
           ("PORTUGAL32","PT32"),("PORTUGAL4","PT4"),("PORTUGAL51","PT51"),
           ("PORTUGAL52","PT52")]
for country, cc in PT_SOUS:
    try:
        calcul(country, cc)
    except Exception as e:
        print(f"  ⚠ {country} : {e}")
merge_sous_pays([cc for _, cc in PT_SOUS], "PT")

# ── ITALIE (18 sous-entités) ───────────────────────────────────────────
IT_SOUS = [("ITALY101","IT101"),("ITALY102","IT102"),("ITALY11","IT11"),
           ("ITALY12","IT12"),("ITALY31","IT31"),("ITALY32","IT32"),
           ("ITALY41","IT41"),("ITALY42","IT42"),("ITALY51","IT51"),
           ("ITALY52","IT52"),("ITALY61","IT61"),("ITALY62","IT62"),
           ("ITALY71","IT71"),("ITALY72","IT72"),("ITALY81","IT81"),
           ("ITALY82","IT82"),("ITALY91","IT91"),("ITALY92","IT92")]
for country, cc in IT_SOUS:
    try:
        calcul(country, cc)
    except Exception as e:
        print(f"  ⚠ {country} : {e}")
merge_sous_pays([cc for _, cc in IT_SOUS], "IT")

# ═══════════════════════════════════════════════════════════════════════
# CONSOLIDATION FINALE — tous les pays
# ═══════════════════════════════════════════════════════════════════════
tous_cc = [cc for _, cc in PAYS_SIMPLES] + ["PT", "IT"]
tables_all = [f"{TIA_SCHEMA}.reserves_bgd_fc_{cc}_2_net" for cc in tous_cc]
tables_all = [t for t in tables_all if _exists(t)]
reserves_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                      [spark.table(t) for t in tables_all])
reserves_all.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.reserves_bgd_fc_all_net")

print("IBNR FC net terminé.")
