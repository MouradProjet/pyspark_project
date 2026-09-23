# -*- coding: utf-8 -*-
"""
ACR CLOT — met en forme les cases reserves (CR + IBNR) et les flux claims
au format BGD, par pays. Produit deux tables persistées par pays :
  - TIA.STOCK_CR_CLOT_TIA_{pays}       (stock des réserves)
  - TIA.HISTO_FLUX_PROVISIONS_{pays}   (flux réserves + claims payés)

Traduction PySpark complète du module SAS.
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete               = "2026_04_V2"
ouput                = "CR_Q125"          # schéma sortie (ex-LIBNAME CR_Q125)
month, day, yr       = "03", "27", "2026"
tx_claims_handling   = 0.03               # taux claims handling (demandé à chaque closing)

TIA_SCHEMA = "tia"
DATE_SFX   = f"{yr}{month}{day}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TIA_SCHEMA}")

BASE = "~/NAS/X/08.Progammes"
ENTITY_MAPPINGS_FILE = (f"{BASE}/Etablissements financiers/PROJETS TRANSVERSAUX"
                        f"/Genworth/TOM Reserving/Table de reserving/Donnees/FY16"
                        f"/Data EXCEL/R/PROCESS RESERVING CLP/ON-SYSTEM/CASE RESERVES"
                        f"/02 Model Properties/Entity_Mappings.xlsx")

# Codes entités → FACL / FICL
ENTITY_FACL = [102,122,132142,152,172,192,212,302,312,502,682,702,712,772,782,
               792,802,812,821,822,832,842,851,852,861,862,872,882,892,902,912,
               922,931,932,942,951,952,961,962,972,982,992]
ENTITY_FICL = [101,121,131,141,151,171,301,311,501,671,681,701,711,771,791,801,
               811,821,831,841,851,861,871,881,891,901,911,921,931,941,951,961,
               971,981,991]


def _sheet(fichier, feuille):
    return (spark.read.format("com.crealytics.spark.excel")
            .option("dataAddress", f"'{feuille}'!A1")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("usePlainNumberFormat", "true")
            .load(fichier))


def classer_entity(df):
    """Attribue FACL / FICL / UNKNOWN selon Entity_CD."""
    return df.withColumn("Entity",
        F.when(F.col("Entity_CD").isin(ENTITY_FACL), F.lit("FACL"))
         .when(F.col("Entity_CD").isin(ENTITY_FICL), F.lit("FICL"))
         .otherwise(F.lit("UNKNOWN")))


def quarter_from_month(acc_yr_col, acc_mnth_col):
    """cats(Acc_Yr, 'Qx') selon le mois."""
    return F.concat(acc_yr_col.cast("string"),
        F.when(acc_mnth_col.isin(1, 2, 3), F.lit("Q1"))
         .when(acc_mnth_col.isin(4, 5, 6), F.lit("Q2"))
         .when(acc_mnth_col.isin(7, 8, 9), F.lit("Q3"))
         .otherwise(F.lit("Q4")))


# ═══════════════════════════════════════════════════════════════════════
# IMPORT DES MAPPINGS (une seule fois, communs à tous les pays)
# ═══════════════════════════════════════════════════════════════════════
FX = _sheet(ENTITY_MAPPINGS_FILE, "FX")
FX.createOrReplaceTempView("FX")
# (les autres feuilles Entity_Mappings/RI_* /CI ne sont pas utilisées en aval)


def acr(pays):
    # ═══════════════════════════════════════════════════════════════════
    # 1. CASE RESERVES (CR) — filtre + agrégation
    # ═══════════════════════════════════════════════════════════════════
    cr = (spark.table(f"{ouput}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
        .filter(F.expr(f"""country = '{pays}' AND STATUS IN ('OP','RO')
                           AND Rsrv_Typ IN ('ICOP','RBNP')
                           AND Rsrv_Grp NOT IN ('ZZ1','ZZ2')
                           AND LEGACY_SCHEME_BOOK = 'TIA'"""))
        .withColumnRenamed("Vintage_year", "Cohort")
        .withColumnRenamed("Rsrv_Amt_Net", "Rsrv_Amt")
        .withColumn("Acc_yr", F.year("Incident_date"))
        .withColumn("Acc_Mnth", F.month("Incident_date"))
        .withColumn("Quarter", quarter_from_month(F.col("Acc_yr"), F.col("Acc_Mnth")))
        .withColumn("POSTE",
            F.when(F.col("Rsrv_Typ") == "ICOP", F.lit("ICOP_CLOT"))
             .when(F.col("Rsrv_Typ") == "RBNP", F.lit("RBNP_CLOT")))
        .withColumn("Entity_CD2", F.col("Entity_CD").cast("double")))
    cr = (cr.groupBy("country", "Rsrv_Grp", "Scheme", "Type_Insurance", "cover",
                     "Cohort", "Entity_CD2", "Entity", "Quarter", "POSTE")
            .agg(F.sum("Rsrv_Amt").alias("Rsrv_Amt"))
            .withColumnRenamed("Entity_CD2", "Entity_CD"))

    # ═══════════════════════════════════════════════════════════════════
    # 2. IBNR & NON-CORE
    # ═══════════════════════════════════════════════════════════════════
    ibnr = (spark.table(f"{ouput}.WPS_DAAP_IBNR_{DATE_SFX}")
        .filter(F.col("country") == pays)
        .withColumnRenamed("Vintage_year", "Cohort")
        .withColumnRenamed("Rsrv_Amt_Net", "Rsrv_Amt")
        .withColumnRenamed("Incident_Quarter", "Quarter")
        .withColumn("POSTE", F.lit("IBNR_CLOT"))
        .withColumn("Entity_CD2", F.col("Entity_CD").cast("double")))
    ibnr = (ibnr.groupBy("country", "Scheme", "Type_Insurance", "Rsrv_Grp", "cover",
                         "Cohort", "Entity_CD2", "Entity", "Quarter", "POSTE")
             .agg(F.sum("Rsrv_Amt").alias("Rsrv_Amt"))
             .withColumnRenamed("Entity_CD2", "Entity_CD"))

    # ═══════════════════════════════════════════════════════════════════
    # 3. RESERVES_TIA = CR + IBNR, jointure FX, calcul PAD / claims handling
    # ═══════════════════════════════════════════════════════════════════
    reserves = cr.unionByName(ibnr, allowMissingColumns=True)
    reserves = reserves.join(
        FX.select(F.col("country").alias("fx_country"), F.col("Currency").alias("Currency_code")),
        reserves.country == F.col("fx_country"), "left").drop("fx_country")

    reserves = (reserves
        .withColumn("Total_AXA", F.col("Rsrv_Amt"))
        .withColumn("Claims_Handling", F.col("Total_AXA") * tx_claims_handling)
        .withColumn("PAD", (F.col("Total_AXA") + F.col("Claims_Handling")) * 0.05)
        .withColumn("NET_AMT", F.col("Total_AXA") + F.col("Claims_Handling")))

    # Classification entité + jointure agent (CARTO_TIA)
    stock_acr = classer_entity(reserves)
    stock_acr = stock_acr.join(
        spark.table(f"{TIA_SCHEMA}.CARTO_TIA").select(
            F.col("Country").alias("c_country"), F.col("scheme").alias("c_scheme"),
            F.col("partner_sales_name").alias("Agent")),
        (stock_acr.Country == F.col("c_country")) & (stock_acr.scheme == F.col("c_scheme")),
        "left").drop("c_country", "c_scheme").dropDuplicates()

    # ═══════════════════════════════════════════════════════════════════
    # 4. CLAIMS PAID
    # ═══════════════════════════════════════════════════════════════════
    claim = (spark.table(f"{ouput}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
        .filter(F.col("country") == pays)
        .withColumnRenamed("Vintage_year", "Cohort")
        .withColumnRenamed("Totl_Amnt_Pd_Net", "Totl_Amnt_Pd")
        .withColumn("Acc_yr", F.year("Incident_date"))
        .withColumn("Acc_Mnth", F.month("Incident_date"))
        .withColumn("Quarter", quarter_from_month(F.col("Acc_yr"), F.col("Acc_Mnth")))
        .withColumn("Entity_CD2", F.col("Entity_CD").cast("double")))
    claim = (claim.groupBy("country", "COVER", "scheme", "Type_Insurance",
                           "Entity_CD2", "Entity", "Cohort", "Quarter")
              .agg(F.sum("Totl_Amnt_Pd").alias("Claim_Paid"))
              .withColumnRenamed("Entity_CD2", "Entity_CD"))
    # jointure FX + classification entité
    claim = claim.join(
        FX.select(F.col("country").alias("fx_country"), F.col("Currency").alias("Currency_code")),
        claim.country == F.col("fx_country"), "left").drop("fx_country")
    claim = classer_entity(claim)
    claim = claim.withColumn("Claim_Paid_Net", F.col("Claim_Paid"))
    # jointure agent
    claim_flux = claim.join(
        spark.table(f"{TIA_SCHEMA}.CARTO_TIA").select(
            F.col("Country").alias("c_country"), F.col("scheme").alias("c_scheme"),
            F.col("partner_sales_name").alias("Agent")),
        (claim.Country == F.col("c_country")) & (claim.scheme == F.col("c_scheme")),
        "left").drop("c_country", "c_scheme").dropDuplicates()

    # ═══════════════════════════════════════════════════════════════════
    # 5. STOCK CR au format BGD (hors TPA)
    # ═══════════════════════════════════════════════════════════════════
    stock_bgd = (stock_acr
        .filter(F.col("Entity") != "TPA")
        .withColumnRenamed("Agent", "RGPT")
        .withColumnRenamed("Cohort", "GEN")
        .withColumn("SURV", F.expr("cast(substring(Quarter,1,4) as int)")))
    stock_bgd = (stock_bgd.groupBy("country", "scheme", "cover", "Entity_CD", "Entity",
                                   "Type_Insurance", "GEN", "SURV", "Currency_code", "POSTE", "RGPT")
                 .agg(F.sum("NET_AMT").alias("NET_AMT")))
    stock_bgd.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.STOCK_CR_CLOT_TIA_{pays}")

    # ═══════════════════════════════════════════════════════════════════
    # 6. FLUX (claims + réserves) au format BGD
    # ═══════════════════════════════════════════════════════════════════
    # Flux claims (FICL/FACL uniquement)
    claim_f = (claim_flux
        .filter(F.col("Entity").isin("FICL", "FACL"))
        .withColumnRenamed("Agent", "RGPT")
        .withColumnRenamed("Claim_Paid_Net", "NET_AMT")
        .withColumnRenamed("Cohort", "GEN")
        .withColumn("GEN", F.coalesce(F.col("GEN"), F.lit(9999)))
        .withColumn("Poste", F.lit("CLAIM")))

    # Flux réserves
    stock_f = (stock_acr
        .withColumnRenamed("Agent", "RGPT")
        .withColumnRenamed("Cohort", "GEN")
        .withColumn("GEN", F.coalesce(F.col("GEN"), F.lit(9999)))
        .withColumn("Poste", F.lit("RESERVES")))

    # Consolidation flux (hors TPA)
    histo = (claim_f.unionByName(stock_f, allowMissingColumns=True)
        .filter(F.col("Entity") != "TPA"))
    histo = (histo.groupBy("country", "scheme", "COVER", "Entity_CD", "Entity",
                           "Type_Insurance", "GEN", "Quarter", "Currency_code", "Poste", "RGPT")
             .agg(F.sum("NET_AMT").alias("NET_AMT")))
    histo.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.HISTO_FLUX_PROVISIONS_{pays}")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION POUR TOUS LES PAYS
# ═══════════════════════════════════════════════════════════════════════
PAYS = ["DE","DK","IE","IT","FI","FR","GR","NI","NL","NO","PL","PT","SE",
        "TR","UK","CH","ES","AT","CO","MX","BE","LT"]

for pays in PAYS:
    print(f"ACR CLOT {pays}...")
    try:
        acr(pays)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

print("ACR CLOT terminé.")
