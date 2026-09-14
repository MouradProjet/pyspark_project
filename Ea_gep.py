# -*- coding: utf-8 -*-
"""
EA_GEP — agrège les GEP par pays (BLK/MF/UPF/MRTRANS), applique le facteur 1.5
sur BLK/MRTRANS du trimestre courant, retire le trimestre T+1, consolide tous
les pays, puis découpe les cases reserves / IBNR / NCC par pays.
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete  = "2025_04_V2"
N       = 2025
ouput   = "CR_Q125"          # schéma des tables WPS_DAAP
month   = "03"
day     = "28"
yr      = "2025"
Q_CUR   = "Q12025"           # trimestre courant (facteur 1.5)
Q_NEXT  = "Q22025"           # trimestre T+1 à retirer

EA_SCHEMA  = "ea"
GEP_SCHEMA = "gep"
CR_SCHEMA  = "cr"

DATE_SFX = f"{yr}{month}{day}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GEP_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CR_SCHEMA}")

DROP_COLS = ["LEGACY_SCHEME_CODE", "LEGACY_AGREEMENT_NUMBER", "LEGACY_RENEWAL_NUMBER"]

# ═══════════════════════════════════════════════════════════════════════
# GEP CONCATENATION par pays
# ═══════════════════════════════════════════════════════════════════════
GROUPE_STD = {"DENMARK", "GERMANY", "GREECE", "IRELAND", "ITALY",
              "PORTUGAL", "SPAIN", "TURKEY", "UK", "SWITZERLAND"}
GROUPE_SANS_MF = {"NORWAY", "POLAND", "SWEDEN", "FINLAND", "COLOMBIA",
                  "AUSTRIA", "MEXICO", "BELGIUM"}


def gep_agr(pays, cc):
    if pays in GROUPE_STD:
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK", f"{EA_SCHEMA}.{pays}_EA_MF",
                   f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS"]
    elif pays in GROUPE_SANS_MF:
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK",
                   f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS"]
    elif pays == "FRANCE":
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK", f"{EA_SCHEMA}.{pays}_EA_MF",
                   f"{EA_SCHEMA}.{pays}_EA_MRTRANS"]
    elif pays == "NETHERLANDS":
        sources = [f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS"]
    elif pays == "NORTHERNIRELAND":
        sources = [f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_BLK"]
    else:
        print(f"  ⚠ {pays} : composition GEP inconnue")
        return

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                [spark.table(t) for t in sources])
    df = df.drop(*DROP_COLS)

    # Facteur 1.5 sur GEP pour BLK/MRTRANS du trimestre courant
    df = df.withColumn("GEP",
        F.when(F.col("payfreq").isin("BLK", "MRTRANS") & (F.col("Qtr_Period") == Q_CUR),
               1.5 * F.col("GEP"))
         .otherwise(F.col("GEP")))
    # Retirer le trimestre T+1
    df = df.filter(~F.col("Qtr_Period").isin(Q_NEXT))

    df.write.mode("overwrite").saveAsTable(f"{GEP_SCHEMA}.{pays}_GEP_ALL")


gep_pays = [
    ("AUSTRIA","AT"),("BELGIUM","BE"),("COLOMBIA","CO"),("DENMARK","DK"),
    ("FINLAND","FI"),("FRANCE","FR"),("GERMANY","DE"),("GREECE","GR"),
    ("IRELAND","IE"),("ITALY","IT"),("MEXICO","MX"),("NETHERLANDS","NL"),
    ("NORTHERNIRELAND","NI"),("NORWAY","NO"),("POLAND","PL"),("PORTUGAL","PT"),
    ("SPAIN","ES"),("SWEDEN","SE"),("SWITZERLAND","CH"),("TURKEY","TR"),("UK","UK"),
]
for pays, cc in gep_pays:
    print(f"GEP {pays}...")
    try:
        gep_agr(pays, cc)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

# ═══════════════════════════════════════════════════════════════════════
# CONSOLIDATION GEP tous pays (MEXICO absent du SET SAS → exclu)
# ═══════════════════════════════════════════════════════════════════════
pays_conso = [
    "AUSTRIA","BELGIUM","COLOMBIA","DENMARK","FINLAND","FRANCE","GERMANY",
    "GREECE","IRELAND","ITALY","NETHERLANDS","NORTHERNIRELAND","NORWAY",
    "POLAND","PORTUGAL","SPAIN","SWEDEN","TURKEY","UK",
]
def _exists(t):
    try:
        spark.table(t); return True
    except Exception:
        return False

tables_gep = [f"{GEP_SCHEMA}.{p}_GEP_ALL" for p in pays_conso]
tables_gep = [t for t in tables_gep if _exists(t)]
gep_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                 [spark.table(t) for t in tables_gep])
gep_all.write.mode("overwrite").saveAsTable(f"{GEP_SCHEMA}.WPS_DAAP_GEP_{DATE_SFX}")


# ═══════════════════════════════════════════════════════════════════════
# CLAIMS RESERVES — découpe par pays
# ═══════════════════════════════════════════════════════════════════════
def split_cr(pays, schema):
    clmhdr = (spark.table(f"{schema}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
              .filter(F.col("country") == pays))
    clmhdr.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_CLMHDR_ALL")

    ibnr = (spark.table(f"{schema}.WPS_DAAP_IBNR_{DATE_SFX}")
            .filter((F.col("country") == pays) & (~F.col("Rsrv_Grp").isin("ZZ1"))))
    ibnr.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_IBNR_ALL")

    # NCC : le SAS lit WPS_DAAP_IBNR (pas NCC) filtré sur ZZ1 — reproduit tel quel
    ncc = (spark.table(f"{schema}.WPS_DAAP_IBNR_{DATE_SFX}")
           .filter((F.col("country") == pays) & (F.col("Rsrv_Grp") == "ZZ1")))
    ncc.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_NCC_ALL")


pays_cr = ["DE","DK","ES","IE","IT","FI","FR","GR","NL","NI","NO","PL",
           "PT","SE","TR","UK","AT","BE","MX"]
for pays in pays_cr:
    print(f"Split CR {pays}...")
    try:
        split_cr(pays, ouput)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

# Colombie : schéma dédié
CR_COLO_SCHEMA = "cr_colo"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CR_COLO_SCHEMA}")


def split_cr_colo(pays, schema):
    clmhdr = (spark.table(f"{schema}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
              .filter(F.col("country") == pays))
    clmhdr.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_CLMHDR_ALL")

    ibnr = (spark.table(f"{schema}.WPS_DAAP_IBNR_{DATE_SFX}")
            .filter((F.col("country") == pays) & (~F.col("Rsrv_Grp").isin("ZZ1"))))
    ibnr.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_IBNR_ALL")

    # Pour CO, le NCC lit bien WPS_DAAP_NCC (pas IBNR)
    ncc = (spark.table(f"{schema}.WPS_DAAP_NCC_{DATE_SFX}")
           .filter((F.col("country") == pays) & (F.col("Rsrv_Grp") == "ZZ1")))
    ncc.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.{pays}_NCC_ALL")


try:
    split_cr_colo("CO", CR_COLO_SCHEMA)
except Exception as e:
    print(f"  ⚠ CO : {e}")


# ═══════════════════════════════════════════════════════════════════════
# CONSOLIDATION GLOBALE (hors CO, + CO, hors CH)
# ═══════════════════════════════════════════════════════════════════════
# CASE RESERVES
case_res = (spark.table(f"{ouput}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
            .filter(F.col("country") != "CO")
            .unionByName(spark.table(f"{CR_SCHEMA}.CO_CLMHDR_ALL"), allowMissingColumns=True)
            .filter(F.col("country") != "CH"))
case_res.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")

# IBNR
ibnr_all = (spark.table(f"{ouput}.WPS_DAAP_IBNR_{DATE_SFX}")
            .filter(F.col("country") != "CO")
            .unionByName(spark.table(f"{CR_SCHEMA}.CO_IBNR_ALL"), allowMissingColumns=True)
            .filter(~F.col("country").isin("CH")))
ibnr_all.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.WPS_DAAP_IBNR_{DATE_SFX}")

# NCC : IBNR consolidé + CO_NCC, hors CH, ZZ1
# (le SAS lit WPS_DAAP_IBNR local, pas NCC — reproduit tel quel)
ncc_all = (spark.table(f"{CR_SCHEMA}.WPS_DAAP_IBNR_{DATE_SFX}")
           .unionByName(spark.table(f"{CR_SCHEMA}.CO_NCC_ALL"), allowMissingColumns=True)
           .filter((~F.col("country").isin("CH")) & (F.col("Rsrv_Grp") == "ZZ1")))
ncc_all.write.mode("overwrite").saveAsTable(f"{CR_SCHEMA}.WPS_DAAP_NCC_{DATE_SFX}")

print("EA_GEP terminé.")
