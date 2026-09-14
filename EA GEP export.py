# -*- coding: utf-8 -*-
"""
EA GEP EXPORT — agrège les GEP par pays (BLK/MF/UPF/MRTRANS), filtre le
trimestre T+1, exporte en CSV ; puis découpe les cases reserves / IBNR / NCC
par pays et exporte.
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete  = "2026_06_Prov"
ouput   = "CR_Q226"          # schéma des tables WPS_DAAP (ex-LIBNAME)
month   = "06"               # STRING pour construire les noms de tables
day     = "26"
yr      = "2026"
hms     = "000000"
dossier = "202606"
q_n     = "Q22026"
q_n1    = "Q32026"           # trimestre T+1 à retirer

EA_SCHEMA = "ea"

# suffixe date des tables WPS_DAAP
DATE_SFX = f"{yr}{month}{day}"


def export_csv(df, chemin):
    """Exporte un DataFrame en CSV (un seul fichier)."""
    (df.coalesce(1).write
       .option("header", "true")
       .mode("overwrite")
       .csv(chemin))


# ═══════════════════════════════════════════════════════════════════════
# GEP CONCATENATION — agrégation par pays
# ═══════════════════════════════════════════════════════════════════════
# Composition des sources selon le pays :
#   groupe standard : BLK + MF + UPF + MRTRANS
#   groupe sans MF  : BLK + UPF + MRTRANS
#   FRANCE          : BLK + MF + MRTRANS (pas d'UPF)
#   NETHERLANDS     : UPF + MRTRANS
#   NORTHERNIRELAND : UPF + BLK
GROUPE_STD = {"DENMARK", "GERMANY", "GREECE", "IRELAND", "ITALY",
              "PORTUGAL", "SPAIN", "TURKEY", "UK", "SWITZERLAND"}
GROUPE_SANS_MF = {"NORWAY", "POLAND", "SWEDEN", "FINLAND", "COLOMBIA",
                  "AUSTRIA", "MEXICO", "BELGIUM"}

DROP_COLS = ["LEGACY_SCHEME_CODE", "LEGACY_AGREEMENT_NUMBER", "LEGACY_RENEWAL_NUMBER"]


def gep_agr(pays, cc):
    # Choisir les tables sources selon le pays
    if pays in GROUPE_STD:
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK_2", f"{EA_SCHEMA}.{pays}_EA_MF",
                   f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS_2"]
    elif pays in GROUPE_SANS_MF:
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK_2",
                   f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS_2"]
    elif pays == "FRANCE":
        sources = [f"{EA_SCHEMA}.{pays}_EA_BLK_2", f"{EA_SCHEMA}.{pays}_EA_MF",
                   f"{EA_SCHEMA}.{pays}_EA_MRTRANS_2"]
    elif pays == "NETHERLANDS":
        sources = [f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_MRTRANS_2"]
    elif pays == "NORTHERNIRELAND":
        sources = [f"{EA_SCHEMA}.{pays}_EA_UPF", f"{EA_SCHEMA}.{pays}_EA_BLK_2"]
    else:
        print(f"  ⚠ {pays} : composition GEP inconnue")
        return

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                [spark.table(t) for t in sources])
    df = df.drop(*DROP_COLS)
    # Retirer le trimestre T+1 (Q32026)
    df = df.filter(~F.col("Qtr_Period").isin(q_n1))
    df.createOrReplaceTempView(f"{pays}_GEP_ALL")

    # Export CSV
    repexp = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
              f"/02_Elements_Techniques/TIA/Arrete reel/PM Export/{dossier}"
              f"/{cc}_gep_davc_{DATE_SFX}_{hms}.csv")
    export_csv(df, repexp)


gep_pays = [
    ("AUSTRIA","at"),("BELGIUM","be"),("COLOMBIA","co"),("DENMARK","dk"),
    ("FINLAND","fi"),("FRANCE","fr"),("GERMANY","de"),("GREECE","gr"),
    ("IRELAND","ie"),("ITALY","it"),("MEXICO","mx"),("NETHERLANDS","nl"),
    ("NORTHERNIRELAND","ni"),("NORWAY","no"),("POLAND","pl"),("PORTUGAL","pt"),
    ("SPAIN","es"),("SWEDEN","se"),("SWITZERLAND","ch"),("TURKEY","tr"),("UK","uk"),
]
for pays, cc in gep_pays:
    print(f"GEP {pays}...")
    try:
        gep_agr(pays, cc)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")


# ═══════════════════════════════════════════════════════════════════════
# CLAIMS RESERVES FILE — découpe par pays
# ═══════════════════════════════════════════════════════════════════════
def split_cr(pays, schema):
    """Découpe cases reserves / IBNR / NCC pour un pays et exporte."""
    base_path = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
                 f"/02_Elements_Techniques/TIA/Arrete reel/PM Export/{dossier}")

    # CLMHDR : cases reserves du pays
    clmhdr = (spark.table(f"{schema}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
              .filter(F.col("country") == pays))
    clmhdr.createOrReplaceTempView(f"{pays}_CLMHDR_ALL")
    export_csv(clmhdr, f"{base_path}/{pays}_CLMHDR_ALL_{DATE_SFX}.csv")

    # IBNR : hors ZZ1
    ibnr = (spark.table(f"{schema}.WPS_DAAP_IBNR_{DATE_SFX}")
            .filter((F.col("country") == pays) & (~F.col("Rsrv_Grp").isin("ZZ1"))))
    ibnr.createOrReplaceTempView(f"{pays}_IBNR_ALL")
    export_csv(ibnr, f"{base_path}/{pays}_IBNR_ALL_{DATE_SFX}.csv")

    # NCC : uniquement ZZ1  (BUG CORRIGÉ : la condition Rsrv_Grp='ZZ1'
    # était collée dans la valeur du filtre par le robot)
    ncc = (spark.table(f"{schema}.WPS_DAAP_NCC_{DATE_SFX}")
           .filter((F.col("country") == pays) & (F.col("Rsrv_Grp") == "ZZ1")))
    ncc.createOrReplaceTempView(f"{pays}_NCC_ALL")
    export_csv(ncc, f"{base_path}/{pays}_NCC_ALL_{DATE_SFX}.csv")


# Tous les pays sauf CO, avec le schéma standard
pays_cr = ["DE","DK","ES","IE","IT","FI","FR","GR","NL","NI","NO","PL",
           "PT","SE","TR","UK","AT","BE","MX"]
for pays in pays_cr:
    print(f"Split CR {pays}...")
    try:
        split_cr(pays, ouput)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

# Colombie : schéma dédié cr_colo
CR_COLO_SCHEMA = "cr_colo"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CR_COLO_SCHEMA}")
try:
    split_cr("CO", CR_COLO_SCHEMA)
except Exception as e:
    print(f"  ⚠ CO : {e}")


# ═══════════════════════════════════════════════════════════════════════
# CONSOLIDATION GLOBALE (hors CO puis + CO, hors CH)
# ═══════════════════════════════════════════════════════════════════════
base_path = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
             f"/02_Elements_Techniques/TIA/Arrete reel/PM Export/{dossier}")

# CASE RESERVES : hors CO, + CO du schéma dédié, hors CH
case_res = (spark.table(f"{ouput}.WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
            .filter(F.col("country") != "CO")
            .unionByName(spark.table("CO_CLMHDR_ALL"), allowMissingColumns=True)
            .filter(F.col("country") != "CH"))
case_res.createOrReplaceTempView(f"WPS_DAAP_CASE_RESERVES_{DATE_SFX}")
export_csv(case_res, f"{base_path}/WPS_DAAP_CASE_RESERVES_{DATE_SFX}.csv")

# IBNR : hors CO, + CO, hors CH
ibnr_all = (spark.table(f"{ouput}.WPS_DAAP_IBNR_{DATE_SFX}")
            .filter(F.col("country") != "CO")
            .unionByName(spark.table("CO_IBNR_ALL"), allowMissingColumns=True)
            .filter(~F.col("country").isin("CH")))
ibnr_all.createOrReplaceTempView(f"WPS_DAAP_IBNR_{DATE_SFX}")
export_csv(ibnr_all, f"{base_path}/WPS_DAAP_IBNR_{DATE_SFX}.csv")

# NCC : IBNR consolidé + CO_NCC, hors CH, uniquement ZZ1
ncc_all = (spark.table(f"WPS_DAAP_IBNR_{DATE_SFX}")
           .unionByName(spark.table("CO_NCC_ALL"), allowMissingColumns=True)
           .filter((~F.col("country").isin("CH")) & (F.col("Rsrv_Grp") == "ZZ1")))
ncc_all.createOrReplaceTempView(f"WPS_DAAP_NCC_{DATE_SFX}")
export_csv(ncc_all, f"{base_path}/WPS_DAAP_NCC_{DATE_SFX}.csv")

print("Export GEP / CR terminé.")
