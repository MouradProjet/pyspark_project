# -*- coding: utf-8 -*-
"""
SPECIFICATION — chargement des paramètres pays (Model Properties).

Chaque pays a un fichier Excel '{pays} Model Properties.xlsx' contenant
plusieurs feuilles (RESERVE_GROUP_SPEC, MNTHLY_BNFT_LIMITS, ...).
Chaque feuille est PERSISTÉE en table Delta {PARAMS_SCHEMA}.{pays}_{FEUILLE},
accessible depuis n'importe quelle session (pas besoin de recharger les Excel).
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
balancedate   = "26/06/2026"
arrete        = "2026_06_Prov"
PARAMS_SCHEMA = "params"          # schéma où persister les tables de paramètres

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PARAMS_SCHEMA}")

BASE = ("~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
        "/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM"
        "/CASES RESERVES/Model Properties")

# Feuilles à charger : communes à tous les pays + spécifiques par pays.
# Chaque entrée = (feuille, suffixe_table). La table s'appelle {pays}_{suffixe}.
FEUILLES_COMMUNES = [
    ("RESERVE_GROUP_SPEC",     "RESERVE_GROUP_SPEC"),
    ("MNTHLY_BNFT_LIMITS",     "MNTHLY_BNFT_LIMITS"),
    ("OTSTANDING_BLNC_LIMITS", "OTSTANDING_BLNC_LIMITS"),
    ("TRANS_TYPE_MAP",         "TRANS_TYPE_MAP"),
]
FEUILLES_PAR_PAYS = {
    "FR": [("SCHEME_DATABASE", "SCHEME_DATABASE"), ("BEN_POUC", "BEN_POUC")],
    "UK": [("FIXED_BNFT_LIMITS", "FIXED_BNFT_LIMITS")],
}

# Colonnes numériques (bornes) à caster en double après lecture, par feuille.
# inferSchema='false' lit tout en texte pour ne pas tronquer les décimaux ;
# on cast ensuite explicitement en double là où la valeur est numérique.
COLONNES_NUM = {
    "MNTHLY_BNFT_LIMITS":     ["LOWER", "UPPER"],
    "OTSTANDING_BLNC_LIMITS": ["LOWER", "UPPER"],
}


def import_sheet(fichier, feuille, table):
    """Lit une feuille Excel et la persiste en table Delta (décimaux préservés)."""
    df = (spark.read.format("com.crealytics.spark.excel")
          .option("dataAddress", f"'{feuille}'!A1")
          .option("header", "true")
          .option("inferSchema", "false")          # texte → pas de troncature
          .option("usePlainNumberFormat", "true")  # valeur brute, pas l'affichage
          .load(fichier))
    # Cast des colonnes numériques connues (virgule décimale gérée)
    for col in COLONNES_NUM.get(feuille, []):
        if col in df.columns:
            df = df.withColumn(col, F.regexp_replace(F.col(col), ",", ".").cast("double"))
    df.write.mode("overwrite").saveAsTable(f"{PARAMS_SCHEMA}.{table}")


def specification(pays):
    """Charge et persiste toutes les feuilles de paramètres pour un pays."""
    fichier = f"{BASE.format(arrete=arrete)}/{pays} Model Properties.xlsx"
    feuilles = FEUILLES_COMMUNES + FEUILLES_PAR_PAYS.get(pays, [])
    for feuille, suffixe in feuilles:
        # cas particulier UK : la table est {pays}FIXED_BNFT_LIMITS (sans '_')
        table = f"{pays}{suffixe}" if suffixe == "FIXED_BNFT_LIMITS" else f"{pays}_{suffixe}"
        import_sheet(fichier, feuille, table)


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION
# ═══════════════════════════════════════════════════════════════════════
pays_list = [
    "UK", "FI", "FR", "SE", "PT", "DE", "PL", "IT", "NO", "ES",
    "IE", "NI", "NL", "GR", "TR", "CH", "DK", "AT", "BE", "CO",
    "MX", "LT", "LV", "EE",
    # "LU",  # commenté dans le SAS
]

for pays in pays_list:
    print(f"Chargement des Model Properties pour {pays}...")
    try:
        specification(pays)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

print("Chargement terminé.")
