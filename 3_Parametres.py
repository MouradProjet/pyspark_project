# -*- coding: utf-8 -*-
"""
SPECIFICATION — chargement des paramètres pays (Model Properties).

Chaque pays a un fichier Excel '{pays} Model Properties.xlsx' contenant
plusieurs feuilles. Ce programme les persiste en tables Delta
params.{pays}_{FEUILLE}, accessibles depuis n'importe quelle session.

Version explicite : la fonction specification(pays) traite chaque pays via un
bloc lisible. Les 4 feuilles communes sont chargées pour tous les pays, puis
les feuilles spécifiques (FR, UK) sont ajoutées selon le pays.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
balancedate   = "26/06/2026"
arrete        = "2026_06_Prov"
PARAMS_SCHEMA = "params"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PARAMS_SCHEMA}")


def import_sheet(fichier, feuille, table):
    """Lit une feuille Excel et la persiste en table Delta (décimaux préservés).

    inferSchema=false lit tout en texte pour ne pas tronquer les petits décimaux
    (ex. 0.002) ; usePlainNumberFormat lit la valeur brute et non l'affichage.
    Les colonnes de bornes LOWER/UPPER sont ensuite castées en double.
    """
    df = (spark.read.format("com.crealytics.spark.excel")
          .option("dataAddress", f"'{feuille}'!A1")
          .option("header", "true")
          .option("inferSchema", "false")
          .option("usePlainNumberFormat", "true")
          .load(fichier))
    # Cast des bornes numériques si présentes (virgule décimale gérée)
    for col in ("LOWER", "UPPER"):
        if col in df.columns:
            df = df.withColumn(col, F.regexp_replace(F.col(col), ",", ".").cast("double"))
    df.write.mode("overwrite").saveAsTable(f"{PARAMS_SCHEMA}.{table}")


def specification(pays):
    """Charge et persiste les feuilles de paramètres pour un pays."""

    fichier = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
               f"/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM"
               f"/CASES RESERVES/Model Properties/{pays} Model Properties.xlsx")

    # ── Feuilles communes à TOUS les pays ──────────────────────────────
    import_sheet(fichier, "RESERVE_GROUP_SPEC",     f"{pays}_RESERVE_GROUP_SPEC")
    import_sheet(fichier, "MNTHLY_BNFT_LIMITS",     f"{pays}_MNTHLY_BNFT_LIMITS")
    import_sheet(fichier, "OTSTANDING_BLNC_LIMITS", f"{pays}_OTSTANDING_BLNC_LIMITS")
    import_sheet(fichier, "TRANS_TYPE_MAP",         f"{pays}_TRANS_TYPE_MAP")

    # ── Feuilles supplémentaires selon le pays ─────────────────────────
    if pays == "FR":
        import_sheet(fichier, "SCHEME_DATABASE", f"{pays}_SCHEME_DATABASE")
        import_sheet(fichier, "BEN_POUC",        f"{pays}_BEN_POUC")

    elif pays == "UK":
        import_sheet(fichier, "FIXED_BNFT_LIMITS", f"{pays}FIXED_BNFT_LIMITS")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION — un appel par pays
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
