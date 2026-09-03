# -*- coding: utf-8 -*-
"""
SPECIFICATION — chargement des paramètres pays (Model Properties).

Chaque pays a un fichier Excel '{pays} Model Properties.xlsx' contenant
plusieurs feuilles (RESERVE_GROUP_SPEC, MNTHLY_BNFT_LIMITS, ...).
Chaque feuille est chargée dans une temp view {pays}_{FEUILLE}.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

balancedate = "26/06/2026"   # dernier vendredi du trimestre
arrete      = "2026_06_Prov"


def import_sheet(fichier, feuille, vue):
    """Lit une feuille précise d'un fichier Excel dans une temp view."""
    df = (spark.read.format("com.crealytics.spark.excel")
          .option("dataAddress", f"'{feuille}'!A1")
          .option("header", "true")
          .load(fichier))
    df.createOrReplaceTempView(vue)


def specification(pays):
    """Charge toutes les feuilles de paramètres pour un pays."""
    fichier = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
               f"/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM"
               f"/CASES RESERVES/Model Properties/{pays} Model Properties.xlsx")

    # ── Feuilles communes à tous les pays ──────────────────────────────
    import_sheet(fichier, "RESERVE_GROUP_SPEC",     f"{pays}_RESERVE_GROUP_SPEC")
    import_sheet(fichier, "MNTHLY_BNFT_LIMITS",     f"{pays}_MNTHLY_BNFT_LIMITS")
    import_sheet(fichier, "OTSTANDING_BLNC_LIMITS", f"{pays}_OTSTANDING_BLNC_LIMITS")
    import_sheet(fichier, "TRANS_TYPE_MAP",         f"{pays}_TRANS_TYPE_MAP")

    # ── Feuilles supplémentaires pour la FRANCE ────────────────────────
    if pays == "FR":
        import_sheet(fichier, "SCHEME_DATABASE", f"{pays}_SCHEME_DATABASE")
        import_sheet(fichier, "BEN_POUC",        f"{pays}_BEN_POUC")

    # ── Feuille supplémentaire pour le UK ──────────────────────────────
    if pays == "UK":
        import_sheet(fichier, "FIXED_BNFT_LIMITS", f"{pays}FIXED_BNFT_LIMITS")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION POUR TOUS LES PAYS
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
