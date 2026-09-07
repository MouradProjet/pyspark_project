
"""
SPLIT COUNTRY — découpe la base P&L globale (DAAP niveau 1, due only) par pays.

Pour chaque pays, filtre GLOBAL sur countryid_vorig = pays et sauvegarde
la table tia.GLOBAL_PL_{country}.

Note : la macro SAS %resize (redimensionnement des colonnes caractères à leur
longueur minimale) n'a PAS d'équivalent utile en Spark — les chaînes Spark
n'ont pas de longueur fixe. Elle est donc supprimée.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete    = "2026_04_V2"
TIA_SCHEMA = "tia"          # ex-LIBNAME TIA

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TIA_SCHEMA}")


def split_country(country):
    """Filtre la base globale sur le pays et sauvegarde tia.GLOBAL_PL_{country}."""
    df = (spark.table(f"{TIA_SCHEMA}.DAAP_LEVEL_1_DUEONLY")
          .filter(F.col("countryid_vorig") == country))
    df.write.mode("overwrite").saveAsTable(f"{TIA_SCHEMA}.GLOBAL_PL_{country}")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION POUR TOUS LES PAYS
# ═══════════════════════════════════════════════════════════════════════
pays_list = [
    "PE", "LU", "AT", "BE", "CH", "CO", "DE", "DK", "FI", "FR",
    "GR", "IE", "MX", "NI", "NL", "NO", "PL", "PT", "SE", "TR",
    "UK", "LT", "ES", "IT",
]

for country in pays_list:
    print(f"Split P&L pour {country}...")
    try:
        split_country(country)
    except Exception as e:
        print(f"  ⚠ {country} : {e}")

print("Split terminé.")
