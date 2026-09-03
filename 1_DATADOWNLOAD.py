# -*- coding: utf-8 -*-
"""
EXTRACTION CLAIMS — construction des bases CLMHDR & CLMTRNS
(cases reserves ICOP & RBNP)

Version Databricks : les tables sources du data lake sont DÉJÀ disponibles
dans le catalogue Databricks (schéma 'claim'), donc pas de connexion ODBC/JDBC.
On lit directement avec spark.table().
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete = "2026_06_Prov"

# Schémas Databricks
SOURCE_SCHEMA = "claim"   # où sont les tables sources (ex-LIBNAME CLAIM2 ODBC)
TARGET_SCHEMA = "data"    # où écrire les tables résultat (ex-LIBNAME data)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")


# ═══════════════════════════════════════════════════════════════════════
# FONCTION D'EXTRACTION PAR PAYS
# ═══════════════════════════════════════════════════════════════════════
def datadownload_dtw(pays):
    """Construit les bases CLMHDR (en-têtes) et CLMTRNS (détail) pour un pays."""

    # ── CLMHDR : en-têtes de sinistres ─────────────────────────────────
    # Sélection normalisée directement depuis la table source.
    # format=/length= du SAS supprimés (inutiles en Spark) ;
    # datepart() → to_date() (les colonnes source sont des timestamps).
    clmhdr = spark.sql(f"""
        SELECT
            COUNTRY_CD                      AS Country,
            CLA_CASE_NO,
            POLICY_LINE_NO,
            POLICY_LINE_SEQ_NO,
            COVER,
            scheme,
            to_date(incident_date)          AS incident_date,
            to_date(NOTIFICATION_DATE)      AS NOTIFICATION_DATE,
            to_date(COVER_START_DATE)       AS COVER_START_DATE,
            to_date(COVER_END_DATE)         AS COVER_END_DATE,
            to_date(FIRST_OPEN_DATE)        AS FIRST_OPEN_DATE,
            to_date(FIRST_CLOSE_DATE)       AS FIRST_CLOSE_DATE,
            to_date(REOPEN_DATE)            AS REOPEN_DATE,
            to_date(RECLOSE_DATE)           AS RECLOSE_DATE,
            STATUS,
            CLOSE_CODE,
            INSURANCE_TERM,
            UW_COMPANY,
            CLAIM_MONTHLY_BENEFIT,
            POLICY_MONTHLY_BENEFIT,
            OUTSTANDING_LIFE_BALANCE,
            to_date(POLICY_EXPIRY_DATE)     AS POLICY_EXPIRY_DATE,
            MAX_NO_OF_PAYMENTS,
            IS_BULK,
            OUTSTANDING_NONLIFE_BALANCE,
            GROUP_POL_NO,
            TOTAL_PAYMENTS,
            TOTAL_NON_OTHER_PAYMENTS,
            TOTAL_NON_OTHER_PAYMENTS_AMT,
            to_date(BIRTH_DATE)             AS BIRTH_DATE,
            GENDER,
            POLICY_NO,
            EVENT_TYPE,
            DECLINE,
            DECLINE_REASON_REF,
            potential_clm_amt               AS POTENTIAL_CLM_AMT,
            to_date(DECISION_DATE)          AS DECISION_DATE,
            to_date(last_activity_date)     AS last_activity_date,
            PROD_ID,
            cause_code,
            informer_type
        FROM {SOURCE_SCHEMA}.{pays}_claim_head_tiariadmin
    """)
    clmhdr.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.{pays}_CLMHDR")

    # ── CLMTRNS : détail des transactions de sinistres ─────────────────
    clmtrns = spark.sql(f"""
        SELECT
            COUNTRY_CD                      AS Country,
            CLA_CASE_NO,
            to_date(TRANS_DATE)             AS TRANS_DATE,
            CURRENCY_AMT,
            SPECIFICATION,
            ITEM_CLASS,
            GROSS_AMT,
            to_date(DUE_DATE)               AS DUE_DATE,
            SUBITEM_TYPE,
            ACC_ITEM_NO
        FROM {SOURCE_SCHEMA}.{pays}_claim_det_tiariadmin
    """)
    clmtrns.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.{pays}_CLMTRNS")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION POUR TOUS LES PAYS
# ═══════════════════════════════════════════════════════════════════════
pays_list = [
    "DE", "FR", "FI", "NO", "IT", "ES", "IE", "GR", "NI", "NL",
    "PL", "PT", "TR", "DK", "SE", "UK", "CH", "AT", "BE", "MX",
    "LU", "LT", "CO", "EE", "KR", "PE", "LV",
]

for pays in pays_list:
    print(f"Extraction claims pour {pays}...")
    try:
        datadownload_dtw(pays=pays)
    except Exception as e:
        # Certains pays ont des tables vides — on continue sans bloquer
        print(f"  ⚠ {pays} : {e}")


# ═══════════════════════════════════════════════════════════════════════
# CORRECTION UK : le code pays source est 'GB', normalisé en 'UK'
# ═══════════════════════════════════════════════════════════════════════
uk_clmhdr = (spark.table(f"{TARGET_SCHEMA}.UK_CLMHDR")
             .withColumn("Country",
                 F.when(F.col("Country") == "GB", F.lit("UK"))
                  .otherwise(F.col("Country"))))
uk_clmhdr.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.UK_CLMHDR")

uk_clmtrns = (spark.table(f"{TARGET_SCHEMA}.UK_CLMTRNS")
              .withColumn("Country",
                  F.when(F.col("Country") == "GB", F.lit("UK"))
                   .otherwise(F.col("Country"))))
uk_clmtrns.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.UK_CLMTRNS")

print("Extraction claims terminée.")
