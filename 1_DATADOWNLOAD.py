# -*- coding: utf-8 -*-
"""
EXTRACTION CLAIMS DU DATA LAKE
Construit les bases CLMHDR & CLMTRNS (cases reserves ICOP & RBNP)
Traduction PySpark du programme SAS d'origine (ALSENY SOW, 2018).

Ce script est du CODE D'INFRASTRUCTURE (extraction depuis une base externe
WPS_SHINE_BLCL via JDBC). La logique de connexion doit être adaptée à votre
environnement Databricks (URL, driver, secrets).
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES — à compléter par l'utilisateur
# ═══════════════════════════════════════════════════════════════════════
arrete    = "2026_06_Prov"
data_path = (f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
             f"/02_Elements_Techniques/TIA/Extraction Donnees/Claims Extracts")

# Schéma cible pour les tables extraites (remplace le LIBNAME 'data')
spark.sql("CREATE SCHEMA IF NOT EXISTS data")

# ═══════════════════════════════════════════════════════════════════════
# CONNEXION AU DATA LAKE (ex-LIBNAME CLAIM / CLAIM2 ODBC → JDBC Spark)
# ═══════════════════════════════════════════════════════════════════════
# Le SAS utilisait une connexion ODBC vers WPS_SHINE_BLCL, schéma
# 'global_claims_extracts'. En Databricks, on lit via JDBC.
# ADAPTEZ ces paramètres à votre environnement (URL, driver, identifiants).
#
# Idéalement, stockez les identifiants dans un secret scope Databricks :
#   dbutils.secrets.get(scope="wps", key="jdbc-url")
JDBC_URL    = "jdbc:db2://<host>:<port>/WPS_SHINE_BLCL"   # TODO: à renseigner
JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver"                 # TODO: driver réel
JDBC_USER   = dbutils.secrets.get(scope="wps", key="user")      # TODO
JDBC_PWD    = dbutils.secrets.get(scope="wps", key="password")  # TODO
SOURCE_SCHEMA = "global_claims_extracts"


def read_claim_table(table_name):
    """Lit une table du data lake claims via JDBC."""
    return (spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", JDBC_DRIVER)
            .option("user", JDBC_USER)
            .option("password", JDBC_PWD)
            .option("dbtable", f"{SOURCE_SCHEMA}.{table_name}")
            .load())


# ═══════════════════════════════════════════════════════════════════════
# FONCTION D'EXTRACTION PAR PAYS
# ═══════════════════════════════════════════════════════════════════════
def datadownload_dtw(pays):
    """Extrait et normalise les bases claim header (CLMHDR) et detail
    (CLMTRNS) pour un pays donné."""

    # ── CLMHDR : en-têtes de sinistres ─────────────────────────────────
    # 1. Copie brute de la table source vers le schéma 'data'
    claim_header = read_claim_table(f"{pays}_claim_head_tiariadmin")
    claim_header.write.mode("overwrite").saveAsTable(f"data.{pays}_claim_header")

    # 2. Sélection normalisée (dates converties, colonnes utiles)
    #    Les format=/length= du SAS sont supprimés (inutiles en Spark) ;
    #    datepart() → to_date() car les colonnes source sont des timestamps.
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
        FROM data.{pays}_claim_header
    """)
    clmhdr.write.mode("overwrite").saveAsTable(f"data.{pays}_CLMHDR")

    # ── CLMTRNS : détail des transactions de sinistres ─────────────────
    claim_detail = read_claim_table(f"{pays}_claim_det_tiariadmin")
    claim_detail.write.mode("overwrite").saveAsTable(f"data.{pays}_claim_detail")

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
        FROM data.{pays}_claim_detail
    """)
    clmtrns.write.mode("overwrite").saveAsTable(f"data.{pays}_CLMTRNS")


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
# CORRECTION UK : le code pays source est 'GB', on le normalise en 'UK'
# ═══════════════════════════════════════════════════════════════════════
uk_clmhdr = (spark.table("data.UK_CLMHDR")
             .withColumn("Country",
                 F.when(F.col("Country") == "GB", F.lit("UK"))
                  .otherwise(F.col("Country"))))
uk_clmhdr.write.mode("overwrite").saveAsTable("data.UK_CLMHDR")

uk_clmtrns = (spark.table("data.UK_CLMTRNS")
              .withColumn("Country",
                  F.when(F.col("Country") == "GB", F.lit("UK"))
                   .otherwise(F.col("Country"))))
uk_clmtrns.write.mode("overwrite").saveAsTable("data.UK_CLMTRNS")

print("Extraction claims terminée.")
