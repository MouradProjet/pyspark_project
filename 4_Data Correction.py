# -*- coding: utf-8 -*-
"""
EXTRACTION CLAIMS DU DATA LAKE — Correction & mise en forme
Construit les bases CLMHDR & CLMTRNS pour le calcul des cases reserves (ICOP & RBNP).
Auteur original SAS : ALSENY SOW. Traduction PySpark manuelle propre.

Prérequis : les tables data.{pays}_CLMHDR, data.{pays}_CLMTRNS et les tables
de paramètres ({pays}_RESERVE_GROUP_SPEC, {pays}_MNTHLY_BNFT_LIMITS,
{pays}_OTSTANDING_BLNC_LIMITS, {pays}_TRANS_TYPE_MAP, {pays}_SCHEME_DATABASE)
sont déjà disponibles dans le catalogue Databricks.
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete       = "2026_06_Prov"
balancedate  = "2026-06-26"          # date de balance au format ISO (yyyy-MM-dd)
INPUT_SCHEMA = "input"               # ex-LIBNAME input
DATA_SCHEMA  = "data"                # ex-LIBNAME data

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {INPUT_SCHEMA}")

# balancedate comme littéral date Spark, réutilisé dans les requêtes
BAL = f"DATE'{balancedate}'"


def data_correction(pays):
    """Corrige et met en forme les claims d'un pays."""

    # ═══════════════════════════════════════════════════════════════════
    # 1. CLMHDR_0 : sélection + normalisation des dates
    # ═══════════════════════════════════════════════════════════════════
    clmhdr_0 = spark.sql(f"""
        SELECT
            Country,
            CLA_CASE_NO              AS Clm_Nmbr,
            POLICY_LINE_NO           AS Policy_Line_No,
            POLICY_LINE_SEQ_NO       AS Policy_Line_Seq_No,
            cover                    AS Cvr_Typ,
            SCHEME                   AS Schm,
            INCIDENT_DATE            AS Accdnt_Dt,
            year(INCIDENT_DATE)      AS Acc_Yr,
            month(INCIDENT_DATE)     AS Acc_Mnth,
            FIRST_OPEN_DATE          AS Rgstrtn_Dt,
            year(FIRST_OPEN_DATE)    AS Rgstrtn_Yr,
            month(FIRST_OPEN_DATE)   AS Rgstrtn_Mnth,
            FIRST_CLOSE_DATE,
            RECLOSE_DATE,
            STATUS,
            OUTSTANDING_LIFE_BALANCE,
            OUTSTANDING_NONLIFE_BALANCE,
            POTENTIAL_CLM_AMT,
            uw_company               AS Undrwrtng_Cmpny,
            MAX_NO_OF_PAYMENTS       AS Max_Nmbr_Bnfts,
            CASE WHEN INSURANCE_TERM = 1
                 THEN date_add(cover_END_DATE, 20*365)
                 ELSE cover_END_DATE END       AS Expry_dt,
            cover_START_DATE         AS Incptn_Dt,
            INSURANCE_TERM           AS Insrnc_Trm,
            CLAIM_MONTHLY_BENEFIT,
            POLICY_MONTHLY_BENEFIT,
            IS_BULK,
            PROD_ID                  AS Prdct,
            GENDER                   AS Gndr,
            BIRTH_DATE               AS Dt_of_Brth,
            cause_code,
            informer_type,
            Legal_Entity
        FROM {DATA_SCHEMA}.{pays}_CLMHDR
        WHERE STATUS NOT IN ('EC')
          AND FIRST_OPEN_DATE <= {BAL}
          AND FIRST_OPEN_DATE IS NOT NULL
          AND INCIDENT_DATE   IS NOT NULL
    """)

    # Correction Insurance Term (< 1 ou null) pour les bulk → 1
    clmhdr_0 = clmhdr_0.withColumn(
        "Insrnc_Trm",
        F.when(
            ((F.col("Insrnc_Trm") < 1) | F.col("Insrnc_Trm").isNull())
            & (F.col("IS_BULK") == "Y"),
            F.lit(1)
        ).otherwise(F.col("Insrnc_Trm")))
    clmhdr_0.createOrReplaceTempView(f"{pays}_CLMHDR_0")

    # ═══════════════════════════════════════════════════════════════════
    # 2. CLMTRNS_all : transactions avec année/mois comptable
    # ═══════════════════════════════════════════════════════════════════
    clmtrns = spark.sql(f"""
        SELECT
            t.country,
            t.CLA_CASE_NO AS Clm_Nmbr,
            t.TRANS_DATE  AS Trns_Dt,
            CASE WHEN day(TRANS_DATE) > day({BAL})
                 THEN CASE WHEN month(TRANS_DATE)=12 THEN year(TRANS_DATE)+1 ELSE year(TRANS_DATE) END
                 ELSE year(TRANS_DATE) END           AS Acnt_Yr,
            CASE WHEN day(TRANS_DATE) > day({BAL})
                 THEN CASE WHEN month(TRANS_DATE)=12 THEN 1 ELSE month(TRANS_DATE)+1 END
                 ELSE month(TRANS_DATE) END           AS Acnt_Mnth,
            -t.GROSS_AMT AS Amt,
            CASE WHEN t.ITEM_CLASS = 2 THEN 'C' ELSE m.TRANS_TYPE END AS Trns_Type
        FROM {DATA_SCHEMA}.{pays}_CLMTRNS t
        LEFT  JOIN {pays}_TRANS_TYPE_MAP m ON t.SPECIFICATION = m.SPECIFICATION
        INNER JOIN {pays}_CLMHDR_0       h ON t.CLA_CASE_NO   = h.Clm_Nmbr
        WHERE h.Clm_Nmbr IS NOT NULL
          AND t.TRANS_DATE <= {BAL}
        ORDER BY CLA_CASE_NO
    """)
    clmtrns.write.mode("overwrite").saveAsTable(f"{INPUT_SCHEMA}.{pays}_CLMTRNS_all")
    clmtrns.createOrReplaceTempView(f"{pays}_CLMTRNS_all")

    # ═══════════════════════════════════════════════════════════════════
    # 3. CLMHDR : benefit mensuel + date de clôture
    # ═══════════════════════════════════════════════════════════════════
    clmhdr = (spark.table(f"{pays}_CLMHDR_0")
        .withColumn("Mnthly_Bnft",
            F.when((F.col("CLAIM_MONTHLY_BENEFIT") != 0) & F.col("CLAIM_MONTHLY_BENEFIT").isNotNull(),
                   F.col("CLAIM_MONTHLY_BENEFIT"))
             .when((F.col("POLICY_MONTHLY_BENEFIT") != 0) & F.col("POLICY_MONTHLY_BENEFIT").isNotNull(),
                   F.col("POLICY_MONTHLY_BENEFIT")))
        .withColumn("Cls_Dt",
            F.when(F.col("RECLOSE_DATE").isNotNull(), F.col("RECLOSE_DATE"))
             .when(F.col("FIRST_CLOSE_DATE").isNotNull(), F.col("FIRST_CLOSE_DATE")))
        .drop("CLAIM_MONTHLY_BENEFIT", "POLICY_MONTHLY_BENEFIT", "RECLOSE_DATE", "FIRST_CLOSE_DATE"))
    clmhdr.createOrReplaceTempView(f"{pays}_CLMHDR_1")

    # ═══════════════════════════════════════════════════════════════════
    # 4. Total payé par claim
    # ═══════════════════════════════════════════════════════════════════
    total_paid = spark.sql(f"""
        SELECT Clm_Nmbr,
               sum(Amt) AS Totl_Amnt_Pd,
               sum(CASE WHEN Trns_Type = 'O' THEN 0 ELSE Amt END) AS Totl_Bnfts_Amnt_Pd
        FROM {INPUT_SCHEMA}.{pays}_CLMTRNS_all
        GROUP BY Clm_Nmbr
    """)
    total_paid.createOrReplaceTempView(f"{pays}_TOTAL_AMOUNT_PAID")

    # 5. CLMHDR_2 = CLMHDR_1 + total payé (jointure), null → 0
    clmhdr_2 = (spark.table(f"{pays}_CLMHDR_1")
        .join(total_paid, ["Clm_Nmbr"], "left")
        .withColumn("Totl_Amnt_Pd", F.coalesce(F.col("Totl_Amnt_Pd"), F.lit(0)))
        .withColumn("Totl_Bnfts_Amnt_Pd", F.coalesce(F.col("Totl_Bnfts_Amnt_Pd"), F.lit(0))))
    clmhdr_2.createOrReplaceTempView(f"{pays}_CLMHDR_2")

    # 6. Premier / dernier benefit payé
    firstlast = spark.sql(f"""
        SELECT Clm_Nmbr,
            CASE WHEN day(min(Trns_Dt)) > day({BAL})
                 THEN CASE WHEN month(min(Trns_Dt))=12 THEN year(min(Trns_Dt))+1 ELSE year(min(Trns_Dt)) END
                 ELSE year(min(Trns_Dt)) END AS Frst_Bnft_Pd_Yr,
            CASE WHEN day(min(Trns_Dt)) > day({BAL})
                 THEN CASE WHEN month(min(Trns_Dt))=12 THEN 1 ELSE month(min(Trns_Dt))+1 END
                 ELSE month(min(Trns_Dt)) END AS Frst_Bnft_Pd_Mnth,
            CASE WHEN day(max(Trns_Dt)) > day({BAL})
                 THEN CASE WHEN month(max(Trns_Dt))=12 THEN year(max(Trns_Dt))+1 ELSE year(max(Trns_Dt)) END
                 ELSE year(max(Trns_Dt)) END AS latst_Bnft_Pd_Yr,
            CASE WHEN day(max(Trns_Dt)) > day({BAL})
                 THEN CASE WHEN month(max(Trns_Dt))=12 THEN 1 ELSE month(max(Trns_Dt))+1 END
                 ELSE month(max(Trns_Dt)) END AS latst_Bnft_Pd_Mnth
        FROM {INPUT_SCHEMA}.{pays}_CLMTRNS_all
        WHERE Amt > 1 AND Trns_Type <> 'O'
        GROUP BY Clm_Nmbr
    """)
    firstlast.createOrReplaceTempView(f"{pays}_FIRSTLASTBENEFIT")

    # 7. CLMHDR_3 = CLMHDR_2 + firstlast, null → 0, Prdct dérivé du Schm
    clmhdr_3 = (spark.table(f"{pays}_CLMHDR_2")
        .join(firstlast, ["Clm_Nmbr"], "left")
        .withColumn("Frst_Bnft_Pd_Yr", F.coalesce(F.col("Frst_Bnft_Pd_Yr"), F.lit(0)))
        .withColumn("Frst_Bnft_Pd_Mnth", F.coalesce(F.col("Frst_Bnft_Pd_Mnth"), F.lit(0)))
        .withColumn("latst_Bnft_Pd_Yr", F.coalesce(F.col("latst_Bnft_Pd_Yr"), F.lit(0)))
        .withColumn("latst_Bnft_Pd_Mnth", F.coalesce(F.col("latst_Bnft_Pd_Mnth"), F.lit(0)))
        .withColumn("OUTSTANDING_LIFE_BALANCE", F.coalesce(F.col("OUTSTANDING_LIFE_BALANCE"), F.lit(0)))
        .withColumn("OUTSTANDING_NONLIFE_BALANCE", F.coalesce(F.col("OUTSTANDING_NONLIFE_BALANCE"), F.lit(0)))
        .withColumn("POTENTIAL_CLM_AMT", F.coalesce(F.col("POTENTIAL_CLM_AMT"), F.lit(0)))
        .withColumn("Mnthly_Bnft", F.coalesce(F.col("Mnthly_Bnft"), F.lit(0)))
        .withColumn("Prdct",
            F.when((F.col("Prdct") == "") & (F.length("Schm") == 4), F.expr("substring(Schm,1,2)"))
             .when((F.col("Prdct") == "") & (F.length("Schm") > 3), F.expr("substring(Schm,1,3)"))
             .otherwise(F.col("Prdct"))))
    clmhdr_3.createOrReplaceTempView(f"{pays}_CLMHDR_3")

    # ═══════════════════════════════════════════════════════════════════
    # 8. Ajout du Reserve Group (règle différente pour FR)
    # ═══════════════════════════════════════════════════════════════════
    if pays == "FR":
        # FR : jointure sur SCHEME_DATABASE puis sous-produit puis reserve group
        clmhdr_all = spark.sql(f"""
            SELECT h.*, s.SUB_PRODUCT,
                   s.PAYMENT_BENEFIT AS Clm_Pymnt_Basis, s.PRODUCT_TYPE
            FROM {pays}_CLMHDR_3 h
            LEFT JOIN {pays}_SCHEME_DATABASE s
              ON h.Schm = s.Schm AND h.Cvr_Typ = s.COVER_TYPE
        """)
        clmhdr_all = clmhdr_all.withColumn("Sub_Prdct",
            F.when(~F.col("SUB_PRODUCT").isin("MORTGAGE"), F.lit("NMORTGAGE"))
             .when(F.col("SUB_PRODUCT").isin("MORTGAGE"), F.lit("MORTGAGE")))
        clmhdr_all.createOrReplaceTempView(f"{pays}_CLMHDR_all_0")

        clmhdr_all = spark.sql(f"""
            SELECT h.*, s.Rsrv_Grp
            FROM {pays}_CLMHDR_all_0 h
            LEFT JOIN {pays}_RESERVE_GROUP_SPEC s
              ON h.Cvr_Typ = s.Cvr_Typ AND h.Sub_Prdct = s.Sub_Prdct
                 AND h.Clm_Pymnt_Basis = s.Clm_Pymnt_Basis
        """)
        clmhdr_all = clmhdr_all.withColumn("Rsrv_Grp",
            F.when((F.col("Rsrv_Grp") == "") & F.col("Cvr_Typ").isin("DA","DB","DS","DC"), F.lit("GD1"))
             .when((F.col("Rsrv_Grp") == "") & F.col("Cvr_Typ").isin("DI","DJ"), F.lit("GD3"))
             .when((F.col("Rsrv_Grp") == "") & F.col("Cvr_Typ").isin("RR","RU"), F.lit("GR1"))
             .when((F.col("Rsrv_Grp") == "") & F.col("Cvr_Typ").isin("GP"), F.lit("GP1"))
             .when((F.col("Rsrv_Grp") == "") & F.col("Cvr_Typ").isin("LA","LL","LR","DY","DZ"), F.lit("GL1"))
             .when(F.col("Rsrv_Grp") == "", F.lit("ZZ1"))
             .otherwise(F.col("Rsrv_Grp")))
    else:
        # Autres pays : jointure directe sur RESERVE_GROUP_SPEC (Cvr_Typ)
        clmhdr_all = spark.sql(f"""
            SELECT h.*, s.Rsrv_Grp
            FROM {pays}_CLMHDR_3 h
            LEFT JOIN {pays}_RESERVE_GROUP_SPEC s ON h.Cvr_Typ = s.Cvr_Typ
            ORDER BY Clm_Nmbr
        """)
        clmhdr_all = clmhdr_all.withColumn("Rsrv_Grp",
            F.when(F.col("Rsrv_Grp") == "", F.lit("ZZ1")).otherwise(F.col("Rsrv_Grp")))

    clmhdr_all.createOrReplaceTempView(f"{pays}_CLMHDR_all")

    # ═══════════════════════════════════════════════════════════════════
    # 9. FILTRES : mettre en ZZ2 les schemes hors on-system (par pays)
    # ═══════════════════════════════════════════════════════════════════
    def to_zz2(df, cond):
        return df.withColumn("Rsrv_Grp",
            F.when(F.expr(cond), F.lit("ZZ2")).otherwise(F.col("Rsrv_Grp")))

    df = spark.table(f"{pays}_CLMHDR_all")

    if pays == "ES":
        df = to_zz2(df, "Schm LIKE 'H1%' OR Schm LIKE 'H2%' OR Schm LIKE 'H3%' OR Schm LIKE 'H4%' "
                        "OR Schm LIKE 'H5%' OR Schm LIKE 'H6%' OR Schm LIKE 'HPA%' "
                        "OR Schm LIKE 'S1%' OR Schm LIKE 'S2%' OR Schm LIKE 'S3%' OR Schm LIKE 'S4%' "
                        "OR Schm LIKE 'S5%' OR Schm LIKE 'S6%' OR Schm LIKE 'S7%'")
    if pays == "IT":
        df = to_zz2(df, "Schm LIKE 'LN1%'")
    if pays == "NO":
        df = to_zz2(df, "Schm IN ('TA.1','TB.1','TC.1','TD.1','TE.1','TF.1','TG.1','TH.1','TI.1','TJ.1')")
        df = to_zz2(df, "Schm LIKE 'ED.%' OR Schm LIKE 'EE.%' OR Schm LIKE 'EG.%' OR Schm LIKE 'EH.%' "
                        "OR Schm LIKE 'EI.%' OR Schm LIKE 'EJ.%' OR Schm LIKE 'EK.%' OR Schm LIKE 'EL.%' OR Schm LIKE 'EM.%'")
    if pays in ("DE", "TR"):
        df = to_zz2(df, "Undrwrtng_Cmpny IN ('501','502')")
    if pays == "DK":
        df = to_zz2(df, "Schm LIKE '5B%' OR Schm LIKE '5C%' OR Schm LIKE '1F%' OR Schm LIKE '1G%'")
        df = to_zz2(df, "Schm LIKE 'Q%'")
    if pays == "FI":
        df = to_zz2(df, "Schm LIKE 'SN%'")
    if pays == "SE":
        df = to_zz2(df, "Schm LIKE 'ED.%' OR Schm LIKE 'EE.%' OR Schm LIKE 'EF.%' OR Schm LIKE 'EG.%' "
                        "OR Schm LIKE 'EH.%' OR Schm LIKE 'EI.%' OR Schm LIKE 'EJ.%'")
        df = to_zz2(df, "Schm LIKE 'ZA.%'")
    else:
        # ATTENTION : dans le SAS, le 8A.% est un %ELSE — il s'applique à TOUS
        # les pays SAUF SE (pas seulement quelques-uns).
        df = to_zz2(df, "Schm LIKE '8A.%'")
    if pays == "UK":
        df = to_zz2(df, "Schm LIKE 'CFA%' OR Schm LIKE 'CFN%'")
    if pays == "GR":
        df = to_zz2(df, "Schm IN ('BPI.1','BPJ.1','BPK.1','BPL.1','BPM.1','EM1.1','EM2.1','GM1.1')")
    if pays == "DE":
        df = to_zz2(df, "Schm IN ('P4.2','P4.3')")
    if pays == "IE":
        df = to_zz2(df, "Schm IN ('EV.3','EV.4')")

    # Suisse Cembra : max 9 paiements pour certains schemes récents (ex-IF/THEN manuel)
    if pays == "CH":
        df = df.withColumn("Max_Nmbr_Bnfts",
            F.when(
                F.col("Schm").isin("GO.1","GN.1","G9.1","G9.2","G9.3","G9.4",
                                   "G3.1","G3.2","G3.3","G3.4","G6.1","G6.2","G6.3","GL.1","GM.1")
                & (F.year("Rgstrtn_Dt") > 2014)
                & (F.col("Max_Nmbr_Bnfts") == 12),
                F.lit(9)
            ).otherwise(F.col("Max_Nmbr_Bnfts")))

    # France : corrections spécifiques
    if pays == "FR":
        df = df.withColumn("Undrwrtng_Cmpny",
            F.when(F.expr("Schm LIKE '1%' AND Cvr_Typ LIKE 'D%'"), F.lit("102"))
             .otherwise(F.col("Undrwrtng_Cmpny")))
        df = df.withColumn("Schm",
            F.when(F.col("Clm_Nmbr") == 1050126, F.lit("EFD.1")).otherwise(F.col("Schm")))

    df.createOrReplaceTempView(f"{pays}_CLMHDR_all")

    # ═══════════════════════════════════════════════════════════════════
    # 10. Correction du monthly benefit (moyenne par produit/groupe)
    # ═══════════════════════════════════════════════════════════════════
    # Moyenne par produit
    avg_prdct = spark.sql(f"""
        SELECT h.Rsrv_Grp, h.Prdct, count(h.Clm_Nmbr) AS COUNT, mean(h.Mnthly_Bnft) AS Avg_Mnthly_Bnft
        FROM {pays}_CLMHDR_all h
        INNER JOIN {pays}_MNTHLY_BNFT_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
        WHERE h.Mnthly_Bnft > m.LOWER AND h.Mnthly_Bnft < m.UPPER
        GROUP BY h.Rsrv_Grp, h.Prdct
    """)
    avg_prdct.createOrReplaceTempView("MNTHLY_BNFT_AVRG_PRDCT")
    # Moyenne par groupe
    avg_grp = spark.sql(f"""
        SELECT h.Rsrv_Grp, count(h.Clm_Nmbr) AS COUNT, mean(h.Mnthly_Bnft) AS Avg_Mnthly_Bnft
        FROM {pays}_CLMHDR_all h
        INNER JOIN {pays}_MNTHLY_BNFT_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
        WHERE h.Mnthly_Bnft > m.LOWER AND h.Mnthly_Bnft < m.UPPER
        GROUP BY h.Rsrv_Grp
    """)
    avg_grp.createOrReplaceTempView("MNTHLY_BNFT_AVRG_GRP")
    # Claims à corriger + valeur corrigée
    corr = spark.sql(f"""
        SELECT DISTINCT c.Clm_Nmbr,
               CASE WHEN p.COUNT > 9 THEN p.Avg_Mnthly_Bnft ELSE g.Avg_Mnthly_Bnft END AS Mnthly_Bnft
        FROM (
            SELECT h.Clm_Nmbr, h.Rsrv_Grp, h.Prdct, h.Mnthly_Bnft
            FROM {pays}_CLMHDR_all h
            INNER JOIN {pays}_MNTHLY_BNFT_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
            WHERE (h.Mnthly_Bnft < m.LOWER OR h.Mnthly_Bnft > m.UPPER OR h.Mnthly_Bnft IS NULL)
              AND h.STATUS IN ('OP','RO')
        ) c
        LEFT JOIN MNTHLY_BNFT_AVRG_PRDCT p ON c.Prdct = p.Prdct AND c.Rsrv_Grp = p.Rsrv_Grp
        LEFT JOIN MNTHLY_BNFT_AVRG_GRP   g ON c.Rsrv_Grp = g.Rsrv_Grp
    """)
    corr.createOrReplaceTempView("MNTHLY_BNFT_CORR")
    # Appliquer : remplacer Mnthly_Bnft par la valeur corrigée quand elle existe
    df = (spark.table(f"{pays}_CLMHDR_all").alias("h")
          .join(corr.select("Clm_Nmbr", F.col("Mnthly_Bnft").alias("Mnthly_Bnft_new")).alias("c"),
                ["Clm_Nmbr"], "left")
          .withColumn("Mnthly_Bnft",
              F.when(F.col("Mnthly_Bnft_new").isNotNull(), F.col("Mnthly_Bnft_new"))
               .otherwise(F.col("Mnthly_Bnft")))
          .drop("Mnthly_Bnft_new"))
    df.createOrReplaceTempView(f"{pays}_CLMHDR_all")

    # ═══════════════════════════════════════════════════════════════════
    # 11. Correction outstanding balance
    # ═══════════════════════════════════════════════════════════════════
    # Si OUTSTANDING_LIFE_BALANCE est null ou 0 → prendre le non-life
    df = spark.table(f"{pays}_CLMHDR_all").withColumn(
        "OUTSTANDING_LIFE_BALANCE",
        F.when(F.col("OUTSTANDING_LIFE_BALANCE").isNull() | (F.col("OUTSTANDING_LIFE_BALANCE") == 0),
               F.col("OUTSTANDING_NONLIFE_BALANCE"))
         .otherwise(F.col("OUTSTANDING_LIFE_BALANCE")))
    df.createOrReplaceTempView(f"{pays}_CLMHDR_all")

    # Montants des transactions (item class 2,3,4) par claim
    claims_trns = spark.sql(f"""
        SELECT DISTINCT t.CLA_CASE_NO AS Clm_Nmbr, sum(-GROSS_AMT) AS AMT
        FROM {DATA_SCHEMA}.{pays}_CLMTRNS t
        INNER JOIN {DATA_SCHEMA}.{pays}_CLMHDR h ON h.CLA_CASE_NO = t.CLA_CASE_NO
        WHERE ITEM_CLASS IN (2,3,4)
        GROUP BY t.CLA_CASE_NO
    """)
    claims_trns.createOrReplaceTempView("CLAIMS_IN_CLMTRNS")

    # Moyennes par produit / groupe
    blnc_avg_prdct = spark.sql(f"""
        SELECT h.Rsrv_Grp, h.Prdct, count(h.Clm_Nmbr) AS COUNT, mean(t.AMT) AS AVG_AMT
        FROM CLAIMS_IN_CLMTRNS t
        INNER JOIN {pays}_CLMHDR_all h ON t.Clm_Nmbr = h.Clm_Nmbr
        INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
        WHERE t.AMT > m.LOWER AND t.AMT < m.UPPER AND t.AMT IS NOT NULL
        GROUP BY h.Rsrv_Grp, h.Prdct
    """)
    blnc_avg_prdct.createOrReplaceTempView("OTSTANDING_BLNC_AVRG_PRDCT")
    blnc_avg_grp = spark.sql(f"""
        SELECT h.Rsrv_Grp, count(h.Clm_Nmbr) AS COUNT, mean(t.AMT) AS AVG_AMT
        FROM CLAIMS_IN_CLMTRNS t
        INNER JOIN {pays}_CLMHDR_all h ON t.Clm_Nmbr = h.Clm_Nmbr
        INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
        WHERE t.AMT > m.LOWER AND t.AMT < m.UPPER AND t.AMT IS NOT NULL
        GROUP BY h.Rsrv_Grp
    """)
    blnc_avg_grp.createOrReplaceTempView("OTSTANDING_BLNC_AVRG_GRP")
    # Claims à corriger + valeur corrigée
    blnc_corr = spark.sql(f"""
        SELECT DISTINCT c.Clm_Nmbr,
               CASE WHEN p.COUNT > 9 THEN p.AVG_AMT ELSE g.AVG_AMT END AS OUTSTANDING_LIFE_BALANCE
        FROM (
            SELECT h.Clm_Nmbr, h.Rsrv_Grp, h.Prdct
            FROM {pays}_CLMHDR_all h
            INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m ON h.Rsrv_Grp = m.Rsrv_Grp
            WHERE h.STATUS IN ('OP','RO')
              AND (h.OUTSTANDING_LIFE_BALANCE < m.LOWER
                   OR h.OUTSTANDING_LIFE_BALANCE > m.UPPER
                   OR h.OUTSTANDING_LIFE_BALANCE IS NULL)
        ) c
        LEFT JOIN OTSTANDING_BLNC_AVRG_PRDCT p ON c.Prdct = p.Prdct AND c.Rsrv_Grp = p.Rsrv_Grp
        LEFT JOIN OTSTANDING_BLNC_AVRG_GRP   g ON c.Rsrv_Grp = g.Rsrv_Grp
    """)
    blnc_corr.createOrReplaceTempView("OTSTNDNG_BLNC_CORR")
    # Appliquer
    df = (spark.table(f"{pays}_CLMHDR_all")
          .join(blnc_corr.select("Clm_Nmbr",
                    F.col("OUTSTANDING_LIFE_BALANCE").alias("OLB_new")), ["Clm_Nmbr"], "left")
          .withColumn("OUTSTANDING_LIFE_BALANCE",
              F.when(F.col("OLB_new").isNotNull(), F.col("OLB_new"))
               .otherwise(F.col("OUTSTANDING_LIFE_BALANCE")))
          .drop("OLB_new")
          .withColumn("OUTSTANDING_LIFE_BALANCE", F.coalesce(F.col("OUTSTANDING_LIFE_BALANCE"), F.lit(0)))
          .drop("OUTSTANDING_NONLIFE_BALANCE")
          .withColumnRenamed("OUTSTANDING_LIFE_BALANCE", "Otstndng_Balnc"))
    df.write.mode("overwrite").saveAsTable(f"{INPUT_SCHEMA}.{pays}_CLMHDR_ALL")
    df.createOrReplaceTempView(f"{pays}_CLMHDR_all")

    # ═══════════════════════════════════════════════════════════════════
    # 12. Correction GAP potential amount (groupe GP1)
    # ═══════════════════════════════════════════════════════════════════
    pot_avg = spark.sql(f"""
        SELECT h.Rsrv_Grp, count(h.Clm_Nmbr) AS COUNT, mean(h.POTENTIAL_CLM_AMT) AS AVG_POTENTIAL_CLM_AMT
        FROM {INPUT_SCHEMA}.{pays}_CLMHDR_ALL h
        WHERE h.STATUS IN ('OP','RO') AND h.Rsrv_Grp = 'GP1'
        GROUP BY h.Rsrv_Grp
    """)
    pot_avg.createOrReplaceTempView("POTENTIAL_CLM_AMT_AVRG_GRP")
    pot_corr = spark.sql(f"""
        SELECT DISTINCT c.Clm_Nmbr,
               CASE WHEN c.POTENTIAL_CLM_AMT = 0 THEN p.AVG_POTENTIAL_CLM_AMT
                    ELSE c.POTENTIAL_CLM_AMT END AS POTENTIAL_CLM_AMT
        FROM (
            SELECT h.Clm_Nmbr, h.Rsrv_Grp, h.POTENTIAL_CLM_AMT
            FROM {INPUT_SCHEMA}.{pays}_CLMHDR_ALL h
            WHERE h.STATUS IN ('OP','RO') AND h.Rsrv_Grp = 'GP1'
        ) c
        LEFT JOIN POTENTIAL_CLM_AMT_AVRG_GRP p ON c.Rsrv_Grp = p.Rsrv_Grp
    """)
    pot_corr.createOrReplaceTempView("POTENTIAL_CLM_AMT_CORR")
    # Appliquer
    df = (spark.table(f"{INPUT_SCHEMA}.{pays}_CLMHDR_ALL")
          .join(pot_corr.select("Clm_Nmbr",
                    F.col("POTENTIAL_CLM_AMT").alias("POT_new")), ["Clm_Nmbr"], "left")
          .withColumn("POTENTIAL_CLM_AMT",
              F.when(F.col("POT_new").isNotNull(), F.col("POT_new"))
               .otherwise(F.col("POTENTIAL_CLM_AMT")))
          .drop("POT_new"))
    df.write.mode("overwrite").saveAsTable(f"{INPUT_SCHEMA}.{pays}_CLMHDR_ALL")


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION
# ═══════════════════════════════════════════════════════════════════════
pays_list = [
    "FI", "UK", "FR", "SE", "PT", "DE", "NO", "ES", "CH", "IT",
    "PL", "IE", "NL", "NI", "GR", "TR", "DK", "AT", "BE", "CO",
    "MX", "LT", "LV", "EE",
    # "LU",
]

for pays in pays_list:
    print(f"Correction claims pour {pays}...")
    try:
        data_correction(pays)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

print("Correction terminée.")
