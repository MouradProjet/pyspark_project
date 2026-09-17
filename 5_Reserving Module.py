# -*- coding: utf-8 -*-
"""
MODULE RESERVING CLP — calcul des cases reserves ON-SYSTEM (ICOP & RBNP).
Traduction PySpark complète du module SAS (auteurs : ALSENY SOW / GNANISSO SARE).

⚠️ CALCUL RÉGLEMENTAIRE DES PROVISIONS — à VALIDER actuariellement
(comparer Rsrv_Amt_Gross / Rsrv_Amt_Net vs SAS sur un pays témoin) avant production.

Structure :
  - acr(cover_typ, pays)  : calcule les réserves pour un couple (couverture, pays)
  - fusion(pays)          : consolide toutes les couvertures d'un pays + réassurance
  - boucle finale         : ACR × 6 couvertures + fusion, pour chaque pays
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ═══════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════════════
arrete            = "2026_06_Prov"
exer              = "Q226"
yr_of_calculation = "2026"
month_reserving   = 6
day_reserving     = 26
yr_reserving      = 2026

INPUT_SCHEMA  = "input"      # ex-LIBNAME input
OUTPUT_SCHEMA = "cr_q226"    # ex-LIBNAME CR_Q226

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")

BASE = ("~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}"
        "/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES")

# Date de balance (littéral date Spark)
BAL = f"DATE'{yr_reserving:04d}-{month_reserving:02d}-{day_reserving:02d}'"
# Date de reserving (make_date(mois, jour, an) SAS → make_date(an, mois, jour))
DATE_RESERVING = f"DATE'{yr_reserving:04d}-{month_reserving:02d}-{day_reserving:02d}'"

# Familles de couvertures
COVER_DURATION    = {"IU", "DIS"}
COVER_ACCEPTATION = {"LIFE", "CI", "PTD", "GAP"}

# Pays qui utilisent le mapping cover "Updated" (v2) au lieu de v1
PAYS_MAPPING_V2 = {"GR", "SE", "NO", "IT"}


def _sheet(fichier, feuille):
    """Lit une feuille Excel (décimaux préservés)."""
    return (spark.read.format("com.crealytics.spark.excel")
            .option("dataAddress", f"'{feuille}'!A1")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("usePlainNumberFormat", "true")
            .load(fichier))


# ═══════════════════════════════════════════════════════════════════════
# MACRO ACR : calcul des réserves pour un (cover_typ, pays)
# ═══════════════════════════════════════════════════════════════════════
def acr(cover_typ, pays, guideline=12):
    base_dir = BASE.format(arrete=arrete)
    import_02  = f"{base_dir}/Model Properties/Mapping Cover Initial.xlsx"
    import_022 = f"{base_dir}/Model Properties/Mapping Cover Updated.xlsx"

    # ── 1. Mapping des couvertures + Prod_type ─────────────────────────
    clmhdr = (spark.table(f"{INPUT_SCHEMA}.{pays}_CLMHDR_all")
        .withColumn("Prod_type",
            F.when(F.col("Rsrv_Grp").isin("GD1", "GL1", "GR1"), F.lit("Mortgage"))
             .otherwise(F.lit("Non_Mortgage"))))

    mapping_file = import_022 if pays in PAYS_MAPPING_V2 else import_02
    mapping = _sheet(mapping_file, "Mapping_cover").select("Cvr_Typ", "Cover").dropDuplicates()
    clmhdr = clmhdr.join(mapping, ["Cvr_Typ"], "left")

    # Correction SE : Cvr_Typ 'DK' → cover 'DIS'
    clmhdr = clmhdr.withColumn("Cover",
        F.when((F.col("Country") == "SE") & (F.col("Cvr_Typ") == "DK"), F.lit("DIS"))
         .otherwise(F.col("Cover")))
    clmhdr.createOrReplaceTempView(f"{pays}_CLMHDR_ALL")

    # ── 2. Filtrer sur la couverture + hors ZZ1/ZZ2 ────────────────────
    reserves_all = (clmhdr
        .filter((F.col("cover") == cover_typ) & (~F.col("Rsrv_Grp").isin("ZZ1", "ZZ2"))))

    # ── 3. FIRSTLASTBENEFIT : dernière transaction par claim ───────────
    firstlast = spark.sql(f"""
        SELECT Clm_Nmbr,
            CASE WHEN day(max(Trns_Dt)) > day({BAL})
                 THEN year(max(Trns_Dt)) ELSE year(max(Trns_Dt)) END AS latst_Bnft_Pd_Yr,
            CASE WHEN day(max(Trns_Dt)) > day({BAL})
                 THEN CASE WHEN month(max(Trns_Dt))=12 THEN 1 ELSE month(max(Trns_Dt)) END
                 ELSE month(max(Trns_Dt)) END AS latst_Bnft_Pd_Mnth,
            CASE WHEN day(max(Trns_Dt)) > day({BAL})
                 THEN day(max(Trns_Dt)) ELSE day(max(Trns_Dt)) END AS latst_Bnft_Pd_Dy
        FROM {INPUT_SCHEMA}.{pays}_CLMTRNS_ALL
        WHERE Amt > 1 AND Trns_Type <> 'O'
        GROUP BY Clm_Nmbr
    """)
    # trns_dt = date de la dernière transaction
    clmtrns_samp = firstlast.withColumn("trns_dt",
        F.make_date(F.col("latst_Bnft_Pd_Yr"), F.col("latst_Bnft_Pd_Mnth"), F.col("latst_Bnft_Pd_Dy")))

    # ── 4. Classification ICOP / RBNP / CLOSE / NON-CORE ───────────────
    r = (reserves_all
        .withColumn("Date_of_reserving", F.expr(DATE_RESERVING))
        .withColumn("Totl_Bnfts_Amnt_Pd", F.coalesce(F.col("Totl_Bnfts_Amnt_Pd"), F.lit(0)))
        .withColumn("Mnthly_Bnft", F.coalesce(F.col("Mnthly_Bnft"), F.lit(0)))
        .withColumn("Otstndng_Balnc", F.coalesce(F.col("Otstndng_Balnc"), F.lit(0)))
        # Age_surv = int(yrdif(naissance, accident)) = années entières
        .withColumn("Age_surv",
            F.floor(F.datediff(F.col("Accdnt_Dt"), F.col("Dt_of_Brth")) / 365.25).cast("int")))

    # Rsrv_Typ selon Rsrv_Grp / STATUS / dates
    core = ~F.col("Rsrv_Grp").isin("ZZ1", "ZZ2")
    noncore = F.col("Rsrv_Grp").isin("ZZ1", "ZZ2")
    closed = F.col("STATUS").isin("CL", "DC")
    opened = F.col("STATUS").isin("OP", "RO")
    dr = F.col("Date_of_reserving")

    r = r.withColumn("Rsrv_Typ",
        F.when(core & closed, F.lit("CLOSE"))
         .when(noncore & closed, F.lit("NON-CORE CLOSE"))
         .when(noncore & opened, F.lit("NON-CORE OPEN"))
         # OP/RO core : ICOP si premier paiement passé et bénéfices > 1
         .when(core & opened & (F.col("first_pd_date") <= dr) & (F.col("Totl_Bnfts_Amnt_Pd") > 1),
               F.lit("ICOP"))
         # sinon RBNP dans les autres cas OP/RO
         .when(core & opened, F.lit("RBNP"))
         .otherwise(F.lit(None)))
    # cas spécial : CI/LIFE/PTD/GAP avec bénéfices > 0 → RBNP
    r = r.withColumn("Rsrv_Typ",
        F.when(core & opened & F.col("cover").isin("CI", "LIFE", "PTD", "GAP")
               & (F.col("Totl_Bnfts_Amnt_Pd") > 0), F.lit("RBNP"))
         .otherwise(F.col("Rsrv_Typ")))
    r.createOrReplaceTempView(f"Reserves_all_{cover_typ}_{pays}")

    # ── 5. Âge moyen par pays/cover + jointures ────────────────────────
    age_avg = (r.groupBy("Country", "Cover")
        .agg(F.floor(F.mean("Age_surv")).cast("int").alias("AVG_AGE")))
    r = r.join(clmtrns_samp.select("Clm_Nmbr", "trns_dt"), ["Clm_Nmbr"], "left")
    r = r.join(age_avg, ["Country", "Cover"], "left")

    # ── 6. Variables dérivées (Nmbr_Mnths_Pndng, Mnths_lag, Nmbr_Bnfts_Pd)
    mois_reg = F.expr("(year(Date_of_reserving)-year(Rgstrtn_Dt))*12 + month(Date_of_reserving)-month(Rgstrtn_Dt) + 1")
    mois_trns = F.expr("(year(Date_of_reserving)-year(trns_dt))*12 + month(Date_of_reserving)-month(trns_dt)")
    is_rbnp = F.col("Rsrv_Typ") == "RBNP"
    is_icop = F.col("Rsrv_Typ") == "ICOP"
    nb_bnft = F.floor(F.col("Totl_Bnfts_Amnt_Pd") / F.col("Mnthly_Bnft") + 0.5)

    r = (r
        .withColumn("Nmbr_Mnths_Pndng", F.when(is_rbnp, mois_reg).otherwise(F.lit(0)))
        .withColumn("Mnths_lag",
            F.when(is_icop, mois_trns).when(is_rbnp, mois_reg).otherwise(F.lit(0)))
        .withColumn("Nmbr_Mnths_Pndng2", F.col("Nmbr_Mnths_Pndng"))
        .withColumn("Mnths_lag2", F.col("Mnths_lag"))
        .withColumn("Nmbr_Bnfts_Pd",
            F.when(is_icop | (F.col("Rsrv_Typ") == "CLOSE"), nb_bnft).otherwise(F.lit(0)))
        .withColumn("Nmbr_Bnfts_Pd2", F.col("Nmbr_Bnfts_Pd"))
        .withColumn("Nmbr_Bnfts_Pd", F.coalesce(F.col("Nmbr_Bnfts_Pd"), F.lit(0)))
        .withColumn("Nmbr_Bnfts_Pd2", F.coalesce(F.col("Nmbr_Bnfts_Pd2"), F.lit(0))))

    # ── 7. Corrections âge/sexe manquants + plafonds ───────────────────
    r = (r
        .withColumn("Gndr", F.when(F.col("Gndr").isin("", "X"), F.lit("M")).otherwise(F.col("Gndr")))
        .withColumn("Age_surv",
            F.when(F.col("Age_surv").isNull() | (F.col("Age_surv") <= 0), F.col("AVG_AGE"))
             .otherwise(F.col("Age_surv")))
        .withColumn("Nmbr_Mnths_Pndng", F.when(F.col("Nmbr_Mnths_Pndng") > 12, F.lit(12)).otherwise(F.col("Nmbr_Mnths_Pndng")))
        .withColumn("Mnths_lag", F.when(F.col("Mnths_lag") > 12, F.lit(12)).otherwise(F.col("Mnths_lag")))
        .withColumn("Nmbr_Bnfts_Pd", F.when(F.col("Nmbr_Bnfts_Pd") > 60, F.lit(60)).otherwise(F.col("Nmbr_Bnfts_Pd")))
        .withColumn("Age_surv",
            F.when((F.col("cover") == "IU") & (F.col("Age_surv") > 80), F.lit(70))
             .when(F.col("cover").isin("DIS", "LIFE", "CI", "PTD", "GAP") & (F.col("Age_surv") > 100), F.lit(100))
             .otherwise(F.col("Age_surv"))))

    # ── 8. Import tables de facteurs + jointures ───────────────────────
    import_01 = f"{base_dir}/Reserving tables/{cover_typ}/Tables_{pays}_{cover_typ}.xlsx"
    r = _joindre_facteurs(r, import_01, cover_typ)

    # ── 9. Calcul des réserves (formules de régression) ────────────────
    r = _calcul_reserves(r, cover_typ)

    # renommage Nmbr_Mnths_Pndng2 → Nmbr_Mnths_Pndng, etc.
    r = (r.drop("Nmbr_Mnths_Pndng", "Nmbr_Bnfts_Pd")
          .withColumnRenamed("Nmbr_Mnths_Pndng2", "Nmbr_Mnths_Pndng")
          .withColumnRenamed("Nmbr_Bnfts_Pd2", "Nmbr_Bnfts_Pd"))

    r.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}._CLMHDR_{cover_typ}_{pays}")


def _joindre_facteurs(r, fichier, cover_typ):
    """Importe et joint les facteurs de régression (duration + acceptation)."""
    def cast_factors(df):
        for c in df.columns:
            if c.startswith(("factor_", "Intercept_")):
                df = df.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast("double"))
        return df

    if cover_typ in COVER_DURATION:
        dur = _sheet(fichier, "Duration")
        age_D    = cast_factors(dur.select("Age_surv", "factor_age_D").dropDuplicates())
        gender_D = cast_factors(dur.select("Gndr", "factor_sexe_D", "Intercept_D").dropDuplicates())
        number_D = cast_factors(dur.select("Nmbr_Bnfts_Pd", "factor_numbr_D").dropDuplicates())
        lag_D    = cast_factors(dur.select("Mnths_lag", "factor_lag_D").dropDuplicates())
        max_D    = cast_factors(dur.select("Max_Nmbr_Bnfts", "factor_max_D").dropDuplicates())
        r = (r.join(age_D, ["Age_surv"], "left")
              .join(number_D, ["Nmbr_Bnfts_Pd"], "left")
              .join(max_D, ["Max_Nmbr_Bnfts"], "left")
              .join(gender_D, ["Gndr"], "left")
              .join(lag_D, ["Mnths_lag"], "left"))
    else:
        for c in ["factor_age_D", "factor_numbr_D", "factor_max_D",
                  "factor_sexe_D", "Intercept_D", "factor_lag_D"]:
            r = r.withColumn(c, F.lit(0.0))

    acc = _sheet(fichier, "Acceptation")
    age_A    = cast_factors(acc.select("Age_surv", "factor_age_A").dropDuplicates())
    gender_A = cast_factors(acc.select("Gndr", "factor_sexe_A", "Intercept_A").dropDuplicates())
    waiting_A = cast_factors(acc.select("Nmbr_Mnths_Pndng", "factor_month_A").dropDuplicates())
    r = (r.join(age_A, ["Age_surv"], "left")
          .join(gender_A, ["Gndr"], "left")
          .join(waiting_A, ["Nmbr_Mnths_Pndng"], "left"))
    return r


def _calcul_reserves(df, cover_typ):
    """Applique les formules de régression exponentielle → Rsrv_Amt."""
    lin_D = (F.col("Intercept_D") + F.col("factor_age_D") + F.col("factor_sexe_D")
             + F.col("factor_max_D") + F.col("factor_numbr_D") + F.col("factor_lag_D"))
    lin_A = (F.col("Intercept_A") + F.col("factor_age_A")
             + F.col("factor_sexe_A") + F.col("factor_month_A"))
    prob_A = F.exp(lin_A) / (1 + F.exp(lin_A))

    if cover_typ in COVER_DURATION:
        df = df.withColumn("Nmbr_Bnfts_Otstndng",
            F.when(F.col("Rsrv_Typ").isin("ICOP", "RBNP"), F.exp(lin_D))
             .otherwise(F.lit(0.0)))
        df = df.withColumn("Probablty_Otstndng",
            F.when(F.col("Rsrv_Typ") == "RBNP", prob_A).otherwise(F.lit(0.0)))
        df = df.withColumn("Rsrv_Amt",
            F.when((F.col("Rsrv_Typ") == "ICOP") & F.col("cover").isin("IU", "DIS"),
                   F.col("Mnthly_Bnft") * F.col("Nmbr_Bnfts_Otstndng"))
             .when((F.col("Rsrv_Typ") == "RBNP") & F.col("cover").isin("IU", "DIS"),
                   F.col("Probablty_Otstndng") * F.col("Mnthly_Bnft") * F.col("Nmbr_Bnfts_Otstndng"))
             .otherwise(F.lit(0.0)))
    else:
        df = df.withColumn("Nmbr_Bnfts_Otstndng", F.lit(0.0))
        df = df.withColumn("Probablty_Otstndng",
            F.when(F.col("Rsrv_Typ") == "RBNP", prob_A).otherwise(F.lit(0.0)))
        df = df.withColumn("Rsrv_Amt",
            F.when((F.col("Rsrv_Typ") == "RBNP") & F.col("cover").isin("CI", "LIFE", "PTD"),
                   F.col("Probablty_Otstndng") * F.col("Otstndng_Balnc"))
             .when((F.col("Rsrv_Typ") == "RBNP") & (F.col("cover") == "GAP"),
                   F.col("Probablty_Otstndng") * F.col("potential_clm_amt"))
             .otherwise(F.lit(0.0)))
        df = df.withColumn("Rsrv_Amt",
            F.when(F.col("Rsrv_Grp").isin("ZZ1", "ZZ2"), F.lit(0.0)).otherwise(F.col("Rsrv_Amt")))
    return df


# ═══════════════════════════════════════════════════════════════════════
# MACRO FUSION : consolidation des couvertures + réassurance
# ═══════════════════════════════════════════════════════════════════════
def fusion(pays):
    base_dir = BASE.format(arrete=arrete)

    # ── 1. Couvertures NON-CORE (ZZ1/ZZ2) hors case reserves ───────────
    hors = (spark.table(f"{pays}_CLMHDR_ALL")
        .filter(F.col("Rsrv_Grp").isin("ZZ1", "ZZ2"))
        .withColumn("Date_of_reserving", F.expr(DATE_RESERVING))
        .withColumn("Rsrv_Typ",
            F.when(F.col("STATUS").isin("OP", "RO"), F.lit("NON-CORE OPEN"))
             .when(F.col("STATUS").isin("CL", "DC"), F.lit("NON-CORE CLOSE")))
        .withColumn("Nmbr_Mnths_Pndng", F.lit(0))
        .withColumn("Nmbr_Bnfts_Pd", F.lit(0))
        .withColumn("Nmbr_Bnfts_Otstndng", F.lit(0.0))
        .withColumn("Probablty_Otstndng", F.lit(0.0))
        .withColumn("Rsrv_Amt", F.lit(0.0)))

    # ── 2. Fusion des 6 couvertures + hors-périmètre ───────────────────
    covers = ["IU", "DIS", "LIFE", "PTD", "CI", "GAP"]
    tables = [f"{OUTPUT_SCHEMA}._CLMHDR_{c}_{pays}" for c in covers]
    tables = [t for t in tables if _exists(t)]
    parts = [spark.table(t) for t in tables] + [hors]
    clmhdr_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)

    # dédup (proc sort nodupkey)
    clmhdr_all = clmhdr_all.dropDuplicates(
        ["Clm_Nmbr", "Policy_Line_No", "Policy_Line_Seq_No", "Schm", "Cvr_Typ"])
    clmhdr_all.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.CLMHDR_all_{pays}")
    clmhdr_all.createOrReplaceTempView(f"CLMHDR_all_{pays}_CR")

    # ── 3. Réassurance : jointure Parametres_Reas + Rsrv_Amt_Net ───────
    import_02 = f"{base_dir}/Model Properties/Mapping Cover Initial.xlsx"
    params_reas = (_sheet(import_02, "Parametres_Reas")
        .withColumn("Original_underwritter",
            F.regexp_replace(F.col("Original_underwritter").cast("string"), " ", ""))
        .withColumn("QP_rei_CLAIM", F.regexp_replace(F.col("QP_rei_CLAIM"), ",", ".").cast("double")))

    cr = (spark.table(f"{OUTPUT_SCHEMA}.CLMHDR_all_{pays}").alias("t1")
        .join(params_reas.select(
                F.col("country").alias("r_country"), F.col("scheme").alias("r_scheme"),
                F.col("cover").alias("r_cover"),
                F.col("Original_underwritter").alias("r_uw"), "QP_rei_CLAIM").alias("t8"),
              (F.col("t1.country") == F.col("t8.r_country")) &
              (F.col("t1.scheme") == F.col("t8.r_scheme")) &
              (F.col("t1.cover") == F.col("t8.r_cover")) &
              (F.col("t1.Entity_CD") == F.col("t8.r_uw")),
              "left")
        .drop("r_country", "r_scheme", "r_cover", "r_uw"))

    # Type_Insurance + neutralisation ZZ1/ZZ2 + renommages
    cr = (cr
        .withColumn("Type_Insurance",
            F.when(F.col("QP_rei_CLAIM").isNull(), F.lit(0)).otherwise(F.lit(4)))
        .withColumn("Rsrv_Amt",
            F.when(F.col("Rsrv_Grp").isin("ZZ1", "ZZ2"), F.lit(0.0)).otherwise(F.col("Rsrv_Amt")))
        .withColumnRenamed("Rsrv_Amt", "Rsrv_Amt_Gross")
        .withColumnRenamed("Totl_Bnfts_Amnt_Pd", "Totl_Bnfts_Amnt_Pd_Gross")
        .withColumnRenamed("Totl_Amnt_Pd", "Totl_Amnt_Pd_Gross")
        # Type_Insurance=0 → QP=1 (pas de cession)
        .withColumn("QP_rei_CLAIM",
            F.when(F.col("Type_Insurance") == 0, F.lit(1.0)).otherwise(F.col("QP_rei_CLAIM"))))

    # Montants nets = brut × QP de cession
    cr = (cr
        .withColumn("Rsrv_Amt_Net", F.col("Rsrv_Amt_Gross") * F.col("QP_rei_CLAIM"))
        .withColumn("Totl_Amnt_Pd_Net", F.col("Totl_Amnt_Pd_Gross") * F.col("QP_rei_CLAIM"))
        .withColumn("Totl_Bnfts_Amnt_Pd_Net", F.col("Totl_Bnfts_Amnt_Pd_Gross") * F.col("QP_rei_CLAIM")))

    cr.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.CLMHDR_all_{pays}_CR")


def _exists(t):
    try:
        spark.table(t); return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION : ACR × 6 couvertures + fusion, pour chaque pays
# ═══════════════════════════════════════════════════════════════════════
COVERS = ["IU", "DIS", "LIFE", "PTD", "CI", "GAP"]
PAYS = ["DE", "DK", "CH", "NO", "FI", "SE", "ES", "PL", "UK", "IT",
        "PT", "IE", "GR"]

for pays in PAYS:
    print(f"═══ RESERVING {pays} ═══")
    for cover_typ in COVERS:
        try:
            acr(cover_typ, pays)
        except Exception as e:
            print(f"  ⚠ ACR {cover_typ} {pays} : {e}")
    try:
        fusion(pays)
    except Exception as e:
        print(f"  ⚠ FUSION {pays} : {e}")

# ── Traitement spécial GRÈCE : réserves forcées à zéro ─────────────────
try:
    gr = (spark.table(f"{OUTPUT_SCHEMA}.CLMHDR_all_GR_CR")
        .withColumn("Rsrv_Amt_Gross", F.lit(0.0))
        .withColumn("Rsrv_Amt_Net", F.lit(0.0)))
    gr.write.mode("overwrite").saveAsTable(f"{OUTPUT_SCHEMA}.CLMHDR_all_GR_CR")
except Exception as e:
    print(f"  ⚠ GR spécial : {e}")

print("Module de reserving terminé.")
