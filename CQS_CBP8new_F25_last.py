from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ── Databricks Unity Catalog configuration ───────────────────────────
_catalog = 'your_catalog'  # TODO: replace with your Unity Catalog name

# Programme de calcul des GEPs concernant les CQS en règle 78
arrete = "2025_09_Q4"
n = 2025
lreseau = "X"
sdb = "Updated SDB Data Files 03.09.2025"
cqs_out_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_catalog}.cqs_out')
cqs_base_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP"
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_catalog}.cqs_base')
cqs_hy25_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2025_04_V2/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {_catalog}.cqs_hy25')

# A compléter par le User
table_gwp = "CBP_ITALY_POLICIES_CLAIMS"
table_claims = "CBP_ITALY_POLICIES_CLAIMS"
year_cut = 2025
variable_cut = "Date_Dbt_Assce"
date_val = datetime.date(2025, 12, 31)
import_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/GEP Stop Loss.xlsx"
export_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/Output_GEP_Com_new.xlsx"


def import_excelx(datafile, out, onglet):
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', f'{onglet}!A1')
        .option('header', 'true')
        .load(datafile))
    _df_tmp.createOrReplaceTempView(out)


def export_excelx(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel') \
        .option('dataAddress', f'{sheet}!A1') \
        .option('header', 'true').mode('overwrite').save(database)


def export_excel(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel') \
        .option('dataAddress', f'{sheet}!A1') \
        .option('header', 'true').mode('overwrite').save(database)


# Import Stop Loss
import_excelx(datafile=import_xx, out="GSL", onglet="Feuil2")


def prepa_table(table_gwp, table_claims):
    Table_CQS_MeF_1 = spark.table(f'{table_gwp}')
    Table_CQS_MeF_1 = (Table_CQS_MeF_1
        .withColumn('date_dbt_assce2', F.expr("""to_date(date_dbt_assce)"""))
        .withColumn('date_dbt_trait2', F.expr("""to_date(date_dbt_trait)"""))
        .withColumn('date_decla2', F.expr("""to_date(date_decla)"""))
        .withColumn('date_embauche2', F.expr("""to_date(date_embauche)"""))
        .withColumn('date_fin_assce2', F.expr("""to_date(date_fin_assce)"""))
        .withColumn('date_liquid2', F.expr("""to_date(date_liquid)"""))
        .withColumn('date_naiss_assre2', F.expr("""to_date(date_naiss_assre)"""))
        .withColumn('date_rachat2', F.expr("""to_date(date_rachat)"""))
        .withColumn('date_recep2', F.expr("""to_date(date_recep)"""))
        .withColumn('date_refus_22', F.expr("""to_date(date_refus_2)"""))
        .withColumn('date_resultat2', F.expr("""to_date(date_resultat)"""))
        .withColumn('date_signature2', F.expr("""to_date(date_signature)"""))
        .withColumn('date_sin2', F.expr("""to_date(date_sin)"""))
    )
    Table_CQS_MeF_1 = Table_CQS_MeF_1.drop('date_dbt_assce', 'date_dbt_trait', 'date_decla', 'date_embauche', 'date_fin_assce', 'date_liquid', 'date_naiss_assre', 'date_rachat', 'date_recep', 'date_refus_2', 'date_resultat', 'date_signature', 'date_sin')
    Table_CQS_MeF_1.createOrReplaceTempView('Table_CQS_MeF_1')

    Table_CQS_MeF_1b = spark.table('Table_CQS_MeF_1')
    Table_CQS_MeF_1b = (Table_CQS_MeF_1b
        .withColumn('date_dbt_assce', F.col('date_dbt_assce2'))
        .withColumn('date_dbt_trait', F.col('date_dbt_trait2'))
        .withColumn('date_decla', F.col('date_decla2'))
        .withColumn('date_embauche', F.col('date_embauche2'))
        .withColumn('date_fin_assce', F.col('date_fin_assce2'))
        .withColumn('date_liquid', F.col('date_liquid2'))
        .withColumn('date_naiss_assre', F.col('date_naiss_assre2'))
        .withColumn('date_rachat', F.col('date_rachat2'))
        .withColumn('date_recep', F.col('date_recep2'))
        .withColumn('date_refus_2', F.col('date_refus_22'))
        .withColumn('date_resultat', F.col('date_resultat2'))
        .withColumn('date_signature', F.col('date_signature2'))
        .withColumn('date_sin', F.col('date_sin2'))
    )
    Table_CQS_MeF_1b = Table_CQS_MeF_1b.drop('date_dbt_assce2', 'date_dbt_trait2', 'date_decla2', 'date_embauche2', 'date_fin_assce2', 'date_liquid2', 'date_naiss_assre2', 'date_rachat2', 'date_recep2', 'date_refus_22', 'date_resultat2', 'date_signature2', 'date_sin2')
    Table_CQS_MeF_1b.createOrReplaceTempView('Table_CQS_MeF_1b')

    Table_CQS_MeF_2 = spark.table('Table_CQS_MeF_1b')
    Table_CQS_MeF_2 = Table_CQS_MeF_2.filter(F.expr(f"""year({variable_cut})<{year_cut} AND FichOrigAdh != 'ST'"""))
    Table_CQS_MeF_2 = (Table_CQS_MeF_2
        .withColumn('Primes_VIE', F.col('Prime_Vie_Brute'))
        .withColumn('Primes_IARD', F.col('Prime_Non_vie_nette'))
        .withColumn('Rachat_VIE', F.col('Prime_remboursee_vie'))
        .withColumn('Rachat_IARD', F.col('Prime_remboursee_non_vie'))
        .withColumn('Year_Rac', F.expr("""year(date_rachat)"""))
    )
    Table_CQS_MeF_2.createOrReplaceTempView('Table_CQS_MeF_2')

    Etat_Recap_CQS = spark.sql("""select
    Generation, Year_Rac, Type_pret,
    sum(Primes_IARD) as Primes_IARD,
    sum(Rachat_Iard) as Rachat_Iard,
    sum(Primes_VIE) as Primes_VIE,
    sum(Rachat_VIE) as Rachat_VIE
    from Table_CQS_MeF_2
    group by Generation,Year_Rac,type_pret""")
    Etat_Recap_CQS.createOrReplaceTempView('Etat_Recap_CQS')


prepa_table(table_gwp=table_gwp, table_claims=table_claims)


def uep_cqs_78(gar, gar2):
    _dfs[f'CQS_PPNA_{gar}'] = spark.table('Table_CQS_MeF_2')
    _dfs[f'CQS_PPNA_{gar}'] = _dfs[f'CQS_PPNA_{gar}'].filter(F.expr(f"""Primes_{gar} IS NOT NULL"""))
    _dfs[f'CQS_PPNA_{gar}'] = _dfs[f'CQS_PPNA_{gar}'].select(
        'ID_Adh', 'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'generation',
        'Date_Dbt_Assce', 'Date_Fin_Assce', 'date_rachat', 'date_sin',
        f'Primes_{gar}', f'Rachat_{gar}', 'type_pret')
    _dfs[f'CQS_PPNA_{gar}'].createOrReplaceTempView(f'CQS_PPNA_{gar}')

    # AJOUT AU FY25 : POUR EVITER QUE LES RACHATS SOIENT COMPTEES DEUX FOIS
    _dfs[f'CQS_PPNA_{gar}'] = spark.table(f'CQS_PPNA_{gar}')
    _dfs[f'CQS_PPNA_{gar}'] = (_dfs[f'CQS_PPNA_{gar}']
        .withColumn('date_rachat2', F.when(F.expr("""year(date_rachat) != 2025"""), F.col('date_rachat')))
    )
    _dfs[f'CQS_PPNA_{gar}'].createOrReplaceTempView(f'CQS_PPNA_{gar}')

    _dfs[f'CQS_PPNA_{gar}_2'] = spark.table(f'CQS_PPNA_{gar}')
    _dfs[f'CQS_PPNA_{gar}_2'] = (_dfs[f'CQS_PPNA_{gar}_2']
        .withColumn('Date_term', F.expr(f"""least(Date_Fin_Assce,{date_val},date_rachat2,date_sin)"""))
        .withColumn('term', F.expr("""month(Date_Fin_Assce)+12*year(Date_Fin_Assce)-(month(Date_Dbt_Assce)+12*year(Date_Dbt_Assce))+1"""))
        .withColumn('term_expoff', F.expr("""month(Date_term)+12*year(Date_term)-(month(Date_Dbt_Assce)+12*year(Date_Dbt_Assce))+1"""))
        .withColumn('Mois_fin_annee', F.expr("""12-month(least(Date_Dbt_Assce,Date_term))+1"""))
        .withColumn('Quotient_res', F.expr("""floor((term_expoff - Mois_fin_annee)/12)"""))
        .withColumn('mois_rest', F.expr("""(term_expoff - Mois_fin_annee)-Quotient_res*12"""))
    )
    _dfs[f'CQS_PPNA_{gar}_2'].createOrReplaceTempView(f'CQS_PPNA_{gar}_2')

    # ====================================================================
    # ACTUARIAL CORE — hand-translated from the %do i=1 %to 15 loops.
    # ====================================================================
    df = spark.table(f'CQS_PPNA_{gar}_2')

    # UEP year 0
    df = df.withColumn(f'UEP_{gar}0',
        F.when(F.expr("term_expoff > Mois_fin_annee"),
               F.expr(f"(term-Mois_fin_annee)*(term-Mois_fin_annee+1)/(term*(term+1))*Primes_{gar}"))
         .when((F.year('date_sin') <= F.col('Generation')) & (F.col('Date_term') == F.col('date_sin')), F.lit(0))
         .when((F.year('date_rachat2') <= F.col('Generation')) & (F.col('Date_term') == F.col('date_rachat2')), F.lit(0))
         .otherwise(F.expr(f"(term-term_expoff)*(term-term_expoff+1)/(term*(term+1))*Primes_{gar}")))

    # UEP years 1..15 (single select)
    uep_cols = []
    for i in range(1, 16):
        uep_cols.append(
            F.when((F.year('date_sin') <= i + F.col('Generation')) & (F.col('Date_term') == F.col('date_sin')), F.lit(0))
            .when((F.year('date_rachat2') <= i + F.col('Generation')) & (F.col('Date_term') == F.col('date_rachat2')), F.lit(0))
            .when((F.col('term_expoff') - F.col('Mois_fin_annee')) > i * 12,
                  F.expr(f"(term - Mois_fin_annee - {i}*12)*(term - Mois_fin_annee - {i}*12 + 1)/((term+1)*term)*Primes_{gar}"))
            .otherwise(F.expr(f"(term-term_expoff)*(term-term_expoff+1)/(term*(term+1))*Primes_{gar}"))
            .alias(f'UEP_{gar}{i}'))
    df = df.select('*', *uep_cols)

    # GEP and UEP_PL (year 0 + 1..15) in one select
    gep_pl_cols = [
        (F.col(f'Primes_{gar}') - F.col(f'UEP_{gar}0')).alias(f'GEP_{gar}0'),
        F.col(f'UEP_{gar}0').alias(f'UEP_PL_{gar}0'),
    ]
    for i in range(1, 16):
        j = i - 1
        gep_pl_cols.append((F.col(f'UEP_{gar}{j}') - F.col(f'UEP_{gar}{i}')).alias(f'GEP_{gar}{i}'))
        gep_pl_cols.append((F.col(f'UEP_{gar}{i}') - F.col(f'UEP_{gar}{j}')).alias(f'UEP_PL_{gar}{i}'))
    df = df.select('*', *gep_pl_cols)

    _dfs[f'CQS_PPNA_{gar}_2'] = df
    _dfs[f'CQS_PPNA_{gar}_2'].createOrReplaceTempView(f'CQS_PPNA_{gar}_2')
    # ====================================================================

    _dfs[f'CQS_UEP_PL__{gar}'] = spark.table(f'CQS_PPNA_{gar}_2')
    _dfs[f'CQS_UEP_PL__{gar}'] = _dfs[f'CQS_UEP_PL__{gar}'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'date_rachat',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret',
        *[f'UEP_PL_{gar}{k}' for k in range(16)])
    _dfs[f'CQS_UEP_PL__{gar}'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}')

    _dfs[f'CQS_GEP_{gar}'] = spark.table(f'CQS_PPNA_{gar}_2')
    _dfs[f'CQS_GEP_{gar}'] = _dfs[f'CQS_GEP_{gar}'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'date_rachat',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret',
        *[f'GEP_{gar}{k}' for k in range(16)])
    _dfs[f'CQS_GEP_{gar}'].createOrReplaceTempView(f'CQS_GEP_{gar}')

    # PROC TRANSPOSE — GEP wide-to-long
    _gep_stack = ', '.join(f"'GEP_{gar}{k}', `GEP_{gar}{k}`" for k in range(16))
    _dfs[f'CQS_GEP_{gar}_2'] = _dfs[f'CQS_GEP_{gar}'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'date_rachat', 'Date_Rachat2',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret',
        F.expr(f"""stack(16, {_gep_stack}) as (variable, GEP)"""))
    _dfs[f'CQS_GEP_{gar}_2'].createOrReplaceTempView(f'CQS_GEP_{gar}_2')

    # PROC TRANSPOSE — UEP_PL wide-to-long
    _uep_stack = ', '.join(f"'UEP_PL_{gar}{k}', `UEP_PL_{gar}{k}`" for k in range(16))
    _dfs[f'CQS_UEP_PL__{gar}_2'] = _dfs[f'CQS_UEP_PL__{gar}'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'date_rachat', 'Date_Rachat2',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret',
        F.expr(f"""stack(16, {_uep_stack}) as (variable, UEP_PL)"""))
    _dfs[f'CQS_UEP_PL__{gar}_2'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_2')

    _dfs[f'CQS_UEP_PL__{gar}_3'] = spark.table(f'CQS_UEP_PL__{gar}_2')
    _dfs[f'CQS_UEP_PL__{gar}_3'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_3')

    _dfs[f'CQS_GEP_{gar}_3'] = spark.table(f'CQS_GEP_{gar}_2')
    _dfs[f'CQS_GEP_{gar}_3'].createOrReplaceTempView(f'CQS_GEP_{gar}_3')

    # SURV = Generation + suffix of the variable name
    _dfs[f'CQS_GEP_{gar}_4'] = spark.table(f'CQS_GEP_{gar}_3')
    _dfs[f'CQS_GEP_{gar}_4'] = _dfs[f'CQS_GEP_{gar}_4'].withColumn(
        'SURV',
        F.col('Generation') + F.regexp_extract(F.col('variable'), r'(\d+)$', 1).cast('int'))
    _dfs[f'CQS_GEP_{gar}_4'].createOrReplaceTempView(f'CQS_GEP_{gar}_4')

    _dfs[f'CQS_UEP_PL__{gar}_4'] = spark.table(f'CQS_UEP_PL__{gar}_3')
    _dfs[f'CQS_UEP_PL__{gar}_4'] = _dfs[f'CQS_UEP_PL__{gar}_4'].withColumn(
        'SURV',
        F.col('Generation') + F.regexp_extract(F.col('variable'), r'(\d+)$', 1).cast('int'))
    _dfs[f'CQS_UEP_PL__{gar}_4'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_4')

    _dfs[f'CQS_GEP_{gar}_5'] = spark.table(f'CQS_GEP_{gar}_4')
    _dfs[f'CQS_GEP_{gar}_5'] = (_dfs[f'CQS_GEP_{gar}_5']
        .withColumn('Rachat', F.when(F.expr("""year(Date_Rachat2)=SURV"""), F.expr(f"""-Rachat_{gar}""")))
        .withColumn('GWP', F.when(F.expr("""year(Date_dbt_Assce)=SURV"""), F.expr(f"""Primes_{gar}""")))
        .withColumn('GAR', F.when(F.lit(gar2 == 'IARD'), F.lit(30)).otherwise(F.lit(10)))
        .withColumn('IDEAN', F.when(F.lit(gar2 == 'IARD'),
                    F.expr("""cast(concat('1', substr(ID_Police,6,4)) as long)"""))
                  .otherwise(F.expr("""cast(concat('1', substr(ID_Police,1,4)) as long)""")))
        .withColumn(f'Rachat_{gar}', F.coalesce(F.col(f'Rachat_{gar}'), F.lit(0)))
    )
    _dfs[f'CQS_GEP_{gar}_5'] = _dfs[f'CQS_GEP_{gar}_5'].filter(~F.expr(f"""SURV>year({date_val})"""))
    _dfs[f'CQS_GEP_{gar}_5'] = _dfs[f'CQS_GEP_{gar}_5'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'date_rachat',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'SURV', 'GAR', 'GEP',
        'GWP', 'Rachat', 'IDEAN', 'type_pret')
    _dfs[f'CQS_GEP_{gar}_5'].createOrReplaceTempView(f'CQS_GEP_{gar}_5')

    _dfs[f'CQS_UEP_PL__{gar}_5'] = spark.table(f'CQS_UEP_PL__{gar}_4')
    _dfs[f'CQS_UEP_PL__{gar}_5'] = (_dfs[f'CQS_UEP_PL__{gar}_5']
        .withColumn('GAR', F.when(F.lit(gar2 == 'IARD'), F.lit(30)).otherwise(F.lit(10)))
        .withColumn('IDEAN', F.when(F.lit(gar2 == 'IARD'),
                    F.expr("""cast(concat('1', substr(ID_Police,6,4)) as long)"""))
                  .otherwise(F.expr("""cast(concat('1', substr(ID_Police,1,4)) as long)""")))
    )
    _dfs[f'CQS_UEP_PL__{gar}_5'] = _dfs[f'CQS_UEP_PL__{gar}_5'].filter(~F.expr(f"""SURV>year({date_val})"""))
    _dfs[f'CQS_UEP_PL__{gar}_5'] = _dfs[f'CQS_UEP_PL__{gar}_5'].select(
        'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce',
        'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'date_rachat',
        f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff',
        'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'SURV', 'GAR', 'UEP_PL',
        'IDEAN', 'type_pret')
    _dfs[f'CQS_UEP_PL__{gar}_5'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_5')


uep_cqs_78(gar="IARD", gar2="IARD")
uep_cqs_78(gar="VIE", gar2="VIE")


def creer_table(gar):
    _dfs[f'CQS_UEP_PL__{gar}_6'] = spark.sql(f"""select distinct Financiere_Adh, ID_Police, sit_prof_1, IDEAN, Generation, GAR, SURV, sum(UEP_PL) as UEP_PL, type_pret
    from CQS_UEP_PL__{gar}_5
    group by Financiere_Adh, ID_Police, sit_prof_1, IDEAN, Generation, GAR, SURV, type_pret""")
    _dfs[f'CQS_UEP_PL__{gar}_6'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_6')

    _dfs[f'CQS_GEP_{gar}_6'] = spark.sql(f"""select distinct Financiere_Adh, ID_Police, sit_prof_1, IDEAN, Generation, GAR, SURV, sum(GEP) as GEP, sum(GWP) as GWP, sum(Rachat) as Rachat, type_pret
    from CQS_GEP_{gar}_5
    group by Financiere_Adh, ID_Police, sit_prof_1, IDEAN, Generation, GAR, SURV, type_pret""")
    _dfs[f'CQS_GEP_{gar}_6'].createOrReplaceTempView(f'CQS_GEP_{gar}_6')


creer_table(gar="IARD")
creer_table(gar="VIE")

CQS_GEP = spark.table('CQS_GEP_IARD_6').union(spark.table('CQS_GEP_VIE_6'))
CQS_GEP = CQS_GEP.withColumn('idean2', F.col('idean').cast('string'))
CQS_GEP = CQS_GEP.drop('idean').withColumnRenamed('idean2', 'idean')
CQS_GEP.createOrReplaceTempView('CQS_GEP')
CQS_GEP.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.CQS_GEP')

CQS_UEP_PL = spark.table('CQS_UEP_PL__IARD_6').union(spark.table('CQS_UEP_PL__VIE_6'))
CQS_UEP_PL = CQS_UEP_PL.withColumn('idean_char', F.col('idean').cast('string'))
CQS_UEP_PL = CQS_UEP_PL.drop('idean').withColumnRenamed('idean_char', 'idean')
CQS_UEP_PL.createOrReplaceTempView('CQS_UEP_PL')
CQS_UEP_PL.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.CQS_UEP_PL')

# ── COMMISSION ──────────────────────────────────────────────────────
chemin_imp = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/00 - Documents et parametres/20250918_Commission_Life_NoLife_2025.xlsx"
import_excelx(datafile=chemin_imp, out="Tx_COM_input", onglet="CQS Commissions")
Tx_COM_input_2 = spark.table('Tx_COM_input').filter(F.col('ID_Police').isNotNull())
Tx_COM_input_2.createOrReplaceTempView('Tx_COM_input_2')

Taux_COM_CQS_VIE_0 = spark.sql("""select Financiere_Adh, Generation, Sit_Prof_1, ID_Police, Comm_Rate_LIFE as Taux_Com
    from Tx_COM_input_2 group by Financiere_Adh, generation, Sit_Prof_1, ID_Police""")
Taux_COM_CQS_VIE_0.createOrReplaceTempView('Taux_COM_CQS_VIE_0')

Taux_COM_CQS_VIE = spark.table('Taux_COM_CQS_VIE_0')
Taux_COM_CQS_VIE = (Taux_COM_CQS_VIE
    .withColumn('GAR', F.lit(10))
    .withColumn('IDEAN', F.expr("""cast(concat('1', substring(ID_Police,1,4)) as long)"""))
)
Taux_COM_CQS_VIE.createOrReplaceTempView('Taux_COM_CQS_VIE')

Taux_COM_CQS_IARD_0 = spark.sql("""select Financiere_Adh, Generation, Sit_Prof_1, ID_Police, Comm_rate_NON_LIFE as Taux_Com
    from Tx_COM_input_2 group by Financiere_Adh, Generation, Sit_Prof_1, ID_Police""")
Taux_COM_CQS_IARD_0.createOrReplaceTempView('Taux_COM_CQS_IARD_0')

Taux_COM_CQS_IARD = spark.table('Taux_COM_CQS_IARD_0')
Taux_COM_CQS_IARD = (Taux_COM_CQS_IARD
    .withColumn('GAR', F.lit(30))
    .withColumn('IDEAN', F.expr("""cast(concat('1', substring(ID_Police,6,4)) as long)"""))
)
Taux_COM_CQS_IARD.createOrReplaceTempView('Taux_COM_CQS_IARD')

Taux_COM_CQS = spark.table('Taux_COM_CQS_VIE').union(spark.table('Taux_COM_CQS_IARD'))
Taux_COM_CQS.createOrReplaceTempView('Taux_COM_CQS')

Taux_COM = spark.table('Taux_COM_CQS')
Taux_COM = Taux_COM.withColumn('idean_char', F.col('idean').cast('string'))
Taux_COM = Taux_COM.filter(~F.expr("""IDEAN IS NULL"""))
Taux_COM = Taux_COM.drop('idean').withColumnRenamed('idean_char', 'idean')
Taux_COM.createOrReplaceTempView('Taux_COM')

CQS_GEP_2 = spark.sql("""select A.*, B.TAUX_COM
    from CQS_GEP A left join Taux_COM B
    on A.Financiere_Adh=B.Financiere_Adh and A.Generation=B.Generation
    and A.Sit_Prof_1=B.Sit_Prof_1 and A.ID_Police=B.ID_Police and A.GAR=B.GAR
    and regexp_replace(A.IDEAN, ' ', '')=regexp_replace(B.IDEAN, ' ', '')""")
CQS_GEP_2.createOrReplaceTempView('CQS_GEP_2')

CQS_GEP_2_bis = spark.table('CQS_GEP_2')
CQS_GEP_2_bis = (CQS_GEP_2_bis
    .withColumn('Taux_COM', F.coalesce(F.col('Taux_COM'), F.lit(0)))
    .withColumn('Rachat', F.coalesce(F.col('Rachat'), F.lit(0)))
    .withColumn('GWP', F.coalesce(F.col('GWP'), F.lit(0)))
    .withColumn('COM', F.expr("""GWP*Taux_COM"""))
    .withColumn('COM_Rachat', F.expr("""Rachat*Taux_COM"""))
    .withColumn('COM_NetLapse', F.col('COM') + F.col('COM_Rachat'))
    .withColumn('GWP_NetLapse', F.col('GWP') + F.col('Rachat'))
)
CQS_GEP_2_bis.createOrReplaceTempView('CQS_GEP_2_bis')

CQS_UEP_PL_2 = spark.sql("""select A.*, B.TAUX_COM
    from CQS_UEP_PL A left join Taux_COM B
    on A.Financiere_Adh=B.Financiere_Adh and A.Generation=B.Generation
    and A.Sit_Prof_1=B.Sit_Prof_1 and A.ID_Police=B.ID_Police and A.GAR=B.GAR and A.IDEAN=B.IDEAN""")
CQS_UEP_PL_2.createOrReplaceTempView('CQS_UEP_PL_2')

CQS_UEP_PL_2_bis = spark.table('CQS_UEP_PL_2')
CQS_UEP_PL_2_bis = (CQS_UEP_PL_2_bis
    .withColumn('Taux_COM', F.coalesce(F.col('Taux_COM'), F.lit(0)))
    .withColumn('DAC', F.expr("""UEP_PL*Taux_COM"""))
)
CQS_UEP_PL_2_bis.createOrReplaceTempView('CQS_UEP_PL_2_bis')

CQS_UEP_PL_3 = spark.sql("""select IDEAN, GAR, type_pret, Surv, generation as gen,
    sum(UEP_PL) as UEP, sum(DAC) as DAC
    from CQS_UEP_PL_2_bis group by IDEAN, GAR, SURV, Gen, type_pret""")
CQS_UEP_PL_3.createOrReplaceTempView('CQS_UEP_PL_3')

CQS_UEP_GEN_SURV = spark.sql("""select IDEAN, GAR, Generation as GEN, SURV,
    sum(UEP_PL) as UEP, sum(DAC) as DAC
    from CQS_UEP_PL_2_bis group by IDEAN, GAR, SURV, Gen, type_pret""")
CQS_UEP_GEN_SURV.createOrReplaceTempView('CQS_UEP_GEN_SURV')

CQS_GEP_3 = spark.sql(f"""select IDEAN, GAR, type_pret, SURV, generation,
    sum(GWP_NetLapse) as PRIMES, sum(COM_NetLapse) as COM
    from CQS_GEP_2_bis where surv <> {n}
    group by IDEAN, GAR, SURV, type_pret, generation""")
CQS_GEP_3.createOrReplaceTempView('CQS_GEP_3')

CQS_GEP_3 = spark.table('CQS_GEP_3').filter(~F.expr("""idean IS NULL""")).filter(~F.expr("""idean=1"""))
CQS_GEP_3.createOrReplaceTempView('CQS_GEP_3')
CQS_GEP_3.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.CQS_GEP_3')

CQS_UEP_PL_3 = spark.table('CQS_UEP_PL_3').filter(~F.expr("""idean IS NULL""")).filter(~F.expr("""idean=1"""))
CQS_UEP_PL_3.createOrReplaceTempView('CQS_UEP_PL_3')
CQS_UEP_PL_3.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.CQS_UEP_PL_3')

CQS_UEP_GEN_SURV = spark.table('CQS_UEP_GEN_SURV').filter(~F.expr("""idean IS NULL""")).filter(~F.expr("""idean=1"""))
CQS_UEP_GEN_SURV.createOrReplaceTempView('CQS_UEP_GEN_SURV')
CQS_UEP_GEN_SURV.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.CQS_UEP_GEN_SURV')

export_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/Output_CQS.xlsx"
export_excel(datatable=spark.table('cqs_out.cqs_gep_3'),     database=export_01, sheet="GEP")
export_excel(datatable=spark.table('cqs_out.cqs_gep_2_bis'), database=export_01, sheet="Détail")
export_excel(datatable=spark.table('cqs_out.cqs_uep_pl_3'),  database=export_01, sheet="UEP")

GEP = spark.table('cqs_gep_3')
GEP.createOrReplaceTempView('GEP')
UEP = spark.table('cqs_uep_pl_3')
UEP.createOrReplaceTempView('UEP')

# ── Garantie 10 ─────────────────────────────────────────────────────
GLS_10 = spark.sql("""select Annee, sum(Stop_Loss) as Stop_Loss from gsl Where GAR = 10 group by Annee""")
GLS_10.createOrReplaceTempView('GLS_10')
EP1 = spark.sql("""select SURV as annee, sum(UEP) as EP1, sum(DAC) as DAC from UEP Where Gar = 10 group by SURV""")
EP1.createOrReplaceTempView('EP1')
EP2 = spark.sql("""select SURV as annee, sum(PRIMES) as EP2, sum(com) as com from GEP Where Gar = 10 group by SURV""")
EP2.createOrReplaceTempView('EP2')
EP_10_ = spark.sql("""select A.annee, EP2-Ep1 AS Ep from EP1 a left join EP2 b on A.annee=B.annee""")
EP_10_.createOrReplaceTempView('EP_10_')

EP_10 = spark.table('EP_10_').join(spark.table('GLS_10'), ['annee'], 'left')
EP_10 = EP_10.withColumn('GEP', F.expr("""(coalesce(Ep, 0) + coalesce(Stop_Loss, 0))"""))
EP_10.createOrReplaceTempView('EP_10')
EP_10 = spark.sql("""select annee, '10' as GAR, EP, Stop_Loss, GEP from EP_10""")
EP_10.createOrReplaceTempView('EP_10')

COM_DAC_10 = spark.sql("""select A.annee, '10' as GAR, com-dac AS COMM_DAC from EP1 a left join EP2 b on A.annee=B.annee""")
COM_DAC_10.createOrReplaceTempView('COM_DAC_10')

# ── Garantie 30 ─────────────────────────────────────────────────────
GLS_30 = spark.sql("""select Annee, sum(Stop_Loss) as Stop_Loss from gsl Where GAR = 30 group by Annee""")
GLS_30.createOrReplaceTempView('GLS_30')
EP1 = spark.sql("""select SURV as annee, sum(UEP) as EP1, sum(DAC) as DAC from UEP Where Gar = 30 group by SURV""")
EP1.createOrReplaceTempView('EP1')
EP2 = spark.sql("""select SURV as annee, sum(PRIMES) as EP2, sum(com) as com from GEP Where Gar = 30 group by SURV""")
EP2.createOrReplaceTempView('EP2')
EP_30_ = spark.sql("""select A.annee, EP2-Ep1 AS Ep from EP1 a left join EP2 b on A.annee=B.annee""")
EP_30_.createOrReplaceTempView('EP_30_')

EP_30 = spark.table('EP_30_').join(spark.table('GLS_30'), ['annee'], 'left')
EP_30 = EP_30.withColumn('GEP', F.expr("""(coalesce(Ep, 0) + coalesce(Stop_Loss, 0))"""))
EP_30.createOrReplaceTempView('EP_30')
EP_30 = spark.sql("""select annee, '30' as GAR, EP, Stop_Loss, GEP from EP_30""")
EP_30.createOrReplaceTempView('EP_30')

COM_DAC_30 = spark.sql("""select A.annee, '30' as GAR, com-dac AS COMM_DAC from EP1 a left join EP2 b on A.annee=B.annee""")
COM_DAC_30.createOrReplaceTempView('COM_DAC_30')

EP = spark.table('EP_10').union(spark.table('EP_30'))
EP.createOrReplaceTempView('EP')
COM_DAC = spark.table('COM_DAC_10').union(spark.table('COM_DAC_30'))
COM_DAC.createOrReplaceTempView('COM_DAC')

# Exports (fixed from the mangled multi-line macro call)
export_excelx(datatable=EP,      database=export_xx, sheet="GEP")
export_excelx(datatable=COM_DAC, database=export_xx, sheet="COM")

# == Durations moyennes pour la base MP ==
duration_cqs_cbp = spark.table('CQS_GEP_IARD_5').union(spark.table('CQS_GEP_VIE_5'))
duration_cqs_cbp = duration_cqs_cbp.filter(~F.expr("""GEP = 0"""))
duration_cqs_cbp = duration_cqs_cbp.select('Financiere_Adh', 'date_dbt_assce', 'date_fin_assce', 'term', 'GAR', 'IDEAN', 'GEP')
duration_cqs_cbp.createOrReplaceTempView('duration_cqs_cbp')

duration_cqs_cbp_moy = duration_cqs_cbp.groupBy('IDEAN', 'GAR', 'Financiere_Adh').agg(F.sum('term').alias('term'))
duration_cqs_cbp_moy.createOrReplaceTempView('duration_cqs_cbp_moy')

duration_moy_cqs_cbp = spark.table('duration_cqs_cbp_moy')
duration_moy_cqs_cbp.createOrReplaceTempView('duration_moy_cqs_cbp')
duration_moy_cqs_cbp.write.mode('overwrite').saveAsTable(f'{_catalog}.cqs_out.duration_moy_cqs_cbp')
