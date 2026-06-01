from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ── Databricks Unity Catalog configuration ───────────────────────────
# Set your catalog name below. Each SAS LIBNAME becomes a schema.
# e.g. LIBNAME BGD_Q425 → schema 'bgd_q425' in catalog 'your_catalog'
_catalog = 'your_catalog'  # TODO: replace with your Unity Catalog name

# /
# Programme de calcul des GEPs concernant les CQS en règle 78
# avec acquisition de la PPNA lorsque Sinistre, et lorsque rachat.
# De plus, il sort les GWP - Rachat - Commissions - Rachat sur COM
# Dépendance avec le fichier d'Antoine cependant par Génération / Financiere
# /
# Répertoire de travail des CQSs CBP
arrete = "2025_09_Q4"
n = 2025
lreseau = "X"
sdb = "Updated SDB Data Files 03.09.2025"
cqs_out_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.cqs_out')  # Unity Catalog schema for CQS_Out
cqs_base_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP"  # LIBNAME CQS_Base
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.cqs_base')  # Unity Catalog schema for CQS_Base
cqs_hy25_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2025_04_V2/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_HY25
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.cqs_hy25')  # Unity Catalog schema for CQS_HY25
# /
# A compléter par le User
# /
table_gwp = "CBP_ITALY_POLICIES_CLAIMS"
table_claims = "CBP_ITALY_POLICIES_CLAIMS"
year_cut = 2025
# si besoin possibilité d'exclure des données à partir d'une année
variable_cut = "Date_Dbt_Assce"
# variable sur laquelle on souhaite apliquer le filtre
date_val = datetime.date(2025, 12, 31)
# date à laquelle on veut observer les UEP
import_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/GEP Stop Loss.xlsx"
export_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/Output_GEP_Com_new"
# Import et préparation de la base + Création d'une table de récap des racahts par génération et année de rachat si besoin
# proc sql ;
# create table Table_CQS_MeF as
# select
# A.*,
# B.date_sin,
# B.Montant_Sin as Montant_Sin
# from CQS_CBP.&table_GWP. A left join CQS_CBP.&table_Claims. B on A.ID_Fusion=B.ID_Assre_Pot ;
# quit ;
def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


def export_excelx(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


def export_excel(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


# Import Stop Loss
import_excelx(datafile=import_xx, out="GSL", onglet="Feuil2")
# import de la table CBP_ITALY_POLICIES_CLAIMS
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
    # Modification AJ V220 : changement de format dans les dates de la base CQS CBP
    # ATTENTION si le format des dates n'est pas en DATETIME29.9, ne pas exécuter ces deux étapes data
    # ATTRIB: attrib date_dbt_assce2    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_dbt_trait2    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_decla2 		  informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_embauche2     informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_fin_assce2    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_liquid2       informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_naiss_assre2  informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_rachat2       informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_recep2        informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_refus_22      informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_resultat2     informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_signature2    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_sin2          informat=ddmmyy10. format=ddmmyy10.
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
    # ATTRIB: attrib date_dbt_assce    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_dbt_trait    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_decla 		  informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_embauche     informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_fin_assce    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_liquid       informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_naiss_assre  informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_rachat       informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_recep        informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_refus_2      informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_resultat     informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_signature    informat=ddmmyy10. format=ddmmyy10.
    # ATTRIB: attrib date_sin          informat=ddmmyy10. format=ddmmyy10.
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
    Generation,
    Year_Rac, Type_pret,
    sum(Primes_IARD) as Primes_IARD,
    sum(Rachat_Iard) as Rachat_Iard,
    sum(Primes_VIE) as Primes_VIE,
    sum(Rachat_VIE) as Rachat_VIE
    from Table_CQS_MeF_2
    group by Generation,Year_Rac,type_pret """)
    Etat_Recap_CQS.createOrReplaceTempView('Etat_Recap_CQS')


prepa_table(table_gwp=table_gwp, table_claims=table_claims)
def uep_cqs_78(gar, gar2):
    _dfs[f'CQS_PPNA_{gar}'] = spark.table('Table_CQS_MeF_2')
    _dfs[f'CQS_PPNA_{gar}'] = _dfs[f'CQS_PPNA_{gar}'].filter(F.expr(f"""Primes_{gar} IS NOT NULL"""))
    # type_pret rajouté au HY24 pour la cession
    _dfs[f'CQS_PPNA_{gar}'] = _dfs[f'CQS_PPNA_{gar}'].select('ID_Adh', 'Financiere_Adh', 'ID_Police', 'sit_prof_1', 'generation', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'date_rachat', 'date_sin', f'Primes_{gar}', f'Rachat_{gar}', 'type_pret')
    _dfs[f'CQS_PPNA_{gar}'].createOrReplaceTempView(f'CQS_PPNA_{gar}')

    # AJOUT AU FY25 : POUR EVITER QUE LES RACHATS SOIENT COMPTEES DEUX FOIS
    _dfs[f'CQS_PPNA_{gar}'] = spark.table(f'CQS_PPNA_{gar}')
    _dfs[f'CQS_PPNA_{gar}'] = (_dfs[f'CQS_PPNA_{gar}']
        .withColumn('date_rachat2', F.when(F.expr("""year(date_rachat) != 2025"""), F.col('date_rachat')))  # no ELSE: null when condition is false
    )
    # FORMAT/INFORMAT: format date_rachat2 ddmmyy10.
    # ou utilisez un autre format comme date9. pour une date courte
    # drop date_rachat; rename date_rachat2=date_rachat;
    _dfs[f'CQS_PPNA_{gar}'].createOrReplaceTempView(f'CQS_PPNA_{gar}')

    # /
    _dfs[f'CQS_PPNA_{gar}_2'] = spark.table(f'CQS_PPNA_{gar}')
    _dfs[f'CQS_PPNA_{gar}_2'] = (_dfs[f'CQS_PPNA_{gar}_2']
        .withColumn('Date_term', F.expr(f"""least(Date_Fin_Assce,{date_val},date_rachat2,date_sin)"""))
        .withColumn('term', F.expr("""month(Date_Fin_Assce)+12*year(Date_Fin_Assce)-(month(Date_Dbt_Assce)+12*year(Date_Dbt_Assce))+1"""))
        .withColumn('term_expoff', F.expr("""month(Date_term)+12*year(Date_term)-(month(Date_Dbt_Assce)+12*year(Date_Dbt_Assce))+1"""))
        .withColumn('Mois_fin_annee', F.expr("""12-month(least(Date_Dbt_Assce,Date_term))+1"""))
        .withColumn('Quotient_res', F.expr("""floor((term_expoff - Mois_fin_annee)/12)"""))
        .withColumn('mois_rest', F.expr("""(term_expoff - Mois_fin_annee)-Quotient_res*12"""))
    )
    # ATTRIB: attrib Date_term format=DDMMYY10.
    _dfs[f'CQS_PPNA_{gar}_2'].createOrReplaceTempView(f'CQS_PPNA_{gar}_2')

    _dfs[f'CQS_PPNA_{gar}_2'] = spark.table(f'CQS_PPNA_{gar}_2')
    _dfs[f'CQS_PPNA_{gar}_2'] = (_dfs[f'CQS_PPNA_{gar}_2']
        .withColumn(f'UEP_{gar}0',
            F.when(F.expr("""term_expoff>Mois_fin_annee"""), F.expr(f"""(term-Mois_fin_annee)*(term-Mois_fin_annee+1)/(term*(term+1))*Primes_{gar}"""))
             .when(F.expr("""(year(date_sin)<= Generation AND Date_term=Date_sin)"""), F.lit(0))
             .when(F.expr("""(year(date_rachat2)<= Generation AND Date_term=date_rachat2)"""), F.lit(0))
             .otherwise(F.expr(f"""(term-term_expoff)*(term-term_expoff+1)/(term*(term+1))*Primes_{gar}""")))
        .withColumn(f'UEP_{gar}{i}',
            F.when(F.expr(f"""(year(date_sin)<= {i}+Generation AND Date_term=Date_sin)"""), F.lit(0))
             .when(F.expr(f"""(year(date_rachat2)<= {i}+Generation AND Date_term=date_rachat2)"""), F.lit(0))
             .when(F.expr(f"""(term_expoff-Mois_fin_annee)>{i}*12"""), F.expr(f"""(term - Mois_fin_annee -{i}*12)*(term - Mois_fin_annee -{i}*12+1)/((term+1)*term)*Primes_{gar}"""))
             .otherwise(F.expr(f"""(term-term_expoff)*(term-term_expoff+1)/(term*(term+1))*Primes_{gar}""")))
        .withColumn(f'GEP_{gar}0', F.expr(f"""Primes_{gar} - UEP_{gar}0"""))
        .withColumn(f'UEP_PL_{gar}0', F.expr(f"""UEP_{gar}0"""))
    )
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %do i=1 %to 15
        # ==============================================================
        # MANUAL REVIEW: indexed column from macro loop — use df.withColumn(f'UEP_{gar}{i}', ...) inside a Python for-loop
        # SAS: UEP_{gar}{i}=0
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %end
        # ==============================================================
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %do i=1 %to 15
        # ==============================================================
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %let j = %eval({i}-1)
        # ==============================================================
        # MANUAL REVIEW: indexed column from macro loop — use df.withColumn(f'GEP_{gar}{i}', ...) inside a Python for-loop
        # SAS: GEP_{gar}{i} = UEP_{gar}{j} - UEP_{gar}{i}
        # MANUAL REVIEW: indexed column from macro loop — use df.withColumn(f'UEP_PL_{gar}{i}', ...) inside a Python for-loop
        # SAS: UEP_PL_{gar}{i} = UEP_{gar}{i} - UEP_{gar}{j}
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %end
        # ==============================================================
    _dfs[f'CQS_PPNA_{gar}_2'].createOrReplaceTempView(f'CQS_PPNA_{gar}_2')

    _dfs[f'CQS_UEP_PL__{gar}'] = spark.table(f'CQS_PPNA_{gar}_2')
    # type_pret rajouté au HY24 pour la cession
    _dfs[f'CQS_UEP_PL__{gar}'] = _dfs[f'CQS_UEP_PL__{gar}'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret')
    _dfs[f'CQS_UEP_PL__{gar}'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}')

    _dfs[f'CQS_GEP_{gar}'] = spark.table(f'CQS_PPNA_{gar}_2')
    # type_pret rajouté au HY24 pour la cession
    _dfs[f'CQS_GEP_{gar}'] = _dfs[f'CQS_GEP_{gar}'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret')
    _dfs[f'CQS_GEP_{gar}'].createOrReplaceTempView(f'CQS_GEP_{gar}')

    _dfs[f'CQS_GEP_{gar}'] = spark.table('CQS_GEP_{gar}').orderBy('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat', 'Primes_{gar}', 'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret')
    _dfs[f'CQS_GEP_{gar}'].createOrReplaceTempView(f'CQS_GEP_{gar}')

    _dfs[f'CQS_UEP_PL__{gar}'] = spark.table('CQS_UEP_PL__{gar}').orderBy('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat', 'Primes_{gar}', 'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret')
    _dfs[f'CQS_UEP_PL__{gar}'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}')

    # PROC TRANSPOSE
    # wide-to-long: 16 columns -> 16 rows (_NAME_ = column name, COL1 = value)
    _dfs[f'CQS_GEP_{gar}_2'] = _dfs[f'CQS_GEP_{gar}'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret', F.expr(f"""stack(16, 'GEP_{gar}0', `GEP_{gar}0`, 'GEP_{gar}1', `GEP_{gar}1`, 'GEP_{gar}2', `GEP_{gar}2`, 'GEP_{gar}3', `GEP_{gar}3`, 'GEP_{gar}4', `GEP_{gar}4`, 'GEP_{gar}5', `GEP_{gar}5`, 'GEP_{gar}6', `GEP_{gar}6`, 'GEP_{gar}7', `GEP_{gar}7`, 'GEP_{gar}8', `GEP_{gar}8`, 'GEP_{gar}9', `GEP_{gar}9`, 'GEP_{gar}10', `GEP_{gar}10`, 'GEP_{gar}11', `GEP_{gar}11`, 'GEP_{gar}12', `GEP_{gar}12`, 'GEP_{gar}13', `GEP_{gar}13`, 'GEP_{gar}14', `GEP_{gar}14`, 'GEP_{gar}15', `GEP_{gar}15`) as (_NAME_, COL1)"""))
    _dfs[f'CQS_GEP_{gar}_2'].createOrReplaceTempView(f'CQS_GEP_{gar}_2')

    # PROC TRANSPOSE
    # wide-to-long: 16 columns -> 16 rows (_NAME_ = column name, COL1 = value)
    _dfs[f'CQS_UEP_PL__{gar}_2'] = _dfs[f'CQS_UEP_PL__{gar}'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'type_pret', F.expr(f"""stack(16, 'UEP_PL_{gar}0', `UEP_PL_{gar}0`, 'UEP_PL_{gar}1', `UEP_PL_{gar}1`, 'UEP_PL_{gar}2', `UEP_PL_{gar}2`, 'UEP_PL_{gar}3', `UEP_PL_{gar}3`, 'UEP_PL_{gar}4', `UEP_PL_{gar}4`, 'UEP_PL_{gar}5', `UEP_PL_{gar}5`, 'UEP_PL_{gar}6', `UEP_PL_{gar}6`, 'UEP_PL_{gar}7', `UEP_PL_{gar}7`, 'UEP_PL_{gar}8', `UEP_PL_{gar}8`, 'UEP_PL_{gar}9', `UEP_PL_{gar}9`, 'UEP_PL_{gar}10', `UEP_PL_{gar}10`, 'UEP_PL_{gar}11', `UEP_PL_{gar}11`, 'UEP_PL_{gar}12', `UEP_PL_{gar}12`, 'UEP_PL_{gar}13', `UEP_PL_{gar}13`, 'UEP_PL_{gar}14', `UEP_PL_{gar}14`, 'UEP_PL_{gar}15', `UEP_PL_{gar}15`) as (_NAME_, COL1)"""))
    _dfs[f'CQS_UEP_PL__{gar}_2'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_2')

    _dfs[f'CQS_UEP_PL__{gar}_3'] = spark.table(f'CQS_UEP_PL__{gar}_2')
    _dfs[f'CQS_UEP_PL__{gar}_3'] = _dfs[f'CQS_UEP_PL__{gar}_3'].drop('COL2')
    _dfs[f'CQS_UEP_PL__{gar}_3'] = _dfs[f'CQS_UEP_PL__{gar}_3'].withColumnRenamed('COL1', 'UEP_PL')
    _dfs[f'CQS_UEP_PL__{gar}_3'] = _dfs[f'CQS_UEP_PL__{gar}_3'].withColumnRenamed('_NAME_', 'variable')
    _dfs[f'CQS_UEP_PL__{gar}_3'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_3')

    _dfs[f'CQS_GEP_{gar}_3'] = spark.table(f'CQS_GEP_{gar}_2')
    _dfs[f'CQS_GEP_{gar}_3'] = _dfs[f'CQS_GEP_{gar}_3'].drop('COL2')
    _dfs[f'CQS_GEP_{gar}_3'] = _dfs[f'CQS_GEP_{gar}_3'].withColumnRenamed('COL1', 'GEP')
    _dfs[f'CQS_GEP_{gar}_3'] = _dfs[f'CQS_GEP_{gar}_3'].withColumnRenamed('_NAME_', 'variable')
    _dfs[f'CQS_GEP_{gar}_3'].createOrReplaceTempView(f'CQS_GEP_{gar}_3')

    _dfs[f'CQS_GEP_{gar}_4'] = spark.table(f'CQS_GEP_{gar}_3')
    _dfs[f'CQS_GEP_{gar}_4'] = (_dfs[f'CQS_GEP_{gar}_4']
        .withColumn('SURV',
        F.when(F.col('Variable') == f"GEP_{gar2}0", F.col('Generation'))
         .when(F.col('Variable') == f"GEP_{gar2}1", F.col('Generation') + 1)
         .when(F.col('Variable') == f"GEP_{gar2}2", F.col('Generation') + 2)
         .when(F.col('Variable') == f"GEP_{gar2}3", F.col('Generation') + 3)
         .when(F.col('Variable') == f"GEP_{gar2}4", F.col('Generation') + 4)
         .when(F.col('Variable') == f"GEP_{gar2}5", F.col('Generation') + 5)
         .when(F.col('Variable') == f"GEP_{gar2}6", F.col('Generation') + 6)
         .when(F.col('Variable') == f"GEP_{gar2}7", F.col('Generation') + 7)
         .when(F.col('Variable') == f"GEP_{gar2}8", F.col('Generation') + 8)
         .when(F.col('Variable') == f"GEP_{gar2}9", F.col('Generation') + 9)
         .when(F.col('Variable') == f"GEP_{gar2}10", F.col('Generation') + 10)
         .when(F.col('Variable') == f"GEP_{gar2}11", F.col('Generation') + 11)
         .when(F.col('Variable') == f"GEP_{gar2}12", F.col('Generation') + 12)
         .when(F.col('Variable') == f"GEP_{gar2}13", F.col('Generation') + 13)
         .when(F.col('Variable') == f"GEP_{gar2}14", F.col('Generation') + 14)
         .when(F.col('Variable') == f"GEP_{gar2}15", F.col('Generation') + 15)
         .otherwise(F.lit(None)  # no ELSE: null when false))
    )
    _dfs[f'CQS_GEP_{gar}_4'].createOrReplaceTempView(f'CQS_GEP_{gar}_4')

    _dfs[f'CQS_UEP_PL__{gar}_4'] = spark.table(f'CQS_UEP_PL__{gar}_3')
    _dfs[f'CQS_UEP_PL__{gar}_4'] = (_dfs[f'CQS_UEP_PL__{gar}_4']
        .withColumn('SURV',
        F.when(F.col('Variable') == f"UEP_PL_{gar2}0", F.col('Generation'))
         .when(F.col('Variable') == f"UEP_PL_{gar2}1", F.col('Generation') + 1)
         .when(F.col('Variable') == f"UEP_PL_{gar2}2", F.col('Generation') + 2)
         .when(F.col('Variable') == f"UEP_PL_{gar2}3", F.col('Generation') + 3)
         .when(F.col('Variable') == f"UEP_PL_{gar2}4", F.col('Generation') + 4)
         .when(F.col('Variable') == f"UEP_PL_{gar2}5", F.col('Generation') + 5)
         .when(F.col('Variable') == f"UEP_PL_{gar2}6", F.col('Generation') + 6)
         .when(F.col('Variable') == f"UEP_PL_{gar2}7", F.col('Generation') + 7)
         .when(F.col('Variable') == f"UEP_PL_{gar2}8", F.col('Generation') + 8)
         .when(F.col('Variable') == f"UEP_PL_{gar2}9", F.col('Generation') + 9)
         .when(F.col('Variable') == f"UEP_PL_{gar2}10", F.col('Generation') + 10)
         .when(F.col('Variable') == f"UEP_PL_{gar2}11", F.col('Generation') + 11)
         .when(F.col('Variable') == f"UEP_PL_{gar2}12", F.col('Generation') + 12)
         .when(F.col('Variable') == f"UEP_PL_{gar2}13", F.col('Generation') + 13)
         .when(F.col('Variable') == f"UEP_PL_{gar2}14", F.col('Generation') + 14)
         .when(F.col('Variable') == f"UEP_PL_{gar2}15", F.col('Generation') + 15)
         .otherwise(F.lit(None)  # no ELSE: null when false))
    )
    _dfs[f'CQS_UEP_PL__{gar}_4'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_4')

    _dfs[f'CQS_GEP_{gar}_5'] = spark.table(f'CQS_GEP_{gar}_4')
    _dfs[f'CQS_GEP_{gar}_5'] = (_dfs[f'CQS_GEP_{gar}_5']
        .withColumn('Rachat', F.when(F.expr("""(year((Date_Rachat2))=SURV)"""), F.expr(f"""-Rachat_{gar}""")))  # no ELSE: null when condition is false
        .withColumn('GWP', F.when(F.expr("""(year((Date_dbt_Assce))=SURV)"""), F.expr(f"""Primes_{gar}""")))  # no ELSE: null when condition is false
        .withColumn('GAR', F.when(F.expr(f"""{gar2}='IARD'"""), F.lit(30)).otherwise(F.lit(10)))
        .withColumn('IDEAN', F.when(F.expr(f"""{gar2}='IARD'"""), F.expr("""cast(concat("1"||substr(ID_Police,6,4)) as long)""")).otherwise(F.expr("""cast(concat("1"||substr(ID_Police,1,4)) as long)""")))
    )
    # type_pret rajouté au HY24 pour la cession
        # IF/THEN (manual review needed): if Rachat_{gar}=. then Rachat_{gar}=0
    _dfs[f'CQS_GEP_{gar}_5'] = _dfs[f'CQS_GEP_{gar}_5'].filter(~F.expr(f"""SURV>year({date_val})"""))
    _dfs[f'CQS_GEP_{gar}_5'] = _dfs[f'CQS_GEP_{gar}_5'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'SURV', 'GAR', 'GEP', 'GWP', 'Rachat', 'IDEAN', 'type_pret')
    _dfs[f'CQS_GEP_{gar}_5'].createOrReplaceTempView(f'CQS_GEP_{gar}_5')

    _dfs[f'CQS_UEP_PL__{gar}_5'] = spark.table(f'CQS_UEP_PL__{gar}_4')
    _dfs[f'CQS_UEP_PL__{gar}_5'] = (_dfs[f'CQS_UEP_PL__{gar}_5']
        .withColumn('GAR', F.when(F.expr(f"""{gar2}='IARD'"""), F.lit(30)).otherwise(F.lit(10)))
        .withColumn('IDEAN', F.when(F.expr(f"""{gar2}='IARD'"""), F.expr("""cast(concat("1"||substr(ID_Police,6,4)) as long)""")).otherwise(F.expr("""cast(concat("1"||substr(ID_Police,1,4)) as long)""")))
    )
    # type_pret rajouté au HY24 pour la cession
    _dfs[f'CQS_UEP_PL__{gar}_5'] = _dfs[f'CQS_UEP_PL__{gar}_5'].filter(~F.expr(f"""SURV>year({date_val})"""))
    _dfs[f'CQS_UEP_PL__{gar}_5'] = _dfs[f'CQS_UEP_PL__{gar}_5'].select('Financiere_Adh', 'ID_Police', 'sit_prof_1', 'ID_Adh', 'Date_Dbt_Assce', 'Date_Fin_Assce', 'Date_Sin', 'Generation', 'Date_Rachat2', 'Date_Rachat', f'Primes_{gar}', f'Rachat_{gar}', 'Date_term', 'term', 'term_expoff', 'Mois_fin_annee', 'Quotient_res', 'mois_rest', 'SURV', 'GAR', 'UEP_PL', 'IDEAN', 'type_pret')
    _dfs[f'CQS_UEP_PL__{gar}_5'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_5')


uep_cqs_78(gar="IARD", gar2="IARD")
uep_cqs_78(gar="VIE", gar2="VIE")
def creer_table(gar):
    _dfs[f'CQS_UEP_PL__{gar}_6'] = spark.sql("""select distinct Financiere_Adh, ID_Police,sit_prof_1, IDEAN,Generation,GAR,SURV, sum(UEP_PL) as UEP_PL, type_pret
    from CQS_UEP_PL__{gar}_5
    group by Financiere_Adh, ID_Police, sit_prof_1, IDEAN,Generation,GAR,SURV, type_pret""")
    _dfs[f'CQS_UEP_PL__{gar}_6'].createOrReplaceTempView(f'CQS_UEP_PL__{gar}_6')

    _dfs[f'CQS_GEP_{gar}_6'] = spark.sql("""select distinct Financiere_Adh, ID_Police, sit_prof_1, IDEAN, Generation,GAR,SURV, sum(GEP) as GEP, sum(GWP) as GWP, sum(Rachat) as Rachat,type_pret
    from CQS_GEP_{gar}_5
    group by Financiere_Adh, ID_Police,sit_prof_1, IDEAN,Generation,GAR,SURV,type_pret""")
    _dfs[f'CQS_GEP_{gar}_6'].createOrReplaceTempView(f'CQS_GEP_{gar}_6')


creer_table(gar="iard")
creer_table(gar="vie")
CQS_GEP = spark.table('CQS_GEP_iard_6') \
    .union(spark.table('CQS_GEP_VIE_6'))
CQS_GEP = (CQS_GEP
    .withColumn('idean2', F.col('idean'))
)
# ATTRIB: attrib idean2 length = $50 format = $50. informat =$50. label ='idean'
CQS_GEP = CQS_GEP.drop('idean')
CQS_GEP = CQS_GEP.withColumnRenamed('idean2', 'idean')
CQS_GEP.createOrReplaceTempView('CQS_GEP')

CQS_UEP_PL = spark.table('CQS_UEP_PL__iard_6') \
    .union(spark.table('CQS_UEP_PL__VIE_6'))
CQS_UEP_PL.createOrReplaceTempView('CQS_UEP_PL')

CQS_UEP_PL = spark.table('CQS_UEP_PL')
CQS_UEP_PL = (CQS_UEP_PL
    .withColumn('idean_char', F.col('idean').cast('string'))
)
CQS_UEP_PL = CQS_UEP_PL.drop('idean')
CQS_UEP_PL = CQS_UEP_PL.withColumnRenamed('idean_char', 'idean')
CQS_UEP_PL.createOrReplaceTempView('CQS_UEP_PL')

CQS_GEP = spark.table('CQS_GEP')
CQS_GEP.createOrReplaceTempView('CQS_GEP')
# LIBNAME CQS_out -> Unity Catalog: {_catalog}.cqs_out
CQS_GEP.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.CQS_GEP')

CQS_UEP_PL = spark.table('CQS_UEP_PL')
CQS_UEP_PL.createOrReplaceTempView('CQS_UEP_PL')
# LIBNAME CQS_OUT -> Unity Catalog: {_catalog}.cqs_out
CQS_UEP_PL.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.CQS_UEP_PL')

# /
# COMMISSION
# /
# Taux de comm à demander en début de processus
def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


chemin_imp = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/00 - Documents et parametres/20250918_Commission_Life_NoLife_2025.xlsx"
input_ = chemin_imp
import_excelx(datafile=input_, out="Tx_COM_input", onglet="CQS Commissions")
Tx_COM_input_2 = spark.table('Tx_COM_input')
Tx_COM_input_2 = Tx_COM_input_2.filter(F.col('ID_Police').isNotNull())
Tx_COM_input_2.createOrReplaceTempView('Tx_COM_input_2')

# Garantie 10
Taux_COM_CQS_VIE_0 = spark.sql("""select 
Financiere_Adh,
Generation, 
Sit_Prof_1,
ID_Police, 
Comm_Rate_LIFE as Taux_Com
from Tx_COM_input_2
group by Financiere_Adh,generation,Sit_Prof_1,ID_Police """)
Taux_COM_CQS_VIE_0.createOrReplaceTempView('Taux_COM_CQS_VIE_0')

Taux_COM_CQS_VIE = spark.table('Taux_COM_CQS_VIE_0')
Taux_COM_CQS_VIE = (Taux_COM_CQS_VIE
    .withColumn('GAR', F.lit(10))
    .withColumn('IDEAN', F.expr("""cast(concat("1"||substring(ID_Police,1,4)) as long)"""))
)
Taux_COM_CQS_VIE.createOrReplaceTempView('Taux_COM_CQS_VIE')

# Garantie 30
Taux_COM_CQS_IARD_0 = spark.sql("""select 
Financiere_Adh,
Generation, 
Sit_Prof_1,
ID_Police,
Comm_rate_NON_LIFE as Taux_Com
from Tx_COM_input_2
group by Financiere_Adh,Generation,Sit_Prof_1,ID_Police """)
Taux_COM_CQS_IARD_0.createOrReplaceTempView('Taux_COM_CQS_IARD_0')

Taux_COM_CQS_IARD = spark.table('Taux_COM_CQS_IARD_0')
Taux_COM_CQS_IARD = (Taux_COM_CQS_IARD
    .withColumn('GAR', F.lit(30))
    .withColumn('IDEAN', F.expr("""cast(concat("1"||substring(ID_Police,6,4)) as long)"""))
)
Taux_COM_CQS_IARD.createOrReplaceTempView('Taux_COM_CQS_IARD')

work = spark.table('Taux_COM_CQS_VIE') \
    .union(spark.table('Taux_COM_CQS_IARD'))
# CQS_Out.
work.createOrReplaceTempView('work')

Taux_COM = spark.table('Taux_COM_CQS')
Taux_COM = (Taux_COM
    .withColumn('idean_char', F.col('idean').cast('string'))
)
Taux_COM = Taux_COM.filter(~F.expr("""IDEAN IS NULL"""))
Taux_COM = Taux_COM.drop('idean')
Taux_COM = Taux_COM.withColumnRenamed('idean_char', 'idean')
Taux_COM.createOrReplaceTempView('Taux_COM')

CQS_GEP_2 = spark.sql("""select A.*,
		   B.TAUX_COM 
	from CQS_GEP A left join Taux_COM B on A.Financiere_Adh=B.Financiere_Adh and A.Generation=B.Generation and
	A.Sit_Prof_1 = B.Sit_Prof_1 and A.ID_Police =B.ID_Police and A.GAR=B.GAR  and 
	regexp_replace(A.IDEAN, ' ', '')=regexp_replace(B.IDEAN, ' ', '') """)
CQS_GEP_2.createOrReplaceTempView('CQS_GEP_2')

CQS_GEP_2_bis = spark.table('CQS_GEP_2')
CQS_GEP_2_bis = (CQS_GEP_2_bis
    .withColumn('Taux_COM', F.when(F.expr("""Taux_COM IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('Rachat', F.when(F.expr("""Rachat IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('COM', F.when(F.expr("""COM IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('GWP', F.when(F.expr("""GWP IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('COM', F.expr("""GWP*Taux_COM"""))
    .withColumn('COM_Rachat', F.expr("""Rachat*Taux_COM"""))
    .withColumn('COM_NetLapse', F.col('COM') + F.col('COM_Rachat'))
    .withColumn('GWP_NetLapse', F.col('GWP') + F.col('Rachat'))
)
CQS_GEP_2_bis.createOrReplaceTempView('CQS_GEP_2_bis')

work = spark.table('CQS_GEP_2_bis')
# cqs_out.
work.createOrReplaceTempView('work')

CQS_UEP_PL_2 = spark.sql("""select A.*,
		   B.TAUX_COM
	from CQS_UEP_PL A 
		left join Taux_COM B 
		on A.Financiere_Adh=B.Financiere_Adh and A.Generation=B.Generation and
		A.Sit_Prof_1 = B.Sit_Prof_1 and A.ID_Police =B.ID_Police and A.GAR=B.GAR  and A.IDEAN=B.IDEAN """)
CQS_UEP_PL_2.createOrReplaceTempView('CQS_UEP_PL_2')

CQS_UEP_PL_2_bis = spark.table('CQS_UEP_PL_2')
CQS_UEP_PL_2_bis = (CQS_UEP_PL_2_bis
    .withColumn('Taux_COM', F.when(F.expr("""Taux_COM IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('DAC', F.expr("""UEP_PL*Taux_COM"""))
)
CQS_UEP_PL_2_bis.createOrReplaceTempView('CQS_UEP_PL_2_bis')

# Création des outputs pour l'inventaire
CQS_UEP_PL_3 = spark.sql("""select
IDEAN,
GAR,
type_pret,
Surv,generation as gen,
sum(UEP_PL) as UEP,
sum(DAC) as DAC
from CQS_UEP_PL_2_bis

group by IDEAN, GAR, SURV ,Gen,type_pret""")
CQS_UEP_PL_3.createOrReplaceTempView('CQS_UEP_PL_3')

CQS_UEP_GEN_SURV = spark.sql("""select
IDEAN,
GAR,
Generation as GEN,
SURV,
sum(UEP_PL) as UEP,
sum(DAC) as DAC
from CQS_UEP_PL_2_bis

group by IDEAN, GAR, SURV, Gen,type_pret""")
CQS_UEP_GEN_SURV.createOrReplaceTempView('CQS_UEP_GEN_SURV')

# base CQS_GEP_3 exportée par la suite
CQS_GEP_3 = spark.sql("""select
IDEAN,
GAR,
type_pret,SURV,generation,
sum(GWP_NetLapse) as PRIMES,
sum(COM_NetLapse) as COM
from CQS_GEP_2_bis
where surv not = {n} 
group by IDEAN, GAR, SURV,type_pret,generation """)
CQS_GEP_3.createOrReplaceTempView('CQS_GEP_3')

CQS_GEP_3 = spark.table('CQS_GEP_3')
CQS_GEP_3 = CQS_GEP_3.filter(~F.expr("""idean IS NULL"""))
CQS_GEP_3 = CQS_GEP_3.filter(~F.expr("""idean=1"""))
CQS_GEP_3.createOrReplaceTempView('CQS_GEP_3')
# LIBNAME CQS_Out -> Unity Catalog: {_catalog}.cqs_out
CQS_GEP_3.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.CQS_GEP_3')

CQS_UEP_PL_3 = spark.table('CQS_UEP_PL_3')
CQS_UEP_PL_3 = CQS_UEP_PL_3.filter(~F.expr("""idean IS NULL"""))
CQS_UEP_PL_3 = CQS_UEP_PL_3.filter(~F.expr("""idean=1"""))
CQS_UEP_PL_3.createOrReplaceTempView('CQS_UEP_PL_3')
# LIBNAME CQS_Out -> Unity Catalog: {_catalog}.cqs_out
CQS_UEP_PL_3.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.CQS_UEP_PL_3')

CQS_UEP_GEN_SURV = spark.table('CQS_UEP_GEN_SURV')
CQS_UEP_GEN_SURV = CQS_UEP_GEN_SURV.filter(~F.expr("""idean IS NULL"""))
CQS_UEP_GEN_SURV = CQS_UEP_GEN_SURV.filter(~F.expr("""idean=1"""))
CQS_UEP_GEN_SURV.createOrReplaceTempView('CQS_UEP_GEN_SURV')
# LIBNAME CQS_Out -> Unity Catalog: {_catalog}.cqs_out
CQS_UEP_GEN_SURV.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.CQS_UEP_GEN_SURV')

def export_excel(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


export_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/Output_CQS.xlsx"
export_excel(datatable="cqs_out.cqs_gep_3", database=export_01, sheet="GEP")
export_excel(datatable="cqs_out.cqs_gep_2_bis", database=export_01, sheet="Détail")
export_excel(datatable="cqs_out.cqs_uep_pl_3", database=export_01, sheet="UEP")
# /
GEP = spark.table('cqs_gep_3')
GEP.createOrReplaceTempView('GEP')

UEP = spark.table('cqs_uep_pl_3')
UEP.createOrReplaceTempView('UEP')

# Garantie 10
GLS_10 = spark.sql("""select Annee, sum(Stop_Loss) as Stop_Loss
 from gsl  
 Where GAR = 10 
 group by Annee""")
GLS_10.createOrReplaceTempView('GLS_10')

EP1 = spark.sql("""select SURV as annee , sum(UEP) as EP1, sum(DAC) as DAC
 from UEP  
 Where Gar = 10 
 group by SURV""")
EP1.createOrReplaceTempView('EP1')

EP2 = spark.sql("""select SURV as annee , sum(PRIMES) as EP2, sum(com) as com
 from GEP  
 Where Gar = 10 
 group by SURV""")
EP2.createOrReplaceTempView('EP2')

EP_10_ = spark.sql("""select A.annee , EP2-Ep1 AS Ep
 from EP1 a left join EP2 b on
 A.annee =B.annee""")
EP_10_.createOrReplaceTempView('EP_10_')

# proc sql ;
# create table  EP_10 As
# select A.annee ,'10' as GAR ,EP, B.Stop_Loss, Ep+B.Stop_Loss as  GEP
# from EP_10 a left join GLS_10 b on
# A.annee =B.annee;run; quit;
EP_10_ = spark.table('EP_10_').orderBy('annee')
EP_10_.createOrReplaceTempView('EP_10_')

GLS_10 = spark.table('GLS_10').orderBy('annee')
GLS_10.createOrReplaceTempView('GLS_10')

# MERGE: LEFT JOIN  (if a  - keep all from left)
EP_10 = spark.table('EP_10_').join(spark.table('GLS_10'), ['annee'], 'left')
EP_10 = (EP_10
    .withColumn('GEP', F.expr("""(coalesce(Ep, 0) + coalesce(Stop_Loss, 0))"""))
)
EP_10.createOrReplaceTempView('EP_10')

EP_10 = spark.sql("""select annee , '10' as GAR ,EP, Stop_Loss,  GEP 
from EP_10 """)
EP_10.createOrReplaceTempView('EP_10')

COM_DAC_10 = spark.sql("""select A.annee,'10' as GAR , com-dac AS COMM_DAC
 from EP1 a left join EP2 b on
 A.annee =B.annee""")
COM_DAC_10.createOrReplaceTempView('COM_DAC_10')

# Garantie 30
GLS_30 = spark.sql("""select Annee, sum(Stop_Loss) as Stop_Loss
 from gsl  
 Where GAR = 30 
 group by Annee""")
GLS_30.createOrReplaceTempView('GLS_30')

EP1 = spark.sql("""select SURV as annee , sum(UEP) as EP1, sum(DAC) as DAC
 from UEP  
 Where Gar = 30 
 group by SURV""")
EP1.createOrReplaceTempView('EP1')

EP2 = spark.sql("""select SURV as annee , sum(PRIMES) as EP2 ,sum(com) as com
 from GEP  
 Where Gar = 30 
 group by SURV""")
EP2.createOrReplaceTempView('EP2')

EP_30_ = spark.sql("""select A.annee , EP2-Ep1 AS Ep
 from EP1 a left join EP2 b on
 A.annee =B.annee""")
EP_30_.createOrReplaceTempView('EP_30_')

# proc sql ;
# create table  EP_30 As
# select A.annee ,'30' as GAR ,EP, B.Stop_Loss, Ep+B.Stop_Loss as  GEP
# from EP_30_ a left join GLS_30 b on
# A.annee =B.annee;run; quit;
EP_30_ = spark.table('EP_30_').orderBy('annee')
EP_30_.createOrReplaceTempView('EP_30_')

GLS_30 = spark.table('GLS_30').orderBy('annee')
GLS_30.createOrReplaceTempView('GLS_30')

# MERGE: LEFT JOIN  (if a  - keep all from left)
EP_30 = spark.table('EP_30_').join(spark.table('GLS_30'), ['annee'], 'left')
EP_30 = (EP_30
    .withColumn('GEP', F.expr("""(coalesce(Ep, 0) + coalesce(Stop_Loss, 0))"""))
)
EP_30.createOrReplaceTempView('EP_30')

EP_30 = spark.sql("""select annee , '30' as GAR ,EP, Stop_Loss,  GEP 
from EP_30 """)
EP_30.createOrReplaceTempView('EP_30')

COM_DAC_30 = spark.sql("""select A.annee,'30' as GAR , com-dac AS COMM_DAC
from EP1 a left join EP2 b on
A.annee =B.annee""")
COM_DAC_30.createOrReplaceTempView('COM_DAC_30')

EP = spark.table('EP_10') \
    .union(spark.table('Ep_30'))
EP.createOrReplaceTempView('EP')

COM_DAC = spark.table('COM_DAC_10') \
    .union(spark.table('COM_DAC_30'))
COM_DAC.createOrReplaceTempView('COM_DAC')

export_excelx(database=export_xx, datatable=EP, sheet=f"GEP )
%EXPORT_EXCELX(DATABASE={export_xx}", datatable=COM_DAC, sheet="COM")
# == On recupère ici les durations moyennes/ Pour le process de création de la base MP  -- Janv 2023 ==
duration_cqs_cbp = spark.table('cqs_gep_iard_5') \
    .union(spark.table('cqs_gep_vie_5'))
duration_cqs_cbp = duration_cqs_cbp.filter(~F.expr("""GEP =0"""))
duration_cqs_cbp = duration_cqs_cbp.select('Financiere_Adh', 'date_dbt_assce', 'date_fin_assce', 'term', 'GAR', 'IDEAN', 'GEP')
duration_cqs_cbp.createOrReplaceTempView('duration_cqs_cbp')

duration_cqs_cbp = spark.table('duration_cqs_cbp').orderBy('IDEAN', 'GAR', 'Financiere_Adh')
duration_cqs_cbp.createOrReplaceTempView('duration_cqs_cbp')

# PROC SUMMARY: SUM of ['term'] grouped by ['IDEAN', 'GAR', 'Financiere_Adh']
duration_cqs_cbp_moy = duration_cqs_cbp.groupBy('IDEAN', 'GAR', 'Financiere_Adh').agg(F.sum('term').alias('term'))
duration_cqs_cbp_moy.createOrReplaceTempView('duration_cqs_cbp_moy')

duration_moy_cqs_cbp = spark.table('duration_cqs_cbp_moy')
duration_moy_cqs_cbp.createOrReplaceTempView('duration_moy_cqs_cbp')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
duration_moy_cqs_cbp.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.duration_moy_cqs_cbp')
