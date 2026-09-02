from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ####################################################
# ################### INVENTAIRE TIA #################
# ####################################################
def import_excel(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


def export_excel(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


def export_excelx(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


# #########################################################################
# ################### 2ème Etape: Extraction des données  #################
# #########################################################################
lreseau = "~/NAS/X"
# Mettre le serveur approprié  entre -> ~/NAS/X  ou -> X:/Inventprev **
arrete = "2026_04_V2"
tia_path = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
# on uniformise les variables
def extract_data(country):
    # check #1 :
    # ################ GL Ledger ##############
    # %let Country = DE ;
    # /
    _dfs[f'GLOBAL_PL_{country}_AGREG_2'] = spark.sql(f"""select 
          gl_period,
          occurence_period, 
          scheme_id,
          scheme_version,
          cover_code_vorig as cover, 
          cohort_date as GEN ,
          (incident_date) as SURV format = ddmmyy10.,
          insurance_type_macro as ins_type, 
          local_currency as currency_code, 
          countryid_vorig as country, 
          axa_entity_code as entity_cd, 
          axa_risk_carrier as entity_name , 
          partner_name_vorig as AGENT_NAME_ORIG ,  
          kpi_name_0 as account_hierarchy_name,
          sum(amt_local_currency) FORMAT = NLBEST12. AS amt
              
    from TIA.GLOBAL_PL_{country}
    group by   gl_period,occurence_period, scheme_id,scheme_version,cover_code_vorig, incident_date,cohort_date,insurance_type_macro, local_currency, countryid_vorig,axa_entity_code, axa_risk_carrier, partner_name_vorig, kpi_name_0 """)
    _dfs[f'GLOBAL_PL_{country}_AGREG_2'].createOrReplaceTempView(f'GLOBAL_PL_{country}_AGREG_2')

    _dfs[f'GLOBAL_PL_{country}_AGREG_3'] = spark.table(f'GLOBAL_PL_{country}_AGREG_2')
    _dfs[f'GLOBAL_PL_{country}_AGREG_3'] = (_dfs[f'GLOBAL_PL_{country}_AGREG_3']
        .withColumn('POSTE', F.lit(None).cast(StringType()))  # LENGTH POSTE $40
        .withColumn('SCHEME', F.lit(None).cast(StringType()))  # LENGTH SCHEME $40
        .withColumn('entity', F.lit(None).cast(StringType()))  # LENGTH entity $40
        .withColumn('POSTE',
            F.when(F.expr("""account_hierarchy_name IN ('Claims Handling Fees - Direct','Claims Paid - Accepted','Claims Paid - Direct','Claims Handling Fees - Accepted','Claims Doctor Fees - Accepted','Claims Doctor Fees - Direct')"""), F.lit('CLAIM'))
             .when(F.expr("""account_hierarchy_name IN ('Gross Written Premium Cancellations - Direct','Gross Written Premium Cancellations - Accepted','Gross Written Premiums gross of Cancellations - Accepted','Gross Written Premiums gross of Cancellations - Direct','Gross Written Premiums gross of Cancellations - Ceded')"""), F.lit('PREMIUM'))
             .when(F.expr("""account_hierarchy_name IN ('Gross Commission Cancellations - Direct','Gross Commission Cancellations - Accepted','Gross Commission Cancellations - Ceded','Gross Commissions - Accepted','Gross Commissions - Direct','Gross Commissions - Ceded')"""), F.lit('COMMISSION'))
             .when(F.expr("""account_hierarchy_name IN ('Profit Share - Direct','Profit Share - Accepted' ,'Profit Share - BLE Settlement')"""), F.lit('PS_PAID'))
             .when(F.expr("""account_hierarchy_name IN ('Claims Paid - Ceded')"""), F.lit('CLAIM_CED'))
             .otherwise(F.lit('')))
        .withColumn('Type_Insurance', F.when(F.expr("""ins_type IN ('DIRECT UNDERWRITER','DIRECT INSURER')"""), F.lit(0)))
        .withColumn('Type_Insurance', F.when(F.expr("""account_hierarchy_name IN ('Claims Paid - Ceded','Gross Commissions - Ceded','Gross Written Premiums gross of Cancellations - Ceded') AND Type_Insurance=0"""), F.lit(11)))
        .withColumn('Type_Insurance', F.when(F.expr("""account_hierarchy_name IN ('Claims Paid - Ceded','Gross Commissions - Ceded','Gross Written Premiums gross of Cancellations - Ceded') AND Type_Insurance=4"""), F.lit(8)))
        .withColumn('year_gen', F.expr("""substring(GEN,1,4)*1"""))
        .withColumn('month_gen', F.expr("""substring(GEN,6,7)*1"""))
        .withColumn('GEN2', F.expr("""make_date(month_gen, 01, year_gen)"""))
        .withColumn('year_surv', F.expr("""substring(occurence_period,1,4)*1"""))
        .withColumn('month_surv', F.expr("""substring(occurence_period,6,7)*1"""))
        .withColumn('year_gl', F.expr("""substring(gl_period,1,4)*1"""))
        .withColumn('month_gl', F.expr("""substring(gl_period,6,7)*1"""))
        .withColumn('SURV2', F.expr("""make_date(month_surv, 01, year_surv)"""))
        .withColumn('occurence_period2', F.when(F.expr("""month_surv IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(year_surv,"0",month_surv)""")))
        .withColumn('occurence_period2', F.when(F.expr("""month_surv IN (10,11,12)"""), F.expr("""concat(year_surv,month_surv)""")))
        .withColumn('gl_period2', F.when(F.expr("""month_gl IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(year_gl,"0",month_gl)""")))
        .withColumn('gl_period2', F.when(F.expr("""month_gl IN (10,11,12)"""), F.expr("""concat(year_gl,month_gl)""")))
        .withColumn('entity', F.col('entity_name'))
        .withColumn('SCHEME', F.expr("""concat(trim(scheme_id), '.', (trim(scheme_version )))"""))
    )
    # FORMAT/INFORMAT: FORMAT GEN2  DDMMYY10.
    # FORMAT/INFORMAT: FORMAT SURV2  DDMMYY10.
    # occurence_period2=cats(year_surv,month_surv) ;
    # gl_period2=cats(year_gl,month_gl) ;
    _dfs[f'GLOBAL_PL_{country}_AGREG_3'].createOrReplaceTempView(f'GLOBAL_PL_{country}_AGREG_3')

    _dfs[f'DATABASE_{country}_PL'] = spark.sql(f"""select distinct
          country,
          cover,      
          entity_cd,
          gl_period2 as gl_period,
          occurence_period2 as occurrence_period,
          Type_Insurance,
          SCHEME,
          currency_code,
          entity_name,
          GEN2 as GEN, 
          SURV, 
          AGENT_NAME_ORIG as RGPT, 
          POSTE,
          sum(amt) AS MONTANT, 
          account_hierarchy_name, 
          0000 as claim_case_no, 
          entity_name as entity
             
    from GLOBAL_PL_{country}_AGREG_3
    WHERE POSTE <> '' 
    group by    country, cover, entity_cd, gl_period2,occurence_period2, Type_Insurance, SCHEME, currency_code, entity_name, GEN2, SURV, AGENT_NAME_ORIG, POSTE, account_hierarchy_name, claim_case_no, entity""")
    _dfs[f'DATABASE_{country}_PL'].createOrReplaceTempView(f'DATABASE_{country}_PL')

    _dfs[f'DATABASE_{country}_PL'] = spark.table(f'DATABASE_{country}_PL')
    _dfs[f'DATABASE_{country}_PL'].createOrReplaceTempView(f'DATABASE_{country}_PL')
    # LIBNAME TIA -> base Spark: tia.DATABASE_{country}_PL
    _dfs[f'DATABASE_{country}_PL'].write.mode('overwrite').saveAsTable(f'tia.DATABASE_{country}_PL')

    # PROC DATASETS → Spark table operations

    # PROC DATASETS → Spark table operations

    # PROC DATASETS → Spark table operations

    # PROC DATASETS → Spark table operations


extract_data(country="AT")
extract_data(country="BE")
extract_data(country="CH")
extract_data(country="CO")
extract_data(country="DE")
extract_data(country="DK")
extract_data(country="ES")
extract_data(country="FI")
extract_data(country="FR")
extract_data(country="GR")
extract_data(country="IE")
extract_data(country="IT")
extract_data(country="LT")
extract_data(country="LU")
extract_data(country="MX")
extract_data(country="NI")
extract_data(country="NL")
extract_data(country="NO")
extract_data(country="PE")
extract_data(country="PL")
extract_data(country="PT")
extract_data(country="SE")
extract_data(country="TR")
extract_data(country="UK")
# Concaténation des bases par pays
DATABASE_ALL_PL = spark.table('tia.DATABASE_:')
DATABASE_ALL_PL.createOrReplaceTempView('DATABASE_ALL_PL')
# LIBNAME TIA -> base Spark: tia.DATABASE_ALL_PL
DATABASE_ALL_PL.write.mode('overwrite').saveAsTable('tia.DATABASE_ALL_PL')

# Exlusion des schèmes de Ex-Macao
import_01 = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Scheme Database/Input/SDB.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="flag_legacy", onglet="flag_legacy")
# proc sql ;
# create table DATABASE_ALL_PL_F as
# select distinct
# t1.*,
# t8.Data_Validated AS flag_legacy,
# t8.RPP AS RPP,
# t8.Agent_Id AS Agent_Id,
# t8.agent_name
# from TIA.DATABASE_ALL_PL  t1
# left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.SCHEME=t8.scheme and t1.cover=t8.Cover) ;
# quit ;
# Pour bien isoler Macao il faut faire une combinaison des trois règles
# 1. Agent ID  between 75000 and 79999
# 2. Agent Name with _FOS in the name
# 3. RPP present
# DATA DATABASE_ALL_PL_F ;
# set   DATABASE_ALL_PL_F ;
# if RPP not in ("0","") or Agent_Id =: "75" or prxmatch('/FOS|_FOS/',Agent_Name) then LEGACY_SCHEME_BOOK="MACAO";
# else LEGACY_SCHEME_BOOK="TIA";
# run ;
# DATA TIA.DATABASE_ALL_PL_F ;
# set DATABASE_ALL_PL_F ;
# RUN;
DATABASE_ALL_PL_F = spark.table('tia.DATABASE_ALL_PL')
DATABASE_ALL_PL_F = (DATABASE_ALL_PL_F
    .withColumn('Agent_name', F.when(F.expr("""h.find() != 0"""), F.lit('')))
    .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""h.find() != 0"""), F.lit('TIA')))
)
# FORMAT/INFORMAT: format Data_Validated $1. RPP $14. Agent_ID $7. Agent_name $33. LEGACY_SCHEME_BOOK $5.
# IF/THEN (manual review needed):
#   if _n_ = 1 then do ;
#       declare hash h(dataset:"FLAG_LEGACY") ;
#       __COMMENT__:Création de la table HASH ;
#       h.defineKey("country", "scheme", "Cover") ;
#       h.defineData("Data_Validated", "RPP", "Agent_ID" ,"Agent_name") ;
#       h.defineDone() ;
#   end ;
DATABASE_ALL_PL_F = DATABASE_ALL_PL_F.withColumnRenamed('Data_Validated', 'flag_legacy')
DATABASE_ALL_PL_F.createOrReplaceTempView('DATABASE_ALL_PL_F')
# LIBNAME TIA -> base Spark: tia.DATABASE_ALL_PL_F
DATABASE_ALL_PL_F.write.mode('overwrite').saveAsTable('tia.DATABASE_ALL_PL_F')

DATABASE_ALL_PL_F = spark.table('tia.DATABASE_ALL_PL_F')
DATABASE_ALL_PL_F = (DATABASE_ALL_PL_F
    .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Scheme = 'RCI.1' AND Country = 'IT'"""), F.lit('TIA')))
)
DATABASE_ALL_PL_F.createOrReplaceTempView('DATABASE_ALL_PL_F')
# LIBNAME TIA -> base Spark: tia.DATABASE_ALL_PL_F
DATABASE_ALL_PL_F.write.mode('overwrite').saveAsTable('tia.DATABASE_ALL_PL_F')
