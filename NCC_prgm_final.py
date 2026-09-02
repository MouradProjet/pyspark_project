from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# Paramétrage
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
quarter = "2026Q2"
month_reserving = 06
day_reserving = 26
yr_reserving = 2026
ouput = "CR_Q226"
month = 06
day = 26
yr = 2026
dt_arrete_reel = "26JUN2026"d"
# Quarter à changé à chaque arrêté : 8 derniers
q1 = "_2024Q3_Res"
q2 = "_2024Q4_Res"
q3 = "_2025Q1_Res"
q4 = "_2025Q2_Res"
q5 = "_2025Q3_Res"
q6 = "_2025Q4_Res"
q7 = "_2026Q1_Res"
q8 = "_2026Q2_Res"
s1 = "_2024Q3"
s2 = "_2024Q4"
s3 = "_2025Q1"
s4 = "_2025Q2"
s5 = "_2025Q3"
s6 = "_2025Q4"
s7 = "_2026Q1"
s8 = "_2026Q2"
# /
out_gep_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/GEP/Output/DAAP"  # LIBNAME Out_GEP
spark.sql('CREATE SCHEMA IF NOT EXISTS out_gep')  # base Spark pour LIBNAME Out_GEP
import_01 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/NON-CORE-COVER/Parametre/cover_params.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="all_params", onglet="param_all")
import_excel(file=import_01, out="Greece_params", onglet="param_GR")
import_excel(file=import_01, out="cover_dev", onglet="Cover_code")
import_01 = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2021_11_Q4/02_Elements_Techniques/TIA/Extraction Donnees/20210408 List TIA scheme exclusion - C and TPA.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="flag_double", onglet="Sheet1")
FLAG_DOUBLE = spark.table('FLAG_DOUBLE')
FLAG_DOUBLE.createOrReplaceTempView('FLAG_DOUBLE')

import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="flag_legacy", onglet="flag_legacy")
# /
def non_core(cc):
    # %let CC =SE;
    _dfs[f'{cc}_DATA_0'] = spark.table(f'out_gep.HISTO_FLUX_{cc}')
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].filter(F.expr("""Rsrv_Grp='ZZ1'"""))
    _dfs[f'{cc}_DATA_0'] = (_dfs[f'{cc}_DATA_0']
        .withColumn('year', F.expr("""substring(Quarter,3,4)"""))
        .withColumn('QTR', F.expr("""substring(Quarter,1,2)"""))
        .withColumn('Quarter2', F.expr("""concat(year,QTR)"""))
        .withColumn('Key', F.lit(None).cast(StringType()))  # LENGTH Key $100
        .withColumn('Key', F.expr("""concat(country,Agent,Product,cover,Entity_CD)"""))
    )
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].filter(~F.expr("""country='DK' AND scheme IN ('1F.1','1G.1')"""))
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].select('country', 'scheme', 'cover', 'Entity_CD', 'Quarter', 'COVER_CODE', 'Quarter2', 'REP', 'GEP', 'Claim_Paid', 'Agent', 'Product', 'year', 'QTR', 'Key')
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    # Exclusion des schemes de macao et la categorie C existante dans WEB
    _dfs[f'{cc}_DATA_0'] = spark.sql(f"""select
    t1.*,
    t7.RI_inwards_cash_matching AS flag_web
    from {cc}_DATA_0  t1
    left join flag_double t7 on (t1.country=t7.Country_CD AND t1.scheme=t7.contract_id_version)""")
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    _dfs[f'{cc}_DATA_0'] = spark.table(f'{cc}_DATA_0')
    _dfs[f'{cc}_DATA_0'] = (_dfs[f'{cc}_DATA_0']
        .withColumn('LEGACY_WEBXL_BOOK', F.when(F.expr("""flag_web=''"""), F.lit('TIA')))
        .withColumn('LEGACY_WEBXL_BOOK', F.when(F.expr("""flag_web != ''"""), F.lit('WEBXL')))
    )
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    _dfs[f'{cc}_DATA_0'] = spark.table(f'{cc}_DATA_0')
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].filter(F.expr("""LEGACY_WEBXL_BOOK='TIA'"""))
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].drop('LEGACY_WEBXL_BOOK', 'flag_web')
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    _dfs[f'{cc}_DATA_0'] = spark.sql(f"""select 
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao as Flag_Macao 
    from {cc}_DATA_0  t1 
    left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.scheme=t8.scheme and t1.cover=t8.Cover)""")
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    _dfs[f'{cc}_DATA_0'] = spark.table(f'{cc}_DATA_0')
    _dfs[f'{cc}_DATA_0'] = (_dfs[f'{cc}_DATA_0']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].drop('informer_type', 'RPP', 'Flag_Macao')
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    _dfs[f'{cc}_DATA_0'] = spark.table(f'{cc}_DATA_0')
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].filter(F.expr("""LEGACY_SCHEME_BOOK='TIA'"""))
    _dfs[f'{cc}_DATA_0'] = _dfs[f'{cc}_DATA_0'].drop('LEGACY_SCHEME_BOOK')
    _dfs[f'{cc}_DATA_0'].createOrReplaceTempView(f'{cc}_DATA_0')

    # Fin de l'exclusion
    _dfs[f'{cc}_DATA_1'] = spark.sql(f"""SELECT DISTINCT cover,
                    scheme,
                    Entity_CD as GL_TYPE_NO, 
                    sum(REP) as Risk_earned_prem,
                    sum(Claim_Paid) as Claim_Paid,
                    country as COUNTRY_CD,
                    Product,
                    Agent,
                    Quarter2 as Quarter
    FROM {cc}_DATA_0
    where Quarter2 = substr('{s1}',2,6) or Quarter2 = substr('{s2}',2,6)  or Quarter2 = substr('{s3}',2,6)
    or Quarter2 = substr('{s4}',2,6)  or Quarter2 = substr('{s5}',2,6)  or Quarter2 = substr('{s6}',2,6)  
    or Quarter2 = substr('{s7}',2,6)  or Quarter2 = substr('{s8}',2,6)  
    group by country, scheme,Agent, Product, cover,Entity_CD,Quarter2,Key""")
    _dfs[f'{cc}_DATA_1'].createOrReplaceTempView(f'{cc}_DATA_1')

    _dfs[f'{cc}_DATA_AGG'] = spark.sql(f"""SELECT DISTINCT country,
                    Agent,
                    Product,
                    cover,
                    Entity_CD as GL_TYPE_NO,
                    Quarter2 as Quarter,
                    Key, 
                    sum(GEP) as GEP,
                    sum(REP) as REP,
                    sum(Claim_Paid) as clms_pd
    FROM {cc}_DATA_0
    where Quarter2 = substr('{s1}',2,6) or Quarter2 = substr('{s2}',2,6)  or Quarter2 = substr('{s3}',2,6)
    or Quarter2 = substr('{s4}',2,6)  or Quarter2 = substr('{s5}',2,6)  or Quarter2 = substr('{s6}',2,6)  
    or Quarter2 = substr('{s7}',2,6)  or Quarter2 = substr('{s8}',2,6)  
    group by country, Agent, Product, cover,Entity_CD,Quarter2,Key""")
    _dfs[f'{cc}_DATA_AGG'].createOrReplaceTempView(f'{cc}_DATA_AGG')

    _dfs[f'{cc}_DATA_AGG0'] = spark.sql(f"""SELECT DISTINCT t1.cover,
                    t1.Quarter,
                    t1.Key,
                    sum(t1.REP) as REP,
                    sum(t1.clms_pd) as clms_pd,
                    t2.Group
    FROM {cc}_DATA_AGG t1
    left join WORK.COVER_DEV t2 on (t1.cover=t2.Cover_Code)
    group by Quarter,Key""")
    _dfs[f'{cc}_DATA_AGG0'].createOrReplaceTempView(f'{cc}_DATA_AGG0')

    _dfs[f'{cc}_DATA_AGG00'] = spark.table(f'{cc}_DATA_AGG0')
    _dfs[f'{cc}_DATA_AGG00'] = (_dfs[f'{cc}_DATA_AGG00']
        .withColumn('Ultimate_Liability_Factor', F.lit(0.8))
        .withColumn('REP1', F.expr("""REP * Ultimate_Liability_Factor"""))
    )
    _dfs[f'{cc}_DATA_AGG00'] = _dfs[f'{cc}_DATA_AGG00'].drop('REP', 'Ultimate_Liability_Factor')
    _dfs[f'{cc}_DATA_AGG00'] = _dfs[f'{cc}_DATA_AGG00'].withColumnRenamed('REP1', 'REP')
    _dfs[f'{cc}_DATA_AGG00'].createOrReplaceTempView(f'{cc}_DATA_AGG00')

    if {cc} = GR:
        PROC SQL;
    create table &CC._DATA_AGG1 as
    SELECT t1.*,
           t3.taux_dev                                                                       
    FROM &CC._DATA_AGG00 t1
    left join WORK.GREECE_PARAMS t3 on (t1.Quarter=t3.quarter and t1.Group=t3.Cover)
    group by Quarter,Key; 
    quit;
    # %DO block (non-iterative): %DO;
    PROC SQL;
    create table &CC._DATA_AGG1 as
    SELECT t1.*,
           t3.taux_dev
    FROM &CC._DATA_AGG00 t1
    left join WORK.ALL_PARAMS t3 on (t1.Quarter=t3.quarter and t1.Group=t3.Cover)
    group by Quarter,Key;
    quit;
    %END;
    _dfs[f'{cc}_DATA_AGG1'] = spark.table(f'{cc}_DATA_AGG1')
    _dfs[f'{cc}_DATA_AGG1'] = _dfs[f'{cc}_DATA_AGG1'].filter(~F.expr("""taux_dev IS NULL"""))
    _dfs[f'{cc}_DATA_AGG1'].createOrReplaceTempView(f'{cc}_DATA_AGG1')

    _dfs[f'{cc}_DATA_AGG1'] = spark.table(f'{cc}_DATA_AGG1')
    _dfs[f'{cc}_DATA_AGG1'] = (_dfs[f'{cc}_DATA_AGG1']
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s1}',2,6)"""), F.lit(0.96)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s1}',2,6)"""), F.lit(0.96)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s2}',2,6)"""), F.lit(0.9)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s2}',2,6)"""), F.lit(0.9)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s3}',2,6)"""), F.lit(0.84)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s3}',2,6)"""), F.lit(0.84)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s4}',2,6)"""), F.lit(0.78)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s4}',2,6)"""), F.lit(0.78)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s5}',2,6)"""), F.lit(0.72)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s5}',2,6)"""), F.lit(0.72)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s6}',2,6)"""), F.lit(0.54)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s6}',2,6)"""), F.lit(0.54)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s7}',2,6)"""), F.lit(0.36)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s7}',2,6)"""), F.lit(0.36)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKCredit CardRL101' AND Quarter = substr('{s8}',2,6)"""), F.lit(0.18)))
        .withColumn('taux_dev', F.when(F.expr(f"""Key = 'DKIKANO BANKPersonal LoanDM102' AND Quarter = substr('{s8}',2,6)"""), F.lit(0.18)))
    )
    _dfs[f'{cc}_DATA_AGG1'].createOrReplaceTempView(f'{cc}_DATA_AGG1')

    _dfs[f'{cc}_DATA_AGG2'] = spark.sql(f"""SELECT * , taux_dev*REP  as Expected_claims , REP-Expected_claims as Expected_Reserve
    FROM {cc}_DATA_AGG1
    group by Quarter,Key""")
    _dfs[f'{cc}_DATA_AGG2'].createOrReplaceTempView(f'{cc}_DATA_AGG2')

    _dfs[f'{cc}_DATA_AGG3'] = spark.sql(f"""SELECT distinct key,cover,
                    sum(Expected_claims) as Expected_claims ,
                    sum(Expected_Reserve) as Expected_Reserve,
                    sum(clms_pd) as clms_pd                                                 
    FROM {cc}_DATA_AGG2
    group by key""")
    _dfs[f'{cc}_DATA_AGG3'].createOrReplaceTempView(f'{cc}_DATA_AGG3')

    # quelques checks
    _dfs[f'{cc}_DATA_AGG2'] = spark.table(f'{cc}_DATA_AGG2').orderBy('Quarter', 'Key')
    _dfs[f'{cc}_DATA_AGG2'].createOrReplaceTempView(f'{cc}_DATA_AGG2')

    # PROC SUMMARY: SUM of ['REP', 'clms_pd'] grouped by ['Quarter', 'Key']
    _dfs[f'{cc}_check'] = _dfs[f'{cc}_DATA_AGG2'].groupBy('Quarter', 'Key').agg(F.sum('REP').alias('REP'), F.sum('clms_pd').alias('clms_pd'))
    _dfs[f'{cc}_check'].createOrReplaceTempView(f'{cc}_check')

    _dfs[f'{cc}_check1'] = spark.table(f'{cc}_check')
    _dfs[f'{cc}_check1'] = (_dfs[f'{cc}_check1']
        .withColumn('Number_quarter',
            F.when(F.expr("""clms_pd+REP=0"""), F.lit(0))
             .otherwise(F.lit(1)))
    )
    _dfs[f'{cc}_check1'].createOrReplaceTempView(f'{cc}_check1')

    _dfs[f'{cc}_data_check'] = spark.sql(f"""select key,
    sum(Number_quarter) as Number_quarter
    from {cc}_check1
    group by Key""")
    _dfs[f'{cc}_data_check'].createOrReplaceTempView(f'{cc}_data_check')

    _dfs[f'{cc}_data_check1'] = spark.sql(f"""select key,
    Number_quarter*0.0375*3 as Credib_act_exp
    from {cc}_data_check
    group by Key""")
    _dfs[f'{cc}_data_check1'].createOrReplaceTempView(f'{cc}_data_check1')

    _dfs[f'{cc}_data_check1'] = spark.table(f'{cc}_data_check1')
    _dfs[f'{cc}_data_check1'] = (_dfs[f'{cc}_data_check1']
        .withColumn('Credib_act_exp', F.when(F.expr("""key = 'NOSEB Kort ABPersonal AccidentIP101'"""), F.lit(0.1)))
    )
    _dfs[f'{cc}_data_check1'].createOrReplaceTempView(f'{cc}_data_check1')

    # fin des checks
    _dfs[f'{cc}_Actual_exp'] = spark.sql(f"""select key,
           sum(REP) as REP,
           sum(Expected_Reserve) as Expected_Reserve,
           sum(Expected_claims) as Expected_claims,
           sum(clms_pd) as clms_pd
    from {cc}_DATA_AGG2
    group by Key""")
    _dfs[f'{cc}_Actual_exp'].createOrReplaceTempView(f'{cc}_Actual_exp')

    _dfs[f'{cc}_Actual_exp'] = spark.sql(f"""select key,
           Expected_Reserve,
           clms_pd/Expected_claims as Acc_exp,
           Acc_exp*Expected_Reserve as resv_acc_act
    from {cc}_Actual_exp
    group by Key""")
    _dfs[f'{cc}_Actual_exp'].createOrReplaceTempView(f'{cc}_Actual_exp')

    _dfs[f'{cc}_final_record'] = spark.createDataFrame([], schema=StructType([]))
    _dfs[f'{cc}_final_record'].createOrReplaceTempView(f'{cc}_final_record')

    _dfs[f'RESERVES_NC_{cc}'] = spark.sql(f"""select key, 
           Expected_Reserve,
           (resv_acc_act*Credib_act_exp)+Expected_Reserve*(1-Credib_act_exp) as Res
    from {cc}_final_record
    group by Key""")
    _dfs[f'RESERVES_NC_{cc}'].createOrReplaceTempView(f'RESERVES_NC_{cc}')

    _dfs[f'{cc}_Acc_year_split'] = spark.sql(f"""select key, 
           Quarter,
           Expected_Reserve,
           sum(Expected_Reserve) as total_exp_res,
           Expected_Reserve/total_exp_res as year_split
    from {cc}_DATA_AGG2
    group by Key""")
    _dfs[f'{cc}_Acc_year_split'].createOrReplaceTempView(f'{cc}_Acc_year_split')

    # PROC TRANSPOSE
    # ID present → long-to-wide pivot
    _dfs[f'{cc}_transpos'] = _dfs[f'{cc}_Acc_year_split'].groupBy('Key').pivot('Quarter').agg(F.first(F.col('year_split')))
    _dfs[f'{cc}_transpos'].createOrReplaceTempView(f'{cc}_transpos')

    test = spark.table(f'{cc}_transpos')
    test.createOrReplaceTempView('test')

    # Split par scheme
    _dfs[f'{cc}_Data2'] = spark.sql(f"""SELECT Distinct COUNTRY_CD as country, Agent, Product, scheme, cover, GL_TYPE_NO as Entity_CD,sum(Risk_earned_prem) as REP
    from {cc}_Data_1
    GROUP BY Agent, Product, Scheme, cover, Entity_CD""")
    _dfs[f'{cc}_Data2'].createOrReplaceTempView(f'{cc}_Data2')

    _dfs[f'{cc}_Data3'] = spark.sql(f"""SELECT Distinct country, Agent, Product, scheme, cover, Entity_CD,REP, REP/sum(REP) as Scheme_Premium_Proportion 
    from {cc}_Data2 
    GROUP BY Agent, Product, cover, Entity_CD""")
    _dfs[f'{cc}_Data3'].createOrReplaceTempView(f'{cc}_Data3')

    _dfs[f'{cc}_Data3'] = spark.table(f'{cc}_Data3')
    _dfs[f'{cc}_Data3'] = (_dfs[f'{cc}_Data3']
        .withColumn('Scheme_Premium_Proportion', F.when(F.expr("""Scheme_Premium_Proportion IS NULL"""), F.lit(0)))
    )
    _dfs[f'{cc}_Data3'].createOrReplaceTempView(f'{cc}_Data3')

    _dfs[f'reserves_nc_{cc}'] = spark.table(f'RESERVES_NC_{cc}')
    _dfs[f'reserves_nc_{cc}'] = (_dfs[f'reserves_nc_{cc}']
        .withColumn('Res2', F.expr("""Res*1"""))
    )
    _dfs[f'reserves_nc_{cc}'].createOrReplaceTempView(f'reserves_nc_{cc}')

    _dfs[f'reserves_nc1_{cc}'] = spark.table('reserves_nc_')
    _dfs[f'reserves_nc1_{cc}'].createOrReplaceTempView(f'reserves_nc1_{cc}')

    _dfs[f'reserves_nc1_{cc}'] = spark.table(f'reserves_nc1_{cc}')
    # IF/THEN (manual review needed):
    #   if Key in (
    #       "SEZensumExpense ProtectionIA101" ,
    #       "SEZensumExpense ProtectionIS101" ,
    #       "SEZensumExpense ProtectionUR101" ,
    #       "SEFreedom FinanceExpense ProtectionFL102",
    #       "SEFreedom FinanceExpense ProtectionIA101",
    #       "SEFreedom FinanceExpense ProtectionIS101",
    #       "SEFreedom FinanceExpense ProtectionUR101",
    #       "SEZmartaExpense ProtectionFL102",
    #       "SEZmartaExpense ProtectionIA101",
    #       "SEZmartaExpense ProtectionIS101",
    #       "SEZmartaExpense ProtectionUR101",
    #       "SEZmartaExpense ProtectionIA102",
    #       "SEZmartaExpense ProtectionIM101",
    #       "SEZmartaExpense ProtectionIM102",
    #       "SEZmartaExpense ProtectionIS102",
    #       "SEZmartaExpense ProtectionUU101")
    #       then Res2=Res2*0.5 ;
    _dfs[f'reserves_nc1_{cc}'].createOrReplaceTempView(f'reserves_nc1_{cc}')

    _dfs[f'RESERVES_NCC_{cc}'] = spark.sql(f"""select distinct t1.*, t2.Agent,t2.Product,t2.cover,t2.GL_TYPE_NO
    from RESERVES_NC1_{cc} t1
    left join {cc}_DATA_AGG t2 on (t1.Key=t2.Key)""")
    _dfs[f'RESERVES_NCC_{cc}'].createOrReplaceTempView(f'RESERVES_NCC_{cc}')

    _dfs[f'{cc}_reserves0'] = spark.sql(f"""SELECT Distinct p.country as Country, p.Agent, p.Product, p.scheme as Scheme, p.cover as Cover, p.Entity_CD as UWCO, p.Scheme_Premium_Proportion*q.Res2 as Reserve,
    q.*
    from {cc}_Data3 p
    LEFT JOIN  reserves_ncc_{cc} q on (p.Agent=q.Agent and p.Product=q.Product and p.cover=q.Cover and p.Entity_CD=q.GL_TYPE_NO)""")
    _dfs[f'{cc}_reserves0'].createOrReplaceTempView(f'{cc}_reserves0')

    _dfs[f'{cc}_reserves0'] = spark.table(f'{cc}_reserves0')
    _dfs[f'{cc}_reserves0'] = _dfs[f'{cc}_reserves0'].filter(~F.expr("""Reserve = 0 OR Reserve IS NULL"""))
    _dfs[f'{cc}_reserves0'].createOrReplaceTempView(f'{cc}_reserves0')

    _dfs[f'{cc}_reserves0'] = spark.table(f'{cc}_reserves0')
    _dfs[f'{cc}_reserves0'] = _dfs[f'{cc}_reserves0'].drop('_NAME_', 'GL_TYPE_NO')
    _dfs[f'{cc}_reserves0'].createOrReplaceTempView(f'{cc}_reserves0')

    _dfs[f'{cc}_RESERVES1'] = spark.table(f'{cc}_RESERVES0')
    _dfs[f'{cc}_RESERVES1'] = (_dfs[f'{cc}_RESERVES1']
        .withColumn(f'{q1}', F.expr(f"""Reserve*{s1}"""))
        .withColumn(f'{q2}', F.expr(f"""Reserve*{s2}"""))
        .withColumn(f'{q3}', F.expr(f"""Reserve*{s3}"""))
        .withColumn(f'{q4}', F.expr(f"""Reserve*{s4}"""))
        .withColumn(f'{q5}', F.expr(f"""Reserve*{s5}"""))
        .withColumn(f'{q6}', F.expr(f"""Reserve*{s6}"""))
        .withColumn(f'{q7}', F.expr(f"""Reserve*{s7}"""))
        .withColumn(f'{q8}', F.expr(f"""Reserve*{s8}"""))
    )
    _dfs[f'{cc}_RESERVES1'].createOrReplaceTempView(f'{cc}_RESERVES1')

    _dfs[f'{cc}_RESERVES1'] = spark.table(f'{cc}_RESERVES1')
    _dfs[f'{cc}_RESERVES1'] = _dfs[f'{cc}_RESERVES1'].drop(f'{s1}', f'{s2}', f'{s3}', f'{s4}', f'{s5}', f'{s6}', f'{s7}', f'{s8}')
    _dfs[f'{cc}_RESERVES1'].createOrReplaceTempView(f'{cc}_RESERVES1')

    _dfs[f'{cc}_RESERVES1'] = spark.table(f'{cc}_RESERVES1').orderBy('Country', 'Agent', 'Product', 'Scheme', 'Cover', 'UWCO')
    _dfs[f'{cc}_RESERVES1'].createOrReplaceTempView(f'{cc}_RESERVES1')

    # PROC TRANSPOSE
    # wide-to-long: 8 columns -> 8 rows (_NAME_ = column name, COL1 = value)
    _dfs[f'{cc}_RESERVES2'] = _dfs[f'{cc}_RESERVES1'].select('Country', 'Agent', 'Product', 'Scheme', 'Cover', 'UWCO', F.expr(f"""stack(8, '{q1}', `{q1}`, '{q2}', `{q2}`, '{q3}', `{q3}`, '{q4}', `{q4}`, '{q5}', `{q5}`, '{q6}', `{q6}`, '{q7}', `{q7}`, '{q8}', `{q8}`) as (_NAME_, COL1)"""))
    _dfs[f'{cc}_RESERVES2'].createOrReplaceTempView(f'{cc}_RESERVES2')

    _dfs[f'{cc}_reserves_final'] = spark.table(f'{cc}_RESERVES2')
    _dfs[f'{cc}_reserves_final'] = (_dfs[f'{cc}_reserves_final']
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q1}", F.expr(f"""substring("{q1}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q2}", F.expr(f"""substring("{q2}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q3}", F.expr(f"""substring("{q3}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q4}", F.expr(f"""substring("{q4}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q5}", F.expr(f"""substring("{q5}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q6}", F.expr(f"""substring("{q6}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q7}", F.expr(f"""substring("{q7}",2,6)""")))
        .withColumn('Quarter', F.when(F.col('Quarter') == f"{q8}", F.expr(f"""substring("{q8}",2,6)""")))
        .withColumn('Acc_Yr', F.expr("""substring(Quarter,1,4)"""))
    )
    _dfs[f'{cc}_reserves_final'] = _dfs[f'{cc}_reserves_final'].filter(F.col('Reserve_bis').isNotNull())
    _dfs[f'{cc}_reserves_final'] = _dfs[f'{cc}_reserves_final'].select('Country', 'Scheme', 'Cover', 'UWCO', 'Reserve', 'Acc_Qtr', 'Acc_Yr')
    _dfs[f'{cc}_reserves_final'] = _dfs[f'{cc}_reserves_final'].withColumnRenamed('Quarter', 'Acc_Qtr')
    _dfs[f'{cc}_reserves_final'] = _dfs[f'{cc}_reserves_final'].withColumnRenamed('Reserve_bis', 'Reserve')
    _dfs[f'{cc}_reserves_final'].createOrReplaceTempView(f'{cc}_reserves_final')

    import = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/NON-CORE-COVER/Reporting/{quarter}/Accident Quarter Splits"
    database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{database}!A1').option('header', 'true').mode('overwrite').save(import)


export_excel(database=f"{cc}_reserves_final")
mend()
non_core(cc="PT")
non_core(cc="CH")
non_core(cc="FR")
non_core(cc="DE")
non_core(cc="DK")
non_core(cc="IE")
non_core(cc="GR")
# Debut traitement de la Grèce
GR_RESERVES_FINAL = spark.table('GR_RESERVES_FINAL')
GR_RESERVES_FINAL = (GR_RESERVES_FINAL
    .withColumn('Reserve', F.when(F.expr("""Country = 'GR' AND Scheme IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
)
GR_RESERVES_FINAL.createOrReplaceTempView('GR_RESERVES_FINAL')

export_excel(database="gr_reserves_final")
# Fin traitement de le Grèce
non_core(cc="PL")
non_core(cc="TR")
non_core(cc="ES")
non_core(cc="NO")
non_core(cc="SE")
non_core(cc="FI")
non_core(cc="NL")
non_core(cc="IT")
non_core(cc="UK")
# Debut traitement de UK sur le sinistre en litige sur UK : Ce sera statué en octobre 2026
# data UK_OBS;
# Country = "UK";
# Scheme = "YAD.4";
# Cover = "IE";
# UWCO = 101;
# Acc_Qtr = "2016Q2";
# Reserve = 450000;
# Acc_Yr = "2016";
# run;
# data UK_RESERVES_FINAL_TEST;
# set UK_RESERVES_FINAL;
# run;
# data UK_RESERVES_FINAL;
# set UK_RESERVES_FINAL_TEST UK_OBS;
# run;
# %EXPORT_EXCEL(DATABASE=UK_reserves_final);
# Fin traitement de UK
non_core(cc="CO")
non_core(cc="AT")
non_core(cc="MX")
non_core(cc="BE")
# data UK_RESERVES_FINAL;
# set UK_RESERVES_FINAL_TEST;
# run;
def nc_split(pays):
    # %let pays=GR;
    # RETAIN variables (initial values): {'Country': '0', 'Rsrv_Grp': '0', 'Scheme': '0', 'Scheme2': '0', 'Acc_Yr': '0', 'Acc_Yr2': '0', 'Cover': '0', 'UWCO': '0', 'Incident_Quarter': '0', 'Rsrv_Typ': '0', 'Reserve': '0'}
    _dfs[f'Non_Core_cover_{pays}_In'] = spark.table(f'{pays}_reserves_final')
    _dfs[f'Non_Core_cover_{pays}_In'] = (_dfs[f'Non_Core_cover_{pays}_In']
        .withColumn('Scheme2', F.lit(None).cast(StringType()))  # LENGTH Scheme2 $40
        .withColumn('Acc_Yr', F.expr("""substring(Acc_Qtr,1,4)*1"""))
        .withColumn('Qtr', F.expr("""substring(Acc_Qtr,5,2)"""))
        .withColumn('Incident_Quarter', F.expr("""concat(Acc_Yr,Qtr)"""))
        .withColumn('Rsrv_Grp', F.lit('ZZ1'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Scheme2', F.col('Scheme'))
        .withColumn('Acc_Yr2', F.expr("""Acc_Yr*1"""))
    )
    _dfs[f'Non_Core_cover_{pays}_In'] = _dfs[f'Non_Core_cover_{pays}_In'].select('Country', 'Rsrv_Grp', 'Scheme', 'Scheme2', 'Acc_Yr', 'Acc_Yr2', 'Cover', 'UWCO', 'Reserve', 'Incident_Quarter', 'Rsrv_Typ')
    _dfs[f'Non_Core_cover_{pays}_In'] = _dfs[f'Non_Core_cover_{pays}_In'].withColumnRenamed('UWCO', 'Entity_CD')
    _dfs[f'Non_Core_cover_{pays}_In'].createOrReplaceTempView(f'Non_Core_cover_{pays}_In')

    _dfs[f'Non_Core_cover_{pays}_0'] = spark.sql(f"""SELECT distinct country,
                    Rsrv_Grp,
                    Scheme2 as Scheme,
                    Cover,
                    Entity_CD,
                    Acc_Yr2 as SURV,
                    Incident_Quarter, 
                    Rsrv_Typ,
                    sum(Reserve) AS Reserve
    FROM Non_Core_cover_{pays}_In
    group by country,Rsrv_Grp,Scheme2,Cover,Entity_CD,Incident_Quarter,Rsrv_Typ""")
    _dfs[f'Non_Core_cover_{pays}_0'].createOrReplaceTempView(f'Non_Core_cover_{pays}_0')

    _dfs[f'NC_withplyr_{pays}'] = spark.table(f'{ouput}.CLMHDR_ALL_{pays}')
    _dfs[f'NC_withplyr_{pays}'] = _dfs[f'NC_withplyr_{pays}'].filter(F.expr(f"""country='{pays}' AND Rsrv_Grp IN ('ZZ1')"""))
    _dfs[f'NC_withplyr_{pays}'] = (_dfs[f'NC_withplyr_{pays}']
        .withColumn('Cohort', F.expr("""year(Incptn_Dt)"""))
        .withColumn('Entity_CD', F.expr("""Undrwrtng_Cmpny*1"""))
    )
    _dfs[f'NC_withplyr_{pays}'] = _dfs[f'NC_withplyr_{pays}'].select('Country', 'Cvr_Typ', 'Schm', 'Undrwrtng_Cmpny', 'Entity_CD', 'Cohort', 'Acc_Yr', 'Totl_Amnt_Pd')
    _dfs[f'NC_withplyr_{pays}'] = _dfs[f'NC_withplyr_{pays}'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'NC_withplyr_{pays}'] = _dfs[f'NC_withplyr_{pays}'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'NC_withplyr_{pays}'].createOrReplaceTempView(f'NC_withplyr_{pays}')

    _dfs[f'NC_withplyr_{pays}'] = spark.table(f'NC_withplyr_{pays}').orderBy('Scheme', 'Cover', 'Entity_CD', 'Acc_Yr', 'Cohort')
    _dfs[f'NC_withplyr_{pays}'] = _dfs[f'NC_withplyr_{pays}'].dropDuplicates(['Scheme', 'Cover', 'Entity_CD', 'Acc_Yr', 'Cohort'])
    _dfs[f'NC_withplyr_{pays}'].createOrReplaceTempView(f'NC_withplyr_{pays}')

    _dfs[f'SUM_NC_1_{pays}'] = spark.sql(f"""SELECT country,
                 Scheme,
                 Cover,
                 Entity_CD,
                 Cohort,
                 Acc_yr AS SURV,            
                 sum(Totl_Amnt_Pd) AS MONTANT
         FROM    NC_withplyr_{pays}
         
         group by Scheme,Cover,Entity_CD,Cohort,Acc_yr""")
    _dfs[f'SUM_NC_1_{pays}'].createOrReplaceTempView(f'SUM_NC_1_{pays}')

    _dfs[f'SUM_NC_2_{pays}'] = spark.sql(f"""SELECT DISTINCT country,
                 Scheme,
                 Cover,
                 Entity_CD,
                 Acc_yr AS SURV,             
                 sum(Totl_Amnt_Pd) AS MONTANT2
         FROM    NC_withplyr_{pays}
         
         group by Scheme,Cover,Entity_CD,Acc_yr""")
    _dfs[f'SUM_NC_2_{pays}'].createOrReplaceTempView(f'SUM_NC_2_{pays}')

    _dfs[f'SUM_NC_3_{pays}'] = spark.sql(f"""select 
    t1.*,
    t2.MONTANT2 as MONTANT2
    from SUM_NC_1_{pays} t1 
    left join SUM_NC_2_{pays} t2 on (t1.Scheme=t2.Scheme and t1.Cover=t2.Cover and t1.Entity_CD=t2.Entity_CD and t1.SURV=t2.SURV) """)
    _dfs[f'SUM_NC_3_{pays}'].createOrReplaceTempView(f'SUM_NC_3_{pays}')

    _dfs[f'SUM_NC_4_{pays}'] = spark.table(f'SUM_NC_3_{pays}')
    _dfs[f'SUM_NC_4_{pays}'] = (_dfs[f'SUM_NC_4_{pays}']
        .withColumn('weight', F.expr("""MONTANT/MONTANT2"""))
    )
    _dfs[f'SUM_NC_4_{pays}'] = _dfs[f'SUM_NC_4_{pays}'].filter(~F.expr("""weight IS NULL"""))
    _dfs[f'SUM_NC_4_{pays}'] = _dfs[f'SUM_NC_4_{pays}'].drop('MONTANT', 'MONTANT2')
    _dfs[f'SUM_NC_4_{pays}'].createOrReplaceTempView(f'SUM_NC_4_{pays}')

    _dfs[f'NON_CORE_COVER_{pays}_1'] = spark.sql(f"""select 
    t1.*,
    t2.Cohort,
    t2.weight as weight
    from Non_Core_cover_{pays}_0 t1 
    left join SUM_NC_4_{pays} t2 on (t1.Scheme=t2.Scheme and t1.Cover=t2.Cover and t1.Entity_CD=t2.Entity_CD and t1.SURV=t2.SURV) """)
    _dfs[f'NON_CORE_COVER_{pays}_1'].createOrReplaceTempView(f'NON_CORE_COVER_{pays}_1')

    # RETAIN variables (initial values): {'Country': '0', 'Rsrv_Grp': '0', 'Scheme': '0', 'Cohort': '0', 'Cover': '0', 'Entity_CD': '0', 'Incident_Quarter': '0', 'Date_of_reserving': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt': '0'}
    _dfs[f'NON_CORE_COVER_{pays}'] = spark.table(f'NON_CORE_COVER_{pays}_1')
    _dfs[f'NON_CORE_COVER_{pays}'] = (_dfs[f'NON_CORE_COVER_{pays}']
        .withColumn('Cohort', F.when(F.expr("""Cohort IS NULL"""), F.col('SURV')))
        .withColumn('weight', F.when(F.expr("""weight IS NULL"""), F.lit(1)))
        .withColumn('Rsrv_Amt', F.expr("""weight*Reserve"""))
        .withColumn('Date_of_reserving', F.lit(f'{quarter}'))
    )
    _dfs[f'NON_CORE_COVER_{pays}'] = _dfs[f'NON_CORE_COVER_{pays}'].select('Country', 'Rsrv_Grp', 'Scheme', 'Cover', 'Entity_CD', 'Incident_Quarter', 'Cohort', 'Rsrv_Amt', 'Rsrv_Typ', 'Date_of_reserving')
    _dfs[f'NON_CORE_COVER_{pays}'] = _dfs[f'NON_CORE_COVER_{pays}'].withColumnRenamed('Cohort', 'Vintage_year')
    _dfs[f'NON_CORE_COVER_{pays}'].createOrReplaceTempView(f'NON_CORE_COVER_{pays}')

    _dfs[f'NON_CORE_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Vintage_year,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Date_of_reserving,
                        Rsrv_Typ,
                        sum(Rsrv_Amt) as Rsrv_Amt                                      
         FROM    NON_CORE_COVER_{pays}
         group by country, Scheme,Rsrv_Grp, cover,Vintage_year,Entity_CD,Incident_Quarter,Date_of_reserving,Rsrv_Typ
          """)
    _dfs[f'NON_CORE_{pays}'].createOrReplaceTempView(f'NON_CORE_{pays}')

    _dfs[f'RESERVES_NC_{pays}'] = spark.table(f'NON_CORE_{pays}')
    _dfs[f'RESERVES_NC_{pays}'].createOrReplaceTempView(f'RESERVES_NC_{pays}')

    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Entity_Mappings.xlsx"
    import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Reassurance.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_02, out="Entity_Mappings", onglet="Entity_Mappings")
import_excel(file=import_01, out="Parametres_Reas", onglet="Parametres_Reas")
Parametres_Reas = spark.table('Parametres_Reas')
Parametres_Reas.createOrReplaceTempView('Parametres_Reas')

_dfs[f'CLMHDR_ALL_{pays}'] = spark.table(f'{ouput}.CLMHDR_ALL_{pays}')
_dfs[f'CLMHDR_ALL_{pays}'] = (_dfs[f'CLMHDR_ALL_{pays}']
    .withColumn('Entity_CD', F.expr("""Undrwrtng_Cmpny*1"""))
)
_dfs[f'CLMHDR_ALL_{pays}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}')

PARAMETRES_REAS = spark.table('PARAMETRES_REAS')
PARAMETRES_REAS = (PARAMETRES_REAS
    .withColumn('Entity_CD', F.expr("""Original_underwritter*1"""))
)
PARAMETRES_REAS.createOrReplaceTempView('PARAMETRES_REAS')

_dfs[f'RESERVES_NC_{pays}'] = spark.sql(f"""select distinct
t1.*,
t8.QP_rei_CLAIM AS QP_rei_CLAIM,
t3.Legal_Entity as Entity
from RESERVES_NC_{pays}  t1 
left join Parametres_Reas t8 on (t1.country=t8.country AND t1.scheme=t8.scheme AND t1.cover=t8.cover AND t1.Entity_CD=t8.Entity_CD) 
left join CLMHDR_ALL_{pays} t3 on (t1.country=t3.country AND t1.Scheme=t3.Schm AND t1.Cover=t3.Cvr_Typ AND t1.Entity_CD=t3.Entity_CD) 
 
 """)
_dfs[f'RESERVES_NC_{pays}'].createOrReplaceTempView(f'RESERVES_NC_{pays}')

_dfs[f'RESERVES_NC_{pays}'] = spark.table(f'RESERVES_NC_{pays}')
_dfs[f'RESERVES_NC_{pays}'] = (_dfs[f'RESERVES_NC_{pays}']
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM IS NULL"""), F.lit(0)))
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM NOT IN (.)"""), F.lit(4)))
    .withColumn('QP_rei_CLAIM', F.when(F.expr("""Type_Insurance=0"""), F.lit(1)))
)
_dfs[f'RESERVES_NC_{pays}'] = _dfs[f'RESERVES_NC_{pays}'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_Gross')
_dfs[f'RESERVES_NC_{pays}'].createOrReplaceTempView(f'RESERVES_NC_{pays}')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'Scheme': '0', 'Type_Insurance': '0', 'Cover': '0', 'Entity_CD': '0', 'Entity': '0', 'Incident_Quarter': '0', 'Vintage_year': '0', 'Date_of_reserving': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0'}
_dfs[f'RESERVES_NC_{pays}'] = spark.table(f'RESERVES_NC_{pays}')
_dfs[f'RESERVES_NC_{pays}'] = (_dfs[f'RESERVES_NC_{pays}']
    .withColumn('Rsrv_Amt_Net', F.expr("""Rsrv_Amt_Gross*QP_rei_CLAIM"""))
)
_dfs[f'RESERVES_NC_{pays}'] = _dfs[f'RESERVES_NC_{pays}'].select('country', 'Rsrv_Grp', 'Scheme', 'Type_Insurance', 'Cover', 'Entity_CD', 'Entity', 'Incident_Quarter', 'Vintage_year', 'Date_of_reserving', 'Rsrv_Typ', 'Rsrv_Amt_Gross', 'Rsrv_Amt_Net')
_dfs[f'RESERVES_NC_{pays}'].createOrReplaceTempView(f'RESERVES_NC_{pays}')

_dfs[f'RESERVES_NC_{pays}'] = spark.table(f'RESERVES_NC_{pays}').orderBy('country', 'Rsrv_Grp', 'Scheme', 'Type_Insurance', 'Cover', 'Entity_CD', 'Entity', 'Incident_Quarter', 'Vintage_year', 'Date_of_reserving', 'Rsrv_Typ')
_dfs[f'RESERVES_NC_{pays}'] = _dfs[f'RESERVES_NC_{pays}'].dropDuplicates(['country', 'Rsrv_Grp', 'Scheme', 'Type_Insurance', 'Cover', 'Entity_CD', 'Entity', 'Incident_Quarter', 'Vintage_year', 'Date_of_reserving', 'Rsrv_Typ'])
_dfs[f'RESERVES_NC_{pays}'].createOrReplaceTempView(f'RESERVES_NC_{pays}')

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations
spark.catalog.dropTempView('PARAMETRES_REAS')  # DELETE PARAMETRES_REAS

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

mend()
nc_split(pays="DE")
nc_split(pays="DK")
nc_split(pays="IE")
nc_split(pays="IT")
nc_split(pays="FI")
nc_split(pays="FR")
nc_split(pays="GR")
nc_split(pays="ES")
nc_split(pays="NO")
nc_split(pays="PL")
nc_split(pays="PT")
nc_split(pays="SE")
nc_split(pays="TR")
nc_split(pays="UK")
# Debut traitement de UK sur le sinistre en litige sur UK : Ce sera statué en octobre 2026
# data UK_OBSERVATION;
# country="UK";
# Rsrv_Grp="ZZ1";
# Scheme="YAD.4";
# Type_Insurance=0;
# Cover="IE";
# Entity_CD=101;
# Entity="FICL";
# Incident_Quarter="2016Q2";
# Vintage_year=2016;
# Date_of_reserving="2025Q3";
# Rsrv_Typ="IBNR";
# Rsrv_Amt_Gross=450000;
# Rsrv_Amt_Net=450000;
# run;
# data &Ouput..RESERVES_NC_UK;
# set &Ouput..RESERVES_NC_UK UK_OBSERVATION ;
# run;
# Fin traitement de UK
nc_split(pays="CH")
nc_split(pays="CO")
nc_split(pays="AT")
# RETAIN variables (initial values): {'country': '0', 'Scheme': '0', 'Rsrv_Grp': '0', 'Type_Insurance': '0', 'Cover': '0', 'Entity_CD': '0', 'Entity': '0', 'Incident_Quarter': '0', 'Vintage_year': '0', 'Date_of_reserving': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0'}
from functools import reduce
RESERVES_NC_ALL = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{ouput}.RESERVES_NC_PT'), spark.table(f'{ouput}.RESERVES_NC_DE'), spark.table(f'{ouput}.RESERVES_NC_DK'), spark.table(f'{ouput}.RESERVES_NC_ES'), spark.table(f'{ouput}.RESERVES_NC_IE'), spark.table(f'{ouput}.RESERVES_NC_FI'), spark.table(f'{ouput}.RESERVES_NC_FR'), spark.table(f'{ouput}.RESERVES_NC_GR'), spark.table(f'{ouput}.RESERVES_NC_IT'), spark.table(f'{ouput}.RESERVES_NC_NO'), spark.table(f'{ouput}.RESERVES_NC_PL'), spark.table(f'{ouput}.RESERVES_NC_SE'), spark.table(f'{ouput}.RESERVES_NC_TR'), spark.table(f'{ouput}.RESERVES_NC_UK'), spark.table(f'{ouput}.RESERVES_NC_CH'), spark.table(f'{ouput}.RESERVES_NC_CO'), spark.table(f'{ouput}.RESERVES_NC_AT')])
RESERVES_NC_ALL = (RESERVES_NC_ALL
    .withColumn('Entity', F.when(F.expr("""Entity_CD IN (862) AND cover IN ('RU','DS')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='FI' AND Entity_CD IN (802) AND Entity=''"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='ES' AND Entity_CD IN (821,861,901,911) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='ES' AND Entity_CD IN (902) AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='SE' AND Entity_CD IN (952,902) AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='SE' AND Entity_CD IN (912) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='SE' AND Entity_CD IN (911,951,901) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='TR' AND Entity_CD IN (701) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='NO' AND Entity_CD IN (951,952,912) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='NO' AND Entity_CD IN (911) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='ES' AND Entity_CD IN (912) AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='PL' AND Entity_CD IN (931,911) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='PL' AND Entity_CD IN (912) AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='PT' AND Entity_CD IN (921,951,911,851) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='PT' AND Entity_CD IN (912) AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='FR' AND Entity_CD IN (502) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='TR' AND Entity_CD IN (502,501) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='UK' AND Entity_CD IN (821,831,872) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='UK' AND Entity_CD IN (911,901,131,141,971) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='UK' AND Entity_CD IN ('132','912','902','982','972') AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='DE' AND Entity_CD IN (502,501) AND Entity IN ('','N/A')"""), F.lit('TPA')))
    .withColumn('Entity', F.when(F.expr("""country='CH' AND Entity_CD IN (911) AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""Type_Insurance IN (0,11) AND Entity_CD IN ('101','121','181') AND Entity IN ('','N/A')"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""Type_Insurance IN (0,11) AND Entity_CD IN ('102','122','182') AND Entity IN ('','N/A')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='DK' AND Entity_CD IN (952) AND Entity=''"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='DK' AND Entity_CD IN (951) AND Entity=''"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='GR' AND Entity_CD IN (811,841) AND Entity=''"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='IT' AND Entity_CD IN (821,791,931) AND Entity=''"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='PT' AND Entity_CD IN (931,831,991) AND Entity=''"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""country='TR' AND Entity_CD IN (982) AND Entity=''"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country='TR' AND Entity_CD IN (811,831) AND Entity=''"""), F.lit('FICL')))
)
RESERVES_NC_ALL.createOrReplaceTempView('RESERVES_NC_ALL')

_dfs[f'WPS_DAAP_NCC_{yr}{month}{day}'] = spark.table('RESERVES_NC_ALL')
_dfs[f'WPS_DAAP_NCC_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_NCC_{yr}{month}{day}']
    .withColumn('Entity', F.when(F.expr("""Entity_CD IN (911,951,851,801,861,871,671,811,881,981) AND Entity=''"""), F.lit('FICL')))
    .withColumn('Entity', F.when(F.expr("""Entity_CD IN (812,842,872,932,992) AND Entity=''"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""country = 'CO' AND Entity=''"""), F.lit('FICL')))
)
_dfs[f'WPS_DAAP_NCC_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_NCC_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.WPS_DAAP_NCC_{yr}{month}{day}
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {ouput}')
_dfs[f'WPS_DAAP_NCC_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.WPS_DAAP_NCC_{yr}{month}{day}')

# PROC DATASETS → Spark table operations
spark.catalog.dropTempView('RESERVES_NC_ALL')  # DELETE RESERVES_NC_ALL
