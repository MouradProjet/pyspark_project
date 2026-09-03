from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #####################################################################################################################################################################################
# ##########################################################       Creation des fichiers du Datalake    ######################################################################################
# #####################################################################################################################################################################################
# ######## Name: MODUL RESERVING CLP
# ######## Author: ALSENY SOW
# ######## Date started :26/06/2018
# ######## Date finished:10/07/2018
# ######## Context:
# #####################################################  CREATION DES LIBRARY  #########################################################################################
lreseau = "~/NAS/X"
# Mettre le serveur approprié  entre -> ~/NAS/X  ou -> X:/Inventprev **
arrete = "2026_06_Prov"
ouput = "CR_Q226"
month = 06
day = 26
yr = 2026
input_path = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Input"  # LIBNAME input
# ###################################################  CASES RESERVES  #########################################################################################
from functools import reduce
clmhdr_all_countries = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{ouput}.clmhdr_all_FR'), spark.table(f'{ouput}.clmhdr_all_FI'), spark.table(f'{ouput}.clmhdr_all_CH'), spark.table(f'{ouput}.clmhdr_all_DE'), spark.table(f'{ouput}.clmhdr_all_DK'), spark.table(f'{ouput}.clmhdr_all_ES'), spark.table(f'{ouput}.clmhdr_all_IE'), spark.table(f'{ouput}.clmhdr_all_IT'), spark.table(f'{ouput}.clmhdr_all_NI'), spark.table(f'{ouput}.clmhdr_all_NL'), spark.table(f'{ouput}.clmhdr_all_NO'), spark.table(f'{ouput}.clmhdr_all_GR'), spark.table(f'{ouput}.clmhdr_all_PL'), spark.table(f'{ouput}.clmhdr_all_PT'), spark.table(f'{ouput}.clmhdr_all_NO'), spark.table(f'{ouput}.clmhdr_all_SE'), spark.table(f'{ouput}.clmhdr_all_TR'), spark.table(f'{ouput}.clmhdr_all_UK'), spark.table(f'{ouput}.clmhdr_all_AT'), spark.table(f'{ouput}.clmhdr_all_BE'), spark.table(f'{ouput}.clmhdr_all_CO'), spark.table(f'{ouput}.clmhdr_all_MX')])
clmhdr_all_countries.createOrReplaceTempView('clmhdr_all_countries')
# LIBNAME {ouput} -> base Spark: {ouput}.clmhdr_all_countries
clmhdr_all_countries.write.mode('overwrite').saveAsTable(f'{ouput}.clmhdr_all_countries')

from functools import reduce
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{ouput}.clmhdr_all_FR_CR'), spark.table(f'{ouput}.clmhdr_all_FI_CR'), spark.table(f'{ouput}.clmhdr_all_DE_CR'), spark.table(f'{ouput}.clmhdr_all_DK_CR'), spark.table(f'{ouput}.clmhdr_all_ES_CR'), spark.table(f'{ouput}.clmhdr_all_IE_CR'), spark.table(f'{ouput}.clmhdr_all_IT_CR'), spark.table(f'{ouput}.clmhdr_all_NI_CR'), spark.table(f'{ouput}.clmhdr_all_NL_CR'), spark.table(f'{ouput}.clmhdr_all_NO_CR'), spark.table(f'{ouput}.clmhdr_all_GR_CR'), spark.table(f'{ouput}.clmhdr_all_PL_CR'), spark.table(f'{ouput}.clmhdr_all_PT_CR'), spark.table(f'{ouput}.clmhdr_all_NO_CR'), spark.table(f'{ouput}.clmhdr_all_SE_CR'), spark.table(f'{ouput}.clmhdr_all_TR_CR'), spark.table(f'{ouput}.clmhdr_all_UK_CR'), spark.table(f'{ouput}.clmhdr_all_CH_CR'), spark.table(f'{ouput}.clmhdr_all_AT_CR'), spark.table(f'{ouput}.clmhdr_all_CO_CR'), spark.table(f'{ouput}.clmhdr_all_BE_CR'), spark.table(f'{ouput}.clmhdr_all_MX_CR')])
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].createOrReplaceTempView(f'wps_daap_case_reserves_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.wps_daap_case_reserves_{yr}{month}{day}
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')

_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = spark.sql(f"""SELECT DISTINCT *                                 
FROM {ouput}.wps_daap_case_reserves_{yr}{month}{day}
group by country, Clm_Nmbr""")
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].createOrReplaceTempView(f'wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')

import_01 = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', f"'{onglet}'!A1")
        .option('header', 'true')
        .load(file))
    _df_tmp.createOrReplaceTempView(f'{out}')


import_excel(file=import_01, out="flag_legacy", onglet="flag_legacy")
# Correction du mapping MACAO vs TIA
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = spark.sql(f"""select distinct
t1.*,
t8.RPP AS RPP,
t8.Flag_Macao AS Flag_Macao
from {ouput}.wps_daap_case_reserves_{yr}{month}{day}  t1 
left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.SCHEME=t8.scheme and t1.cover=t8.Cover) """)
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].createOrReplaceTempView(f'wps_daap_case_reserves_{yr}{month}{day}')

_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = spark.table(f'wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = (_dfs[f'wps_daap_case_reserves_{yr}{month}{day}']
    .withColumn('LEGACY_SCHEME_BOOK',
    F.when(F.expr("""Flag_Macao IN ('MACAO')"""), F.lit('MACAO'))
     .when(F.expr("""Flag_Macao  IN ('TIA', '')"""), F.lit('TIA'))
     .otherwise(F.col('LEGACY_SCHEME_BOOK')))
)
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].createOrReplaceTempView(f'wps_daap_case_reserves_{yr}{month}{day}')

_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = spark.table(f'wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'] = _dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].drop('informer_type', 'RPP', 'Flag_Macao')
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].createOrReplaceTempView(f'wps_daap_case_reserves_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.wps_daap_case_reserves_{yr}{month}{day}
_dfs[f'wps_daap_case_reserves_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')

_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'] = spark.table(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'].filter(F.expr("""country NOT IN ('CH') AND LEGACY_SCHEME_BOOK='TIA'"""))
# la DAAP n'est pas responsable du calcul de la suisse, pour les autres c'est encore du off-system et nous n'avons pas le sign-off de l'IT (données non fiables)
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'].filter((F.col('Country').isNotNull() & (F.col('Country') != '')))
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'].drop('LEGACY_SCHEME_BOOK')
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}')

# les tables n'ont pas été qualibrées pour ces pays, pour l'instant on prend les hypothèses de l'allemagne -> à mettre à jour
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'] = spark.sql(f"""SELECT 		
		 country,
		 Rsrv_Grp,
		 Clm_Nmbr,
		 Type_Insurance,
		 SCHEME ,
		 cover,
		 Entity_CD,
		 Entity,
		 Incident_date,
		 Rgstrtn_Dt,
		 Latst_Bnft_Pd_Yr,
		 Latst_Bnft_Pd_Mnth,
		 Vintage_year,
		 Date_of_reserving,
		 STATUS,
		 Totl_Amnt_Pd_Gross,
		 Totl_Amnt_Pd_Net,
		 Totl_Bnfts_Amnt_Pd_Gross,
		 Totl_Bnfts_Amnt_Pd_Net,
		 Probablty_Accptd,
		 Nmbr_Bnfts_Pd,
		 Nmbr_Bnfts_Otstndng,
		 Rsrv_Typ,
		 Rsrv_Amt_Gross,
		 Rsrv_Amt_Net		 		 
		FROM WPS_DAAP_CASE_RESERVES_{yr}{month}{day} 
		GROUP BY country,Clm_Nmbr,SCHEME,cover """)
_dfs[f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_CASE_RESERVES_{yr}{month}{day}')

# #####################################################  IBNR et NCC  #########################################################################################
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR'].filter(F.expr("""Rsrv_Grp != 'ZZ1'"""))
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR')
# LIBNAME {ouput} -> base Spark: {ouput}.WPS_DAAP_IBNR_{yr}{month}{day}_ACR
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}_ACR'].write.mode('overwrite').saveAsTable(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}_ACR')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'Scheme': '0', 'Type_Insurance': '0', 'Cover': '0', 'Entity_CD2': '0', 'Entity': '0', 'Incident_Quarter': '0', 'Vintage_year': '0', 'Date_of_reserving': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0'}
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}_ACR')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Entity_CD2', F.expr("""Entity_CD*1"""))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].drop('Entity_CD')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].withColumnRenamed('Entity_CD2', 'Entity_CD')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}').unionByName(spark.table(f'{ouput}.WPS_DAAP_NCC_{yr}{month}{day}'), allowMissingColumns=True)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Vintage_year',
    F.when(F.expr("""Vintage_year=''"""), F.lit(9999))
     .when(F.expr("""Vintage_year IN (2026, 5015,2918,2077,2088,3007)"""), F.lit(9999))
     .otherwise(F.col('Vintage_year')))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.WPS_DAAP_IBNR_{yr}{month}{day}
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('date_of_reserving', F.when(F.col('date_of_reserving') == f" {yr}substr({arrete},9,2)", F.lit(f'substring({arrete},9,2){yr}')).otherwise(F.col('date_of_reserving')))
    .withColumn('country', F.when(F.expr("""country = ''"""), F.lit('ES')).otherwise(F.col('country')))
    .withColumn('entity', F.when(F.expr("""country = 'ES' AND entity = ''"""), F.lit('FICL')).otherwise(F.col('entity')))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.WPS_DAAP_IBNR_{yr}{month}{day}
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].filter(F.expr("""country != 'CH'"""))
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.sql(f"""SELECT DISTINCT *                                 
FROM    WPS_DAAP_IBNR_{yr}{month}{day}
group by country, Scheme, Type_Insurance, Cover, Entity_CD, Entity, Incident_Quarter, Vintage_year""")
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

import_01 = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2021_11_Q4/02_Elements_Techniques/TIA/Extraction Donnees/20210408 List TIA scheme exclusion - C and TPA.xlsx"
def import_excel(file, out, onglet):
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', f"'{onglet}'!A1")
        .option('header', 'true')
        .load(file))
    _df_tmp.createOrReplaceTempView(f'{out}')


import_excel(file=import_01, out="flag_double", onglet="Sheet1")
FLAG_DOUBLE = spark.table('FLAG_DOUBLE')
FLAG_DOUBLE.createOrReplaceTempView('FLAG_DOUBLE')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.sql(f"""select
t1.*,
t7.RI_inwards_cash_matching AS flag_web
from WPS_DAAP_IBNR_{yr}{month}{day} t1
left join flag_double t7 on (t1.Country=t7.Country_CD AND t1.SCHEME=t7.contract_id_version)""")
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('LEGACY_WEBXL_BOOK',
        F.when(F.expr("""cats(flag_web) IN ('','N/A')"""), F.lit('TIA'))
         .otherwise(F.lit('WEBXL')))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].filter(F.expr("""LEGACY_WEBXL_BOOK='TIA'"""))
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].drop('LEGACY_WEBXL_BOOK', 'flag_web')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

# EXPORT DES BASES POUR LE SHAREPOINT
chemin = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/REPORTING/Datalake"
def export_excel(database):
    database.write.csv(chemin, header=True, mode='overwrite')


export_excel(database=f"wps_daap_case_reserves_{yr}{month}{day}")
export_excel(database=f"WPS_DAAP_IBNR_{yr}{month}{day}")
WPS_DAAP_IBNR_{yr}{month}{day}.write.format('com.crealytics.spark.excel').option('dataAddress', f'{yr}{month}{day}!A1').option('header', 'true').mode('overwrite').save(chemin)
