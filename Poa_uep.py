from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_04_V2"
arrete3 = "Q126"
fichier_import = "Q1 26 POA File  - DAAP Team"
n = 2026
tia_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
# ########################### POA NL ###############################
# ############################################################################
map_rgpt = spark.sql("""Select distinct country, SCHEME, RGPT
from tia.uep_clot_tia_nl""")
map_rgpt.createOrReplaceTempView('map_rgpt')

def import_excelx(file, out, onglet):
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', f"'{onglet}'!J3:O30")
        .option('header', 'true')
        .option('inferSchema', 'false')
        .load(file))
    _df_tmp.createOrReplaceTempView(f'{out}')

    _dfs[f'{out}'] = spark.sql(f"""Select distinct t1.*,'NL' as country ,{n}-1 as  GEN , {n}-1 as SURV ,t2.RGPT
    From  {out} t1
    left join map_rgpt t2 on (t1.SCHEME=t2.SCHEME) """)
    _dfs[f'{out}'].createOrReplaceTempView(f'{out}')


import_05 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/POA/{fichier_import}.xlsx"
import_excelx(file=import_05, out=f"BGD_UEP_DAC_{arrete3}_NL_poa", onglet=arrete3)
def uep_dac(poste):
    _dfs[f'{poste}_RU'] = spark.sql(f"""Select country ,SCHEME , 'RU' as cover, 101 as entity_cd, 'FICL' as entity ,0 as Type_Insurance, 'EUR' as currency_code ,GEN ,SURV ,RGPT ,'{poste}' as POSTE, {poste}_RU as NET_AMT
    	From  BGD_UEP_DAC_{arrete3}_NL_poa """)
    _dfs[f'{poste}_RU'].createOrReplaceTempView(f'{poste}_RU')

    _dfs[f'{poste}_DA'] = spark.sql(f"""Select country ,SCHEME , 'DA' as cover, 101 as entity_cd, 'FICL' as entity ,0 as Type_Insurance, 'EUR' as currency_code ,GEN ,SURV ,RGPT ,'{poste}' as POSTE, {poste}_DA as NET_AMT
    	From  BGD_UEP_DAC_{arrete3}_NL_poa """)
    _dfs[f'{poste}_DA'].createOrReplaceTempView(f'{poste}_DA')

    _dfs[f'{poste}_CLOT_TIA_NL_poa'] = spark.table(f'{poste}_RU').unionByName(spark.table(f'{poste}_DA'), allowMissingColumns=True)
    _dfs[f'{poste}_CLOT_TIA_NL_poa'].createOrReplaceTempView(f'{poste}_CLOT_TIA_NL_poa')


uep_dac("UEP")
uep_dac("DAC")
UEP_CLOT_TIA_NL_2 = spark.table('tia.uep_clot_tia_nl').unionByName(spark.table('uep_clot_tia_nl_poa'), allowMissingColumns=True)
UEP_CLOT_TIA_NL_2.createOrReplaceTempView('UEP_CLOT_TIA_NL_2')
# LIBNAME TIA -> base Spark: tia.UEP_CLOT_TIA_NL_2
UEP_CLOT_TIA_NL_2.write.mode('overwrite').saveAsTable('tia.UEP_CLOT_TIA_NL_2')

DAC_CLOT_TIA_NL_2 = spark.table('tia.dac_clot_tia_nl').unionByName(spark.table('dac_clot_tia_nl_poa'), allowMissingColumns=True)
DAC_CLOT_TIA_NL_2.createOrReplaceTempView('DAC_CLOT_TIA_NL_2')
# LIBNAME TIA -> base Spark: tia.DAC_CLOT_TIA_NL_2
DAC_CLOT_TIA_NL_2.write.mode('overwrite').saveAsTable('tia.DAC_CLOT_TIA_NL_2')
