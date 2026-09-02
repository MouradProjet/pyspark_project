from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

lreseau = "X"
arrete = "2026_04_V2"
ouput = "CR_Q126"
month = 03
day = 27
yr = 2026
quarter = "Q12026"
def moduleresults(pays):
    # *Q ICOP SMRY RESULTS;
    _dfs[f'ICOP_SMRY_RESULTS_{pays}'] = spark.sql(f"""SELECT DISTINCT country,Rsrv_Grp as Rsrv_Grp, Clm_Nmbr, Rsrv_Amt, Probablty_Otstndng, Nmbr_Bnfts_Otstndng, Rsrv_Typ
    	FROM {ouput}.CLMHDR_ALL_{pays}
    	WHERE Rsrv_Amt > 1 AND RSRV_TYP = 'ICOP' and LEGACY_SCHEME_BOOK='TIA' """)
    _dfs[f'ICOP_SMRY_RESULTS_{pays}'].createOrReplaceTempView(f'ICOP_SMRY_RESULTS_{pays}')

    _dfs[f'ICOP_SMRY_RESULTS_{pays}'] = spark.sql(f"""SELECT country,Rsrv_Grp, sum(Rsrv_Amt) as Rsrv_Amt, sum(Probablty_Otstndng) as Effective_Number, 
    	Sum(Nmbr_Bnfts_Otstndng)as NOXMTHSOS, count(Clm_Nmbr) as Actual_Nmbr, Rsrv_Typ
    	FROM ICOP_SMRY_RESULTS_{pays}
    	WHERE Rsrv_Amt > 1 AND RSRV_TYP = 'ICOP' 
    	GROUP BY country,Rsrv_Grp, Rsrv_Typ""")
    _dfs[f'ICOP_SMRY_RESULTS_{pays}'].createOrReplaceTempView(f'ICOP_SMRY_RESULTS_{pays}')

    _dfs[f'RBNP_SMRY_RESULTS_{pays}'] = spark.sql(f"""SELECT country,Rsrv_Grp, sum(Rsrv_Amt) as Rsrv_Amt, sum(Probablty_Otstndng) as Eff_Nmbr_Otstndng, 
    	Sum(Probablty_Otstndng*Nmbr_Bnfts_Otstndng) as ACCXMTHSOS, count(Clm_Nmbr) as Nmbr_Otstndng, Rsrv_Typ
    	FROM {ouput}.CLMHDR_ALL_{pays}
    	WHERE RSRV_TYP = 'RBNP' and LEGACY_SCHEME_BOOK='TIA'
    	GROUP BY country,Rsrv_Grp, Rsrv_Typ""")
    _dfs[f'RBNP_SMRY_RESULTS_{pays}'].createOrReplaceTempView(f'RBNP_SMRY_RESULTS_{pays}')

    _dfs[f'IBNR_SMRY_RESULTS_{pays}'] = spark.sql(f"""SELECT country,Rsrv_Grp, sum(Rsrv_Amt_Gross) as Ibnr_SubGrp
    	FROM {ouput}.WPS_DAAP_IBNR_{yr}{month}{day}
    	WHERE country='{pays}' and Rsrv_Grp not in ('ZZ2','ZZ1') 
    	GROUP BY country,Rsrv_Grp""")
    _dfs[f'IBNR_SMRY_RESULTS_{pays}'].createOrReplaceTempView(f'IBNR_SMRY_RESULTS_{pays}')


moduleresults(pays="DE")
moduleresults(pays="DK")
moduleresults(pays="ES")
moduleresults(pays="IE")
moduleresults(pays="IT")
moduleresults(pays="FI")
moduleresults(pays="GR")
moduleresults(pays="NL")
moduleresults(pays="NI")
moduleresults(pays="NO")
moduleresults(pays="PL")
moduleresults(pays="PT")
moduleresults(pays="SE")
moduleresults(pays="TR")
moduleresults(pays="UK")
moduleresults(pays="AT")
moduleresults(pays="FR")
from functools import reduce
CH_DATA_ICOP_ALL = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table('ICOP_SMRY_RESULTS_DE'), spark.table('ICOP_SMRY_RESULTS_DK'), spark.table('ICOP_SMRY_RESULTS_ES'), spark.table('ICOP_SMRY_RESULTS_FI'), spark.table('ICOP_SMRY_RESULTS_FR'), spark.table('ICOP_SMRY_RESULTS_GR'), spark.table('ICOP_SMRY_RESULTS_IE'), spark.table('ICOP_SMRY_RESULTS_IT'), spark.table('ICOP_SMRY_RESULTS_NI'), spark.table('ICOP_SMRY_RESULTS_NL'), spark.table('ICOP_SMRY_RESULTS_NO'), spark.table('ICOP_SMRY_RESULTS_PL'), spark.table('ICOP_SMRY_RESULTS_PT'), spark.table('ICOP_SMRY_RESULTS_SE'), spark.table('ICOP_SMRY_RESULTS_TR'), spark.table('ICOP_SMRY_RESULTS_UK')])
CH_DATA_ICOP_ALL = CH_DATA_ICOP_ALL.filter(~F.expr("""Rsrv_Grp IN ('ZZ2','ZZ1')"""))
CH_DATA_ICOP_ALL.createOrReplaceTempView('CH_DATA_ICOP_ALL')

from functools import reduce
CH_DATA_RBNP_ALL = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table('RBNP_SMRY_RESULTS_DE'), spark.table('RBNP_SMRY_RESULTS_DK'), spark.table('RBNP_SMRY_RESULTS_ES'), spark.table('RBNP_SMRY_RESULTS_FI'), spark.table('RBNP_SMRY_RESULTS_FR'), spark.table('RBNP_SMRY_RESULTS_GR'), spark.table('RBNP_SMRY_RESULTS_IE'), spark.table('RBNP_SMRY_RESULTS_IT'), spark.table('RBNP_SMRY_RESULTS_NI'), spark.table('RBNP_SMRY_RESULTS_NL'), spark.table('RBNP_SMRY_RESULTS_NO'), spark.table('RBNP_SMRY_RESULTS_PL'), spark.table('RBNP_SMRY_RESULTS_PT'), spark.table('RBNP_SMRY_RESULTS_SE'), spark.table('RBNP_SMRY_RESULTS_TR'), spark.table('RBNP_SMRY_RESULTS_UK')])
CH_DATA_RBNP_ALL = CH_DATA_RBNP_ALL.filter(~F.expr("""Rsrv_Grp IN ('ZZ2','ZZ1')"""))
CH_DATA_RBNP_ALL.createOrReplaceTempView('CH_DATA_RBNP_ALL')

from functools import reduce
CH_DATA_IBNR_ALL = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table('IBNR_SMRY_RESULTS_DE'), spark.table('IBNR_SMRY_RESULTS_DK'), spark.table('IBNR_SMRY_RESULTS_ES'), spark.table('IBNR_SMRY_RESULTS_FI'), spark.table('IBNR_SMRY_RESULTS_FR'), spark.table('IBNR_SMRY_RESULTS_GR'), spark.table('IBNR_SMRY_RESULTS_IE'), spark.table('IBNR_SMRY_RESULTS_IT'), spark.table('IBNR_SMRY_RESULTS_NI'), spark.table('IBNR_SMRY_RESULTS_NL'), spark.table('IBNR_SMRY_RESULTS_NO'), spark.table('IBNR_SMRY_RESULTS_PL'), spark.table('IBNR_SMRY_RESULTS_PT'), spark.table('IBNR_SMRY_RESULTS_SE'), spark.table('IBNR_SMRY_RESULTS_TR'), spark.table('IBNR_SMRY_RESULTS_UK')])
CH_DATA_IBNR_ALL = CH_DATA_IBNR_ALL.filter(~F.expr("""Rsrv_Grp IN ('ZZ2','ZZ1')"""))
CH_DATA_IBNR_ALL.createOrReplaceTempView('CH_DATA_IBNR_ALL')

def export_excel(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


repexp = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/CLAIMS HANDLING DATA/{quarter}/Ouput/CH_DATA_IBNR_ALL.xlsx"
export_excel(datatable=CH_DATA_IBNR_ALL, database=repexp, sheet="CH_DATA_IBNR_ALL")
repexp = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/CLAIMS HANDLING DATA/{quarter}/Ouput/CH_DATA_ICOP_ALL.xlsx"
export_excel(datatable=CH_DATA_ICOP_ALL, database=repexp, sheet="CH_DATA_ICOP_ALL")
repexp = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/CLAIMS HANDLING DATA/{quarter}/Ouput/CH_DATA_RBNP_ALL.xlsx"
export_excel(datatable=CH_DATA_RBNP_ALL, database=repexp, sheet="CH_DATA_RBNP_ALL")