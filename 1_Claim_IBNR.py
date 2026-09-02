from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #################################################################################################################################################################################
# ##########################################################       EXTRACTION DATA IBNR   ########################################################################################
# ################################################################################################################################################################################
# ######## Name: EXTRACTION DATA IBNR
# ######## Author: ALSENY SOW
# ######## Date started :26/06/2018
# ######## Date finished:
# ######## Context: CREATION DES INPUTS  POUR LE CALCUL DES IBNR CLP
# #####################################################  CREATION DES LIBRARY  #########################################################################################
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
qn = "Q226"
# modifier le numero du quarter ( Q1,Q2,Q3,Q4) suivi de l'année 21 pour 2021 par exemple
arrete = "2026_06_Prov"
# ######### Modification à faire:
balancedate = "26/06/2026"
quarter = "2026Q2"
ouput = f"CR_{qn}"
# #################################################################################################################################################################################
# ######################################################   MACRO EXTRACTION PAID   ########################################################################################
# ################################################################################################################################################################################
def dataprep(balancedate):
    # *M3: 1 Set up Year-month grid;
    YRMNTH_GRID = spark.createDataFrame([], schema=StructType([]))
    YRMNTH_GRID = (YRMNTH_GRID
        .withColumn('YR', F.expr(f"""year(intnx('month',cast({balancedate} as double),-i))"""))
        .withColumn('MNTH', F.expr(f"""month(intnx('month',cast({balancedate} as double),-i))"""))
    )
    # DO loop: for i in range(0, 95+1, 1):
    # OUTPUT statement — rows already accumulated by PySpark
    # END DO
    YRMNTH_GRID = YRMNTH_GRID.drop('i')
    YRMNTH_GRID.createOrReplaceTempView('YRMNTH_GRID')

    DEVMONTH = spark.createDataFrame([], schema=StructType([]))
    DEVMONTH = (DEVMONTH
        .withColumn('DEVMONTH', F.col('i'))
    )
    # DO loop: for i in range(1, 60+1, 1):
    # OUTPUT statement — rows already accumulated by PySpark
    # END DO
    DEVMONTH = DEVMONTH.drop('i')
    DEVMONTH.createOrReplaceTempView('DEVMONTH')

    YRMNTHDEV = spark.sql("""SELECT DISTINCT YR, MNTH, DEVMONTH
    	FROM YRMNTH_GRID, DEVMONTH""")
    YRMNTHDEV.createOrReplaceTempView('YRMNTHDEV')


dataprep(balancedate)
import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="flag_legacy", onglet="flag_legacy")
def data_ibnr(grp, pays):
    _dfs[f'CLMHDR_ALL_{pays}'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao AS Flag_Macao
    from CR_{qn}.CLMHDR_ALL_{pays}  t1 
    left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.Schm=t8.scheme and t1.Cvr_Typ=t8.Cover) 
     """)
    _dfs[f'CLMHDR_ALL_{pays}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}')

    _dfs[f'CLMHDR_ALL_{pays}'] = spark.table(f'CLMHDR_ALL_{pays}')
    _dfs[f'CLMHDR_ALL_{pays}'] = (_dfs[f'CLMHDR_ALL_{pays}']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'CLMHDR_ALL_{pays}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}')

    _dfs[f'CLMHDR_ALL_{pays}'] = spark.table(f'CLMHDR_ALL_{pays}')
    # if RPP not in ("0","") then LEGACY_SCHEME_BOOK="MACAO";
    if RPP in ("0","") then LEGACY_SCHEME_BOOK="TIA";
    _dfs[f'CLMHDR_ALL_{pays}'] = _dfs[f'CLMHDR_ALL_{pays}'].drop('informer_type', 'RPP', 'Flag_Macao')
    _dfs[f'CLMHDR_ALL_{pays}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}')
    # LIBNAME CR_{qn} -> base Spark: cr_{qn}.CLMHDR_ALL_{pays}
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS cr_{qn}')
    _dfs[f'CLMHDR_ALL_{pays}'].write.mode('overwrite').saveAsTable(f'cr_{qn}.CLMHDR_ALL_{pays}')

    _dfs[f'CLMHDR_ALL_{pays}'] = spark.table(f'{ouput}.CLMHDR_ALL_{pays}')
    _dfs[f'CLMHDR_ALL_{pays}'] = _dfs[f'CLMHDR_ALL_{pays}'].filter(F.expr("""LEGACY_SCHEME_BOOK='TIA'"""))
    _dfs[f'CLMHDR_ALL_{pays}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}')

    _dfs[f'Export_IBNR_data_{pays}_{grp}_0'] = spark.sql(f"""SELECT  Acc_Yr, Acc_Mnth,
    				case when ((Rgstrtn_Yr-Acc_Yr)*12+ (Rgstrtn_Mnth - Acc_Mnth))+1 <=0 then 1 when ((Rgstrtn_Yr-Acc_Yr)*12 + 
    				(Rgstrtn_Mnth - Acc_Mnth))+1 > 59 then 60 else ((Rgstrtn_Yr-Acc_Yr)*12 + (Rgstrtn_Mnth - Acc_Mnth)) + 1 end as 
    				Dev_Mnth,
    				sum(Totl_Amnt_Pd) as Clms_Paid,
    				sum(Totl_Amnt_Pd + Rsrv_Amt) as Clms_Incrd
    		FROM CLMHDR_ALL_{pays}
    		where Rsrv_Grp='{grp}'
    		GROUP BY Acc_Yr, Acc_Mnth, Dev_Mnth""")
    _dfs[f'Export_IBNR_data_{pays}_{grp}_0'].createOrReplaceTempView(f'Export_IBNR_data_{pays}_{grp}_0')

    _dfs[f'Export_PAID_data_{pays}_{grp}_0'] = spark.sql(f"""SELECT y.YR as AccYr, y.Mnth as AccMnth, y.DEVMONTH as Dev_Mnth, i.Clms_Paid
    		FROM Export_IBNR_data_{pays}_{grp}_0 i
    		RIGHT JOIN YRMNTHDEV y ON (y.YR = i.Acc_Yr and y.Mnth = i.Acc_Mnth and y.DEVMONTH= i.Dev_Mnth)
    		""")
    _dfs[f'Export_PAID_data_{pays}_{grp}_0'].createOrReplaceTempView(f'Export_PAID_data_{pays}_{grp}_0')

    # PROC TRANSPOSE
    # TRANSPOSE: no VAR statement — manual review needed
    _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'] = _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}_0']  # TODO
    _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'].createOrReplaceTempView(f'EXPORT_PAID_DATA_{pays}_{grp}')

    _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'] = spark.table(f'EXPORT_PAID_DATA_{pays}_{grp}')
    _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'] = _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'].drop('_NAME_')
    _dfs[f'EXPORT_PAID_DATA_{pays}_{grp}'].createOrReplaceTempView(f'EXPORT_PAID_DATA_{pays}_{grp}')

    _dfs[f'Q_EXPORT_PAID_DATA_{pays}_{grp}'] = spark.table(f'EXPORT_PAID_DATA_{pays}_{grp}')
    # ARRAY missing = ['_:']
    # DO loop: for i in range(1, dim(missing)+1, 1):
    # IF/THEN (manual review needed):
    #   IF missing(i) =. THEN missing(i)= 0 ;
    # END DO
    _dfs[f'Q_EXPORT_PAID_DATA_{pays}_{grp}'] = _dfs[f'Q_EXPORT_PAID_DATA_{pays}_{grp}'].drop('i')
    _dfs[f'Q_EXPORT_PAID_DATA_{pays}_{grp}'].createOrReplaceTempView(f'Q_EXPORT_PAID_DATA_{pays}_{grp}')

    _dfs[f'Export_IBNR_data_{pays}_{grp}_1'] = spark.sql(f"""SELECT y.YR as AccYr, y.Mnth as AccMnth, y.DEVMONTH as Dev_Mnth, i.Clms_Incrd
    		FROM Export_IBNR_data_{pays}_{grp}_0 i
    		RIGHT JOIN YRMNTHDEV y ON (y.YR = i.Acc_Yr and y.Mnth = i.Acc_Mnth and y.DEVMONTH= i.Dev_Mnth)
    		""")
    _dfs[f'Export_IBNR_data_{pays}_{grp}_1'].createOrReplaceTempView(f'Export_IBNR_data_{pays}_{grp}_1')

    # PROC TRANSPOSE
    # TRANSPOSE: no VAR statement — manual review needed
    _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'] = _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}_1']  # TODO
    _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'].createOrReplaceTempView(f'EXPORT_IBNR_DATA_{pays}_{grp}')

    _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'] = spark.table(f'EXPORT_IBNR_DATA_{pays}_{grp}')
    _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'] = _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'].drop('_NAME_')
    _dfs[f'EXPORT_IBNR_DATA_{pays}_{grp}'].createOrReplaceTempView(f'EXPORT_IBNR_DATA_{pays}_{grp}')

    _dfs[f'Q_EXPORT_IBNR_DATA_{pays}_{grp}'] = spark.table(f'EXPORT_IBNR_DATA_{pays}_{grp}')
    # ARRAY missing = ['_:']
    # DO loop: for i in range(1, dim(missing)+1, 1):
    # IF/THEN (manual review needed):
    #   IF missing(i) =. THEN missing(i)= 0 ;
    # END DO
    _dfs[f'Q_EXPORT_IBNR_DATA_{pays}_{grp}'] = _dfs[f'Q_EXPORT_IBNR_DATA_{pays}_{grp}'].drop('i')
    _dfs[f'Q_EXPORT_IBNR_DATA_{pays}_{grp}'].createOrReplaceTempView(f'Q_EXPORT_IBNR_DATA_{pays}_{grp}')

    chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/IBNR/Inputs/{quarter}/{c}"
    database.write.format('com.crealytics.spark.excel').option('dataAddress', 'Q_EXPORT_DATA!A1').option('header', 'true').mode('overwrite').save(chemin)


export_excel(database=f"Q_EXPORT_IBNR_DATA_{pays}_{grp}", c=pays)
export_excel(database=f"Q_EXPORT_PAID_DATA_{pays}_{grp}", c=pays)
# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

# PROC DATASETS → Spark table operations

mend()
# #################################################################################################################################################################################
# ######################################################  EXTRACTION PAYS & GROUP OF RESERVE    ########################################################################################
# ################################################################################################################################################################################
# AUSTRIA
data_ibnr(grp="GL1", pays="AT")
data_ibnr(grp="GR1", pays="AT")
data_ibnr(grp="GD1", pays="AT")
data_ibnr(grp="GP1", pays="AT")
# GERMANY
data_ibnr(grp="GL1", pays="DE")
data_ibnr(grp="GR1", pays="DE")
data_ibnr(grp="GD1", pays="DE")
data_ibnr(grp="GP1", pays="DE")
# SPAIN
data_ibnr(grp="GR1", pays="ES")
data_ibnr(grp="GD1", pays="ES")
data_ibnr(grp="GC1", pays="ES")
data_ibnr(grp="GL1", pays="ES")
data_ibnr(grp="GP1", pays="ES")
# FINLAND
data_ibnr(grp="GR1", pays="FI")
data_ibnr(grp="GD1", pays="FI")
data_ibnr(grp="GL1", pays="FI")
data_ibnr(grp="GC1", pays="FI")
# ITALY
data_ibnr(grp="GR1", pays="IT")
data_ibnr(grp="GD1", pays="IT")
data_ibnr(grp="GC1", pays="IT")
data_ibnr(grp="GL1", pays="IT")
data_ibnr(grp="GP1", pays="IT")
# DENMARK
data_ibnr(grp="GR1", pays="DK")
data_ibnr(grp="GD1", pays="DK")
data_ibnr(grp="GL1", pays="DK")
data_ibnr(grp="GP1", pays="DK")
# PORTUGAL
data_ibnr(grp="GR1", pays="PT")
data_ibnr(grp="GD1", pays="PT")
data_ibnr(grp="GL1", pays="PT")
data_ibnr(grp="GP1", pays="PT")
# NORWAY
data_ibnr(grp="GR1", pays="NO")
data_ibnr(grp="GD1", pays="NO")
data_ibnr(grp="GL1", pays="NO")
data_ibnr(grp="GC1", pays="NO")
# new
data_ibnr(grp="GP1", pays="NO")
# SWEDEN
data_ibnr(grp="GR1", pays="SE")
data_ibnr(grp="GD1", pays="SE")
data_ibnr(grp="GL1", pays="SE")
# new
data_ibnr(grp="GC1", pays="SE")
# IRELAND
data_ibnr(grp="GR1", pays="IE")
data_ibnr(grp="GD1", pays="IE")
data_ibnr(grp="GL1", pays="IE")
data_ibnr(grp="GC1", pays="IE")
# NORTHEN IRELAND
data_ibnr(grp="GD1", pays="NI")
data_ibnr(grp="GL1", pays="NI")
# NETHERLAND
data_ibnr(grp="GR1", pays="NL")
data_ibnr(grp="GD1", pays="NL")
# SWITZERLAND
data_ibnr(grp="GR1", pays="CH")
data_ibnr(grp="GD1", pays="CH")
data_ibnr(grp="GC1", pays="CH")
# POLAND
data_ibnr(grp="GR1", pays="PL")
data_ibnr(grp="GD1", pays="PL")
data_ibnr(grp="GL1", pays="PL")
data_ibnr(grp="GC1", pays="PL")
data_ibnr(grp="GP1", pays="PL")
# TURKEY
data_ibnr(grp="GR1", pays="TR")
data_ibnr(grp="GD1", pays="TR")
data_ibnr(grp="GL1", pays="TR")
# UK
data_ibnr(grp="GR1", pays="UK")
data_ibnr(grp="GD1", pays="UK")
data_ibnr(grp="GL1", pays="UK")
data_ibnr(grp="GP1", pays="UK")
# new
data_ibnr(grp="GC1", pays="UK")
# GREECE
data_ibnr(grp="GR1", pays="GR")
data_ibnr(grp="GD1", pays="GR")
# FRANCE
data_ibnr(grp="GR1", pays="FR")
data_ibnr(grp="GR2", pays="FR")
data_ibnr(grp="GR3", pays="FR")
data_ibnr(grp="GD1", pays="FR")
data_ibnr(grp="GD2", pays="FR")
data_ibnr(grp="GD3", pays="FR")
data_ibnr(grp="GL1", pays="FR")
data_ibnr(grp="GL2", pays="FR")
data_ibnr(grp="GP1", pays="FR")
# COLOMBIA
data_ibnr(grp="GR1", pays="CO")
data_ibnr(grp="GD1", pays="CO")
# BELGIUM
data_ibnr(grp="GR1", pays="BE")
data_ibnr(grp="GD1", pays="BE")
data_ibnr(grp="GL1", pays="BE")
# MEXICO
data_ibnr(grp="GR1", pays="MX")
data_ibnr(grp="GD1", pays="MX")
data_ibnr(grp="GP1", pays="MX")