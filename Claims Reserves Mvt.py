from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
ouput = "CR_Q226"
month = 06
day = 26
yr = 2026
q = "Q22026"
# #################################################################################################################################################################################
# ######################################################   IMPACT CASE RESERVES : COMPARER LES CASES RESERVES Q1   VS Q2    ##############################################
# ################################################################################################################################################################################
# CR DAAP
# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'Clm_Nmbr': '0', 'cover': '0', 'Entity': '0', 'Totl_Bnfts_Amnt_Pd_Gross': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0', 'informer_type': '0'}
_dfs[f'Table_DAAP_{yr}{month}{day}'] = spark.table(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'Table_DAAP_{yr}{month}{day}'] = _dfs[f'Table_DAAP_{yr}{month}{day}'].filter(F.expr("""STATUS  IN ('OP','RO')  AND Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND LEGACY_SCHEME_BOOK='TIA'"""))
_dfs[f'Table_DAAP_{yr}{month}{day}'] = _dfs[f'Table_DAAP_{yr}{month}{day}'].select('country', 'Rsrv_Grp', 'Clm_Nmbr', 'cover', 'Entity', 'Totl_Bnfts_Amnt_Pd_Gross', 'Rsrv_Typ', 'Rsrv_Amt_Gross', 'Rsrv_Amt_Net', 'informer_type')
_dfs[f'Table_DAAP_{yr}{month}{day}'] = _dfs[f'Table_DAAP_{yr}{month}{day}'].withColumnRenamed('Totl_Bnfts_Amnt_Pd_Gross', 'Totl_Bnfts_Amnt_Pd_DAAP_')
_dfs[f'Table_DAAP_{yr}{month}{day}'] = _dfs[f'Table_DAAP_{yr}{month}{day}'].withColumnRenamed('Rsrv_Amt_Gross', 'Rsrv_Amt_DAAP_')
_dfs[f'Table_DAAP_{yr}{month}{day}'].createOrReplaceTempView(f'Table_DAAP_{yr}{month}{day}')

_dfs[f'Table_DAAP_{yr}{month}{day}'] = spark.sql(f"""SELECT country,
                    Rsrv_Grp,
                    cover,
                    Entity,
                    Rsrv_Typ,
                    sum(Rsrv_Amt_DAAP_{q}) as Rsrv_Amt_DAAP_{q},
                    sum(Rsrv_Amt_Net) as Rsrv_Amt_Net_{q}
                                                       
     FROM    Table_DAAP_{yr}{month}{day}
     group by country, Rsrv_Grp, cover,Entity, Rsrv_Typ
      """)
_dfs[f'Table_DAAP_{yr}{month}{day}'].createOrReplaceTempView(f'Table_DAAP_{yr}{month}{day}')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'Clm_Nmbr': '0', 'cover': '0', 'Entity': '0', 'Totl_Bnfts_Amnt_Pd_Gross': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0', 'informer_type': '0'}
_dfs[f'Table_MACAO_{yr}{month}{day}'] = spark.table(f'{ouput}.wps_daap_case_reserves_{yr}{month}{day}')
_dfs[f'Table_MACAO_{yr}{month}{day}'] = _dfs[f'Table_MACAO_{yr}{month}{day}'].filter(F.expr("""STATUS  IN ('OP','RO')  AND Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND LEGACY_SCHEME_BOOK='MACAO'"""))
_dfs[f'Table_MACAO_{yr}{month}{day}'] = _dfs[f'Table_MACAO_{yr}{month}{day}'].select('country', 'Rsrv_Grp', 'Clm_Nmbr', 'cover', 'Entity', 'Totl_Bnfts_Amnt_Pd_Gross', 'Rsrv_Typ', 'Rsrv_Amt_Gross', 'Rsrv_Amt_Net', 'informer_type')
_dfs[f'Table_MACAO_{yr}{month}{day}'] = _dfs[f'Table_MACAO_{yr}{month}{day}'].withColumnRenamed('Totl_Bnfts_Amnt_Pd_Gross', 'Totl_Bnfts_Amnt_Pd_DAAP_')
_dfs[f'Table_MACAO_{yr}{month}{day}'] = _dfs[f'Table_MACAO_{yr}{month}{day}'].withColumnRenamed('Rsrv_Amt_Gross', 'Rsrv_Amt_DAAP_')
_dfs[f'Table_MACAO_{yr}{month}{day}'].createOrReplaceTempView(f'Table_MACAO_{yr}{month}{day}')

_dfs[f'Table_MACAO_{yr}{month}{day}'] = spark.sql(f"""SELECT country,
                    Rsrv_Grp,
                    cover,
                    Entity,
                    Rsrv_Typ,
                    sum(Rsrv_Amt_DAAP_{q}) as Rsrv_Amt_DAAP_{q},
                    sum(Rsrv_Amt_Net) as Rsrv_Amt_Net_{q}
                                                       
     FROM    Table_MACAO_{yr}{month}{day}
     group by country, Rsrv_Grp, cover,Entity, Rsrv_Typ
      """)
_dfs[f'Table_MACAO_{yr}{month}{day}'].createOrReplaceTempView(f'Table_MACAO_{yr}{month}{day}')

chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"
database = f"WORK.TABLE_DAAP_{yr}{month}{day}"
database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{yr}{month}{day}!A1').option('header', 'true').mode('overwrite').save(chemin)

database1 = f"WORK.TABLE_MACAO_{yr}{month}{day}"
database1.write.format('com.crealytics.spark.excel').option('dataAddress', f'{yr}{month}{day}!A1').option('header', 'true').mode('overwrite').save(chemin)

# #################################################################################################################################################################################
# ######################################################   IMPACT IBNR : COMPARER LES IBNR  Q1 VS  Q2    ##############################################
# ################################################################################################################################################################################

# KO : ligne 99
# PROC SQL;
      create table IBNR_LPI_%substr(&Q.,1,2)&yr. as
             SELECT country,
                    Rsrv_Grp,
                    cover,
                    Entity,
                    sum(Rsrv_Amt_Gross) as Rsrv_Amt                                    
     FROM    &Ouput..WPS_DAAP_IBNR_&yr.&month.&day.
     where Rsrv_Grp not in ("ZZ1")
     group by country, Rsrv_Grp, cover,Entity
      ; 
 quit;
# fin KO

database = f"IBNR_LPI_%substr({q},1,2){yr}"
chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"
database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{yr}{month}{day}!A1').option('header', 'true').mode('overwrite').save(chemin)

# PROC SQL;
# create table NC_LPI_Q42019 as
# SELECT country,
# Rsrv_Grp,
# cover,
# Entity,
# sum(Rsrv_Amt_Gross) as Rsrv_Amt_Q4
# FROM    CR_Q120.WPS_DAAP_IBNR_20191227
# where Rsrv_Grp  in ("ZZ1")
# group by country, Rsrv_Grp, cover,Entity
# ;
# quit;
_dfs[f'NNC_LPI_{q}'] = spark.sql(f"""SELECT country,
                    Rsrv_Grp,
                    cover,
                    Entity,
                    sum(Rsrv_Amt_Gross) as Rsrv_Amt                                    
     FROM    {ouput}.WPS_DAAP_NCC_{yr}{month}{day}
     where Rsrv_Grp in ('ZZ1')
     group by country, Rsrv_Grp, cover,Entity
      """)
_dfs[f'NNC_LPI_{q}'].createOrReplaceTempView(f'NNC_LPI_{q}')

database = f"WORK.NNC_LPI_{q}"
chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/NON-CORE-COVER/Outputs"
database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{yr}{month}{day}!A1').option('header', 'true').mode('overwrite').save(chemin)
