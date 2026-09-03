from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

balancedate = "26/06/2026"
arrete = "2026_06_Prov"
def specification(pays):
    import_ = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/{pays} Model Properties.xlsx"
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', "'RESERVE_GROUP_SPEC'!A1")
        .option('header', 'true')
        .load(import_))
    _df_tmp.createOrReplaceTempView(f'{pays}_RESERVE_GROUP_SPEC')

    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', "'MNTHLY_BNFT_LIMITS'!A1")
        .option('header', 'true')
        .load(import_))
    _df_tmp.createOrReplaceTempView(f'{pays}_MNTHLY_BNFT_LIMITS')

    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', "'OTSTANDING_BLNC_LIMITS'!A1")
        .option('header', 'true')
        .load(import_))
    _df_tmp.createOrReplaceTempView(f'{pays}_OTSTANDING_BLNC_LIMITS')

    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', "'TRANS_TYPE_MAP'!A1")
        .option('header', 'true')
        .load(import_))
    _df_tmp.createOrReplaceTempView(f'{pays}_TRANS_TYPE_MAP')

    if pays == 'FR':
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', "'SCHEME_DATABASE'!A1")
            .option('header', 'true')
            .load(import_))
        _df_tmp.createOrReplaceTempView(f'{pays}_SCHEME_DATABASE')
    
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', "'BEN_POUC'!A1")
            .option('header', 'true')
            .load(import_))
        _df_tmp.createOrReplaceTempView(f'{pays}_BEN_POUC')
    
    if pays == 'UK':
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', "'FIXED_BNFT_LIMITS'!A1")
            .option('header', 'true')
            .load(import_))
        _df_tmp.createOrReplaceTempView(f'{pays}FIXED_BNFT_LIMITS')
    
    # %DO;
    # %END;

specification(pays="UK")
specification(pays="FI")
specification(pays="FR")
specification(pays="SE")
specification(pays="PT")
specification(pays="DE")
specification(pays="PL")
specification(pays="IT")
specification(pays="NO")
specification(pays="ES")
specification(pays="IE")
specification(pays="NI")
specification(pays="NL")
specification(pays="GR")
specification(pays="TR")
specification(pays="CH")
specification(pays="DK")
specification(pays="AT")
specification(pays="BE")
specification(pays="CO")
specification(pays="MX")
specification(pays="LT")
specification(pays="LV")
specification(pays="EE")
# %Specification(pays=LU) ;
