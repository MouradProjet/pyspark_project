from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

lreseau = "~/NAS/X"
# Mettre le serveur approprié  entre -> ~/NAS/X  ou -> X:/Inventprev **
arrete = "2026_04_V2"
tia_path = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
def resize(lib, mem, out):
    CHARCOLS = [r[0] for r in spark.table('dictionary.columns').filter(f"""LIBNAME=upcase('{lib}') AND MEMNAME=upcase('{mem}')
            AND upcase(TYPE)='CHAR'""").select('NAME').collect()]
    CHARCOLS = ' '.join(str(x) for x in CHARCOLS)

    # New dataset with resized char columns
    _dfs[f'{out}'] = spark.table(f'{lib}.{mem}')
        # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
        # The following SAS uses a macro %do/%let loop to generate
        # indexed columns at compile time. Translate by hand using a
        # Python for-loop with df.withColumn(f'col_{i}', ...).
        # SAS: %do i=1 %to {nchars}
        # ==============================================================
    _dfs[f'{out}'].createOrReplaceTempView(f'{out}')

    # remove the informats on character variables as the data already exists
    outlib = f"%upcase(%scan({out},1,'.'))"
    outmem = f"%upcase(%scan({out},2,'.'))"
    if {outmem}=%str():
        %let outmem=&outlib;
          %let outlib=work;
    # PROC DATASETS → Spark table operations


def split_country(country):
    _dfs[f'GLOBAL_PL_{country}'] = spark.table('tia.DAAP_LEVEL_1_DUEONLY')
    _dfs[f'GLOBAL_PL_{country}'] = _dfs[f'GLOBAL_PL_{country}'].filter(F.col('countryid_vorig') == f"{country}")
    _dfs[f'GLOBAL_PL_{country}'].createOrReplaceTempView(f'GLOBAL_PL_{country}')
    # LIBNAME TIA -> base Spark: tia.GLOBAL_PL_{country}
    _dfs[f'GLOBAL_PL_{country}'].write.mode('overwrite').saveAsTable(f'tia.GLOBAL_PL_{country}')

    # %resize(lib=TIA, mem=GLOBAL_PL_&Country., out=TIA.GLOBAL_PL_&Country.);

split_country(country="PE")
split_country(country="LU")
split_country(country="AT")
split_country(country="BE")
split_country(country="CH")
split_country(country="CO")
split_country(country="DE")
split_country(country="DK")
split_country(country="FI")
split_country(country="FR")
split_country(country="GR")
split_country(country="IE")
split_country(country="MX")
split_country(country="NI")
split_country(country="NL")
split_country(country="NO")
split_country(country="PL")
split_country(country="PT")
split_country(country="SE")
split_country(country="TR")
split_country(country="UK")
split_country(country="LT")
split_country(country="ES")
split_country(country="IT")