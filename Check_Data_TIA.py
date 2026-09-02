from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ########################### DAAP   ###############################
lreseau = "~/NAS/X"
arrete = "2026_04_V2"
tia_path = f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
DATABASE_ALL_PL_V3 = spark.table('tia.DATABASE_ALL_PL')
DATABASE_ALL_PL_V3 = DATABASE_ALL_PL_V3.filter(~F.expr("""Type_Insurance IN (11,8) AND POSTE IN ('PREMIUM')"""))
DATABASE_ALL_PL_V3 = DATABASE_ALL_PL_V3.drop('Type_Insurance')
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

DATABASE_ALL_PL_V3 = spark.sql("""select country,entity_name,POSTE,gl_period,sum(MONTANT) as MONTANT     
from DATABASE_ALL_PL_V3
group by country,entity_name,POSTE,gl_period""")
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

DATABASE_ALL_PL_V3 = spark.table('DATABASE_ALL_PL_V3')
DATABASE_ALL_PL_V3 = (DATABASE_ALL_PL_V3
    .withColumn('POSTE', F.when(F.expr("""POSTE='PREMIUM'"""), F.lit('GWP')))
    .withColumn('POSTE', F.when(F.expr("""POSTE='COMMISSION'"""), F.lit('Comms')))
    .withColumn('POSTE', F.when(F.expr("""POSTE='CLAIM'"""), F.lit('Claims')))
)
# if Type_Insurance in (11,8) then delete ; 
drop Type_Insurance ;
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

DATABASE_ALL_PL_V3 = spark.table('DATABASE_ALL_PL_V3').orderBy('country', 'entity_name', 'POSTE', 'gl_period')
DATABASE_ALL_PL_V3 = DATABASE_ALL_PL_V3.dropDuplicates(['country', 'entity_name', 'POSTE', 'gl_period'])
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

# PROC TRANSPOSE
# ID present → long-to-wide pivot
DATABASE_ALL_PL_V3 = DATABASE_ALL_PL_V3.groupBy('country', 'entity_name', 'POSTE', 'gl_period').pivot('POSTE').agg(F.first(F.col('MONTANT')))
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

DATABASE_ALL_PL_V3 = spark.table('DATABASE_ALL_PL_V3')
DATABASE_ALL_PL_V3 = (DATABASE_ALL_PL_V3
    .withColumn('Comms', F.when(F.expr("""Comms IS NULL"""), F.lit(0)))
    .withColumn('GWP', F.when(F.expr("""GWP IS NULL"""), F.lit(0)))
    .withColumn('Claims', F.when(F.expr("""Claims IS NULL"""), F.lit(0)))
    .withColumn('PS_PAID', F.when(F.expr("""PS_PAID IS NULL"""), F.lit(0)))
    .withColumn('CLAIM_CED', F.when(F.expr("""CLAIM_CED IS NULL"""), F.lit(0)))
)
DATABASE_ALL_PL_V3 = DATABASE_ALL_PL_V3.drop('POSTE')
DATABASE_ALL_PL_V3.createOrReplaceTempView('DATABASE_ALL_PL_V3')

# ROW COUNT
count = spark.sql("""select countryid_vorig, count(*) as RowCount
    from tia.daap_level_1_dueonly
    group by countryid_vorig""")
count.createOrReplaceTempView('count')
