from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ── Databricks Unity Catalog configuration ───────────────────────────
# Set your catalog name below. Each SAS LIBNAME becomes a schema.
# e.g. LIBNAME BGD_Q425 → schema 'bgd_q425' in catalog 'your_catalog'
_catalog = 'your_catalog'  # TODO: replace with your Unity Catalog name

topline = "092025 DAVC SHUTTLE FILE FY25 V3"
sdb = "Updated SDB Data Files 03.09.2025"
n = 2025
arrete = "2025_09_Q4"
nom_base_anterio = "base_anteriorite_idean_2025"
def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


# SBD
import_sdb = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/01_Cartographie/{sdb}.xlsx"
import_excelx(datafile=import_sdb, out="SDB_TIA", onglet="TIA SDB")
import_excelx(datafile=import_sdb, out="SDB_MACAO", onglet="Macao SDB")
# Topline
import_01 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/Topline/{topline}.xlsx"
import_excelx(datafile=import_01, out="Topline_Macao", onglet="Combined")
import_excelx(datafile=import_01, out="Topline_TIA", onglet="10. TIA (Macao Clients) Part1")
cqs_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/Macao/CQS CBP Process/01 - Base CBP"  # LIBNAME CQS
cqs_out_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
anterio_path = "~/NAS/X/08.Progammes/Etablissements financiers/OUTIL INVENTAIRE/Base anteriorite"  # LIBNAME ANTERIO
cqs_hy25_path = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2025_04_V2/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_HY25
cbp_italy_policies_claims = spark.table('cbp_italy_policies_claims')
cbp_italy_policies_claims = (cbp_italy_policies_claims
    .withColumn('IDEAN1', F.expr("""cast(concat("1", substring(ID_Police,6,4)) as long)"""))
    .withColumn('IDEAN2', F.expr("""cast(concat("1", substring(ID_Police,1,4)) as long)"""))
)
cbp_italy_policies_claims = cbp_italy_policies_claims.select('idean1', 'idean2', 'type_pret', 'generation')
cbp_italy_policies_claims.createOrReplaceTempView('cbp_italy_policies_claims')

idean_base_cbp = spark.sql("""select distinct idean1,idean2,type_pret
from cbp_italy_policies_claims """)
idean_base_cbp.createOrReplaceTempView('idean_base_cbp')

sdb_tia2 = spark.table('sdb_tia')
sdb_tia2 = (sdb_tia2
    .withColumn('country2', F.expr("""substring(Country,1,2)"""))
)
sdb_tia2 = sdb_tia2.drop('country')
sdb_tia2 = sdb_tia2.withColumnRenamed('country2', 'country')
sdb_tia2.createOrReplaceTempView('sdb_tia2')

sdb_tia_cqs = spark.table('sdb_tia2')
sdb_tia_cqs = sdb_tia_cqs.filter(F.expr("""cqs_split IN ("Kereis Italy")"""))
sdb_tia_cqs.createOrReplaceTempView('sdb_tia_cqs')

idean_sdb = spark.sql("""select distinct scheme as scheme_sdb, country
from sdb_tia_cqs """)
idean_sdb.createOrReplaceTempView('idean_sdb')

idean_cqs = spark.table('idean_base_cbp') \
    .union(spark.table('idean_sdb'))
idean_cqs.createOrReplaceTempView('idean_cqs')

idean_cqs1 = spark.table('idean_cqs')
idean_cqs1 = (idean_cqs1
    .withColumn('idean', F.expr("""trim(cast(idean1 as string))"""))
)
idean_cqs1 = idean_cqs1.filter(F.col('idean1').isNotNull())
idean_cqs1 = idean_cqs1.select('idean', 'type_pret')
idean_cqs1.createOrReplaceTempView('idean_cqs1')

idean_cqs2 = spark.table('idean_cqs')
idean_cqs2 = (idean_cqs2
    .withColumn('idean', F.expr("""trim(cast(idean2 as string))"""))
)
idean_cqs2 = idean_cqs2.filter(F.col('idean2').isNotNull())
idean_cqs2 = idean_cqs2.select('idean', 'type_pret')
idean_cqs2.createOrReplaceTempView('idean_cqs2')

idean_cqs3 = spark.table('idean_cqs')
idean_cqs3 = idean_cqs3.filter(F.col('scheme_sdb').isNotNull())
idean_cqs3 = idean_cqs3.select('idean', 'type_pret')
idean_cqs3 = idean_cqs3.withColumnRenamed('scheme_sdb', 'idean')
idean_cqs3.createOrReplaceTempView('idean_cqs3')

idean_cqs_fin = spark.table('idean_cqs1') \
    .union(spark.table('idean_cqs2') \
    .union(spark.table('idean_cqs3')))
idean_cqs_fin = (idean_cqs_fin
    .withColumn('country', F.lit("IT"))
)
idean_cqs_fin.createOrReplaceTempView('idean_cqs_fin')

carto_cqs = spark.sql("""select distinct * 
from idean_cqs_fin """)
carto_cqs.createOrReplaceTempView('carto_cqs')

# ici solution 2 : faire un left join entre la topline FY25 kereis et la carto FY24
topline_cqs_combined = spark.table('topline_macao')
topline_cqs_combined = topline_cqs_combined.filter(F.expr("""Methis_vs_CBP="Kereis Italy""""))
topline_cqs_combined.createOrReplaceTempView('topline_cqs_combined')

newb = spark.sql("""select distinct idean, "IT" as country
from topline_cqs_combined """)
newb.createOrReplaceTempView('newb')

carto_cqs_newb = spark.table('set')
# carto_cqs_
# ATTRIB: ATTRIB idean FORMAT=$16.
carto_cqs_newb.createOrReplaceTempView('carto_cqs_newb')

newb = spark.table('newb')
# ATTRIB: ATTRIB idean FORMAT=$16.
newb = newb.select('idean', 'country_code')
newb = newb.withColumnRenamed('country_code', 'country')
newb.createOrReplaceTempView('newb')

carto_cqs = spark.table('newb') \
    .union(spark.table('carto_cqs'))
carto_cqs = (carto_cqs
    .withColumn('country', F.lit("IT"))
)
carto_cqs = carto_cqs.select('idean', 'country')
carto_cqs.createOrReplaceTempView('carto_cqs')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
carto_cqs.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.carto_cqs')

carto_cqs = spark.sql("""select distinct * 
from cqs_out.carto_cqs """)
carto_cqs.createOrReplaceTempView('carto_cqs')

# rajouter les schemes de anterio
base_anteriorite_cqs_2025 = spark.table('base_anteriorite_cqs_2025')
base_anteriorite_cqs_2025.createOrReplaceTempView('base_anteriorite_cqs_2025')

idean_anterio = spark.sql("""select distinct idean, country
from base_anteriorite_cqs_2025 """)
idean_anterio.createOrReplaceTempView('idean_anterio')

carto_cqs = spark.table('carto_cqs') \
    .union(spark.table('idean_anterio'))
carto_cqs.createOrReplaceTempView('carto_cqs')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
carto_cqs.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.carto_cqs')
