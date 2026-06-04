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

sdb = "Updated SDB Data Files 03.09.2025"
# Renseigner le nom de la SDB
arrete = "2025_09_Q4"
topline = "092025 DAVC SHUTTLE FILE FY25 V3"
# Pour bien isoler Macao il faut faire une combinaison des trois règles
# 1. Agent ID  between 75000 and 79999
# 2.	Agent Name with _FOS in the name
# 3.	RPP present
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
import_excelx(datafile=import_01, out="Topline_TIA", onglet="10. TIA (Macao Clients) Part1")
cqs_out_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
Topline_TIA2 = spark.table('Topline_TIA')
Topline_TIA2 = Topline_TIA2.filter(F.expr("""Methis_vs_CBP="Kereis Italy""""))
Topline_TIA2.createOrReplaceTempView('Topline_TIA2')

sdb_tia2 = spark.table('sdb_tia')
sdb_tia2 = (sdb_tia2
    .withColumn('Country', F.expr("""regexp_replace(substring(Country,1,2), ' ', '')"""))
    .withColumn('SI',
        F.when(F.expr("""RPP NOT IN ("0","") OR Agent_Id =: "75" OR prxmatch('/FOS|_FOS/',Agent_Name)"""), F.lit("MACAO"))
         .otherwise(F.lit("TIA")))
    .withColumn('IDEAN', F.expr("""substring(RPP,1,5)"""))
    .withColumn('IDEAN_2', F.expr("""concat("1", compress(Macao_Group_Policy))"""))
    .withColumn('IDEAN_3', F.expr("""concat("1", substring(compress(Scheme_Name),1,4))"""))
)
# permet d'avoir l'idean si le rpp  est manquant
# permet d'avoir l'idean si le rpp et Macao_Group_Policy sont maquants
sdb_tia2 = sdb_tia2.select('Country', 'Scheme', 'Scheme_Name', 'Scheme_Description', 'Product', 'Product_Version', 'Agent_Id', 'Agent_Name', 'PMP_Sales_Name', 'Sub_Product', 'PAP_Product', 'Product_Line_Version', 'Cover_Description', 'Business_Type', 'Underwriting_branch', 'Macao_Agent_id', 'Agent_RDU', 'Macao_Group_Policy', 'RPP', 'SI', 'IDEAN', 'IDEAN_2', 'IDEAN_3')
sdb_tia2.createOrReplaceTempView('sdb_tia2')

sdb_macao2 = spark.table('sdb_macao')
sdb_macao2 = (sdb_macao2
    .withColumn('RPP', F.expr("""trim(cast(N_RPP_RPTG as string))"""))
    .withColumn('SI', F.lit("MACAO"))
    .withColumn('IDEAN_', F.expr("""substring(regexp_replace(IDEAN1, ' ', ''),1,5)"""))
    .withColumn('Macao_Group_Policy', F.expr("""regexp_replace(CONTRAT_RPTG, ' ', '')"""))
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN_ = 14731"""), F.expr("""upper(PMP_Sales_Name)""")))  # no ELSE: null when condition is false
)
# ATTRIB: attrib Macao_Group_Policy length = $48 format = $48. informat =$48. label ='IDEAN'
# permet d'unifier les PMP associés
# PMP_Sales_Name
sdb_macao2 = sdb_macao2.select('N_RPP_RPTG', 'COASS_ACCEPT', 'IDEAN1', 'CONTRAT_RPTG', 'RAISON_SOCIALE_RPTG', 'PARTENAIRE_RPTG')
sdb_macao2.createOrReplaceTempView('sdb_macao2')

CARTO_CQS_CBP = spark.table('DURATION_MOY_CQS_CBP')
CARTO_CQS_CBP = (CARTO_CQS_CBP
    .withColumn('Country', F.lit("IT"))
    .withColumn('IDEAN2', F.expr("""regexp_replace(IDEAN, ' ', '')"""))
    .withColumn('PERIMETRE', F.lit("CQS CBP"))
)
# ATTRIB: attrib IDEAN2 length = $14 format = $14. informat =$14. label ='IDEAN'
CARTO_CQS_CBP = CARTO_CQS_CBP.filter(F.col('IDEAN').isNotNull())
CARTO_CQS_CBP = CARTO_CQS_CBP.filter(~F.expr("""IDEAN = 1"""))
CARTO_CQS_CBP = CARTO_CQS_CBP.select('Country', 'IDEAN2', 'Financiere_Adh', 'GAR', 'PERIMETRE')
CARTO_CQS_CBP.createOrReplaceTempView('CARTO_CQS_CBP')

sdb_tia3 = spark.sql("""select t1.*,t2.Financiere_Adh , t2.PERIMETRE,t3.Financiere_Adh as Financiere_Adh2 , t3.PERIMETRE as PERIMETRE2,t4.Financiere_Adh as Financiere_Adh3 , t4.PERIMETRE as PERIMETRE3
	from sdb_tia2 t1 
	left join CARTO_CQS_CBP t2 on  (t1.IDEAN=t2.IDEAN2) and (t1.Country=t2.Country)
	left join CARTO_CQS_CBP t3 on  (t1.IDEAN_2=t3.IDEAN2) and (t1.Country=t3.Country)
	left join CARTO_CQS_CBP t4 on  (t1.IDEAN_3=t4.IDEAN2) and (t1.Country=t4.Country)
  """)
sdb_tia3.createOrReplaceTempView('sdb_tia3')

sdb_tia4 = spark.table('sdb_tia3')
    # no ELSE: column is null when condition is false
    .withColumn('IDEAN', F.when(F.expr("""PERIMETRE IS NULL AND NOT PERIMETRE2 IS NULL"""), F.col('IDEAN_2')))
    # no ELSE: column is null when condition is false
    .withColumn('Financiere_Adh', F.when(F.expr("""PERIMETRE IS NULL AND NOT PERIMETRE2 IS NULL"""), F.col('Financiere_Adh2')))
    # no ELSE: column is null when condition is false
    .withColumn('PERIMETRE', F.when(F.expr("""PERIMETRE IS NULL AND NOT PERIMETRE2 IS NULL"""), F.col('PERIMETRE2')))
    # no ELSE: column is null when condition is false
    .withColumn('IDEAN', F.when(F.expr("""PERIMETRE IS NULL AND PERIMETRE2 IS NULL AND NOT PERIMETRE3 IS NULL"""), F.col('IDEAN_3')))
    # no ELSE: column is null when condition is false
    .withColumn('Financiere_Adh', F.when(F.expr("""PERIMETRE IS NULL AND PERIMETRE2 IS NULL AND NOT PERIMETRE3 IS NULL"""), F.col('Financiere_Adh3')))
    # no ELSE: column is null when condition is false
    .withColumn('PERIMETRE', F.when(F.expr("""PERIMETRE IS NULL AND PERIMETRE2 IS NULL AND NOT PERIMETRE3 IS NULL"""), F.col('PERIMETRE3')))
sdb_tia4 = sdb_tia4.drop('IDEAN_2', 'Financiere_Adh2', 'PERIMETRE2', 'IDEAN_3', 'Financiere_Adh3', 'PERIMETRE3')
sdb_tia4.createOrReplaceTempView('sdb_tia4')

sdb_macao3 = spark.sql("""select t1.*,t2.Financiere_Adh , t2.PERIMETRE
	from sdb_macao2 t1 
	left join CARTO_CQS_CBP t2 on  (t1.IDEAN_=t2.IDEAN2)
""")
sdb_macao3.createOrReplaceTempView('sdb_macao3')

sdb_cqs_cbp_macao = spark.table('sdb_macao3')
sdb_cqs_cbp_macao = (sdb_cqs_cbp_macao
    .withColumn('Scheme', F.col('IDEAN_'))
    .withColumn('IDEAN', F.col('IDEAN_'))
    .withColumn('SOURCE_SBD', F.lit("MACAO"))
    .withColumn('PMP_Sales_Name', F.col('PARTENAIRE_RPTG'))
)
# ATTRIB: attrib SOURCE_SBD length = $14 format = $14. informat =$14. label ='SOURCE_SBD'
sdb_cqs_cbp_macao = sdb_cqs_cbp_macao.select('Scheme', 'PMP_Sales_Name', 'PAP_Product', 'RPP', 'Macao_Group_Policy', 'SI', 'IDEAN', 'Financiere_Adh', 'PERIMETRE', 'SOURCE_SBD')
sdb_cqs_cbp_macao.createOrReplaceTempView('sdb_cqs_cbp_macao')

# data PMP_Sales_Name_MACAO;
# set sdb_cqs_cbp_macao ;
# if PERIMETRE in ("CQS CBP");
# run;
# proc sort data =PMP_Sales_Name_MACAO  (keep = IDEAN PMP_Sales_Name)  nodupkey ; by IDEAN PMP_Sales_Name; run;
sdb_cqs_cbp_tia = spark.table('sdb_tia4')
sdb_cqs_cbp_tia = (sdb_cqs_cbp_tia
    .withColumn('SOURCE_SBD', F.lit("TIA"))
)
# ATTRIB: attrib SOURCE_SBD length = $14 format = $14. informat =$14. label ='SOURCE_SBD'
sdb_cqs_cbp_tia = sdb_cqs_cbp_tia.select('Scheme', 'PAP_Product', 'PMP_Sales_Name', 'RPP', 'Macao_Group_Policy', 'SI', 'IDEAN', 'Financiere_Adh', 'PERIMETRE', 'SOURCE_SBD')
sdb_cqs_cbp_tia.createOrReplaceTempView('sdb_cqs_cbp_tia')

# proc sql ;
# create table sdb_cqs_cbp_tia_2 as select distinct
# t1. *, t2.PMP_Sales_Name as PMP_Sales_Name_2
# from  sdb_cqs_cbp_tia t1
# left join PMP_Sales_Name_MACAO t2  on (t1.IDEAN=t2.IDEAN);
# quit;
# à corriger de façon automatique après l'arrêté : alerter CLP des incohérence en la SBD MACAO ET TIA
flag_CQS_CBP_ = spark.table('sdb_cqs_cbp_tia') \
    .union(spark.table('sdb_cqs_cbp_macao'))
flag_CQS_CBP_ = (flag_CQS_CBP_
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN IN ("14016")"""), F.lit("BANCA DELLA NUOVA TERRA")))  # no ELSE: null when condition is false
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN IN ("14544")"""), F.lit("FIDES")))  # no ELSE: null when condition is false
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN IN ("14920","18353")"""), F.lit("PREXTA")))  # no ELSE: null when condition is false
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN IN ("14074")"""), F.lit("Finsarda")))  # no ELSE: null when condition is false
)
    # no ELSE: column is null when condition is false
    .withColumn('PMP_Sales_Name', F.when(F.expr("""IDEAN IN ("18421")"""), F.lit("Finsarda")))
    # no ELSE: column is null when condition is false
    .withColumn('PAP_Product', F.when(F.expr("""IDEAN IN ("18421")"""), F.lit("CQS")))
flag_CQS_CBP_.createOrReplaceTempView('flag_CQS_CBP_')

# data testttt ;
# set flag_CQS_CBP_ ;
# if IDEAN in( "14016",
# "14544",
# "14074",
# "14920",
# "18353",
# "18421", "18282");
# proc sort data =testttt  (keep = IDEAN PMP_Sales_Name)  nodupkey ; by IDEAN PMP_Sales_Name; run;
# run;
dans_Macao_TIA = spark.table('flag_CQS_CBP_').orderBy('IDEAN')
dans_Macao_TIA = dans_Macao_TIA.dropDuplicates(['IDEAN'])
dans_Macao_TIA.createOrReplaceTempView('dans_Macao_TIA')

pas_dans_Macao_TIA = spark.table('CARTO_CQS_CBP').orderBy('IDEAN2')
pas_dans_Macao_TIA = pas_dans_Macao_TIA.dropDuplicates(['IDEAN2'])
pas_dans_Macao_TIA.createOrReplaceTempView('pas_dans_Macao_TIA')

# MERGE: INNER JOIN (if a and b - matched rows only)
flag_CQS_CBP_Hors_MAC_TIA = spark.table('pas_dans_Macao_TIA').join(spark.table('dans_Macao_TIA'), ['IDEAN'], 'inner')
flag_CQS_CBP_Hors_MAC_TIA = (flag_CQS_CBP_Hors_MAC_TIA
    .withColumn('Scheme', F.col('IDEAN'))
    .withColumn('SI', F.lit("MACAO"))
    .withColumn('PERIMETRE', F.lit("CQS CBP"))
    .withColumn('Financiere_Adh1', F.expr("""regexp_replace(Financiere_Adh, "SpA","S.p.A.")"""))
    .withColumn('Financiere_Adh2', F.expr("""split(Financiere_Adh1, 1, "-", "O")"""))
    .withColumn('PMP_Sales_Name', F.expr("""regexp_replace(Financiere_Adh2, "S.p.A."," ")"""))
    .withColumn('SOURCE_SBD', F.lit("DL CQS CBP"))
)
# ATTRIB: attrib SOURCE_SBD length = $14 format = $14. informat =$14. label ='SOURCE_SBD'
flag_CQS_CBP_Hors_MAC_TIA = flag_CQS_CBP_Hors_MAC_TIA.drop('GAR', 'PAP_Product', 'Financiere_Adh1', 'Financiere_Adh2')
flag_CQS_CBP_Hors_MAC_TIA.createOrReplaceTempView('flag_CQS_CBP_Hors_MAC_TIA')

flag_CQS_CBP_2 = spark.table('flag_CQS_CBP_Hors_MAC_TIA') \
    .union(spark.table('flag_CQS_CBP_'))
flag_CQS_CBP_2.createOrReplaceTempView('flag_CQS_CBP_2')

flag_CQS_CBP_2 = spark.table('flag_CQS_CBP_2').orderBy('Scheme', 'PMP_Sales_Name', 'PAP_Product', 'RPP', 'Macao_Group_Policy', 'SI', 'IDEAN', 'Financiere_Adh', 'PERIMETRE', 'SOURCE_SBD')
flag_CQS_CBP_2 = flag_CQS_CBP_2.dropDuplicates(['Scheme', 'PMP_Sales_Name', 'PAP_Product', 'RPP', 'Macao_Group_Policy', 'SI', 'IDEAN', 'Financiere_Adh', 'PERIMETRE', 'SOURCE_SBD'])
flag_CQS_CBP_2.createOrReplaceTempView('flag_CQS_CBP_2')

flag_cqs_cbp = spark.sql("""select "IT" as Country ,Scheme,RPP, regexp_replace(Macao_Group_Policy, ' ', '') as Macao_Group_Policy,IDEAN, PMP_Sales_Name,Financiere_Adh as Financiere_Adh_Info_DL  ,PAP_Product, SI, PERIMETRE, SOURCE_SBD 
	from flag_CQS_CBP_2 
	where PERIMETRE not in (" ")""")
flag_cqs_cbp.createOrReplaceTempView('flag_cqs_cbp')

carto_cqs_cbp = spark.table('flag_cqs_cbp')
carto_cqs_cbp.createOrReplaceTempView('carto_cqs_cbp')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
carto_cqs_cbp.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.carto_cqs_cbp')
