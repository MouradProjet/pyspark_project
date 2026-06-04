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

# Cession
n = 2025
arrete = "2025_09_Q4"
exer = "Q425"
cqs_out_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
cqs_sin_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves"  # LIBNAME CQS_SIN
# création d'une base avec tous les postes pour appliquer la cession
# MISE EN FORME BASE CY
# ici on enleve toutes PPNA et FAR qui ne viennent pas de la topline regarder code construction bgd CY
donnees_CY = spark.table('set')
# cqs_out.
# where poste in("PRIMES","PSAP","SINISTRES","PPNA");
donnees_CY = donnees_CY.select('IDEAN', 'generation', 'surv', 'gar', 'poste', 'Montant')
donnees_CY.createOrReplaceTempView('donnees_CY')

# Mise en forme de la base PY
# UEP
UEP = spark.sql("""select 
idean,gen as Generation, SURV, gar, Type_Pret,sum(UEP) as PPNA 
from cqs_out.cqs_uep_pl_3
group by idean, Generation,SURV, gar, Type_Pret """)
UEP.createOrReplaceTempView('UEP')

UEP = spark.table('UEP')
UEP = UEP.filter(~F.expr("""idean IS NULL"""))
UEP = UEP.filter(~F.expr("""idean=1"""))
UEP.createOrReplaceTempView('UEP')

UEP = spark.table('UEP').orderBy('idean', 'Generation', 'SURV', 'gar', 'Type_Pret')
UEP.createOrReplaceTempView('UEP')

# PROC TRANSPOSE
# wide-to-long: 1 columns -> 1 rows (_NAME_ = column name, COL1 = value)
UEP = UEP.select('idean', 'Generation', 'SURV', 'gar', 'Type_Pret', F.expr("""stack(1, 'PPNA', `PPNA`) as (_NAME_, COL1)"""))
UEP.createOrReplaceTempView('UEP')

UEP = spark.table('UEP')
UEP = UEP.withColumnRenamed('_NAME_', 'POSTE')
UEP = UEP.withColumnRenamed('COL1', 'Montant')
UEP.createOrReplaceTempView('UEP')

# PSAP
reserves = spark.sql("""select 
idean, gar,surv,generation as Generation, type_pret,sum(reserves) as PSAP
from  cqs_sin.claims_actuals_9
group by idean, gar, surv, Generation, type_pret """)
reserves.createOrReplaceTempView('reserves')

reserves = spark.table('reserves').orderBy('idean', 'Generation', 'SURV', 'gar', 'Type_Pret')
reserves.createOrReplaceTempView('reserves')

# PROC TRANSPOSE
# wide-to-long: 1 columns -> 1 rows (_NAME_ = column name, COL1 = value)
reserves = reserves.select('idean', 'Generation', 'SURV', 'gar', 'Type_Pret', F.expr("""stack(1, 'PSAP', `PSAP`) as (_NAME_, COL1)"""))
reserves.createOrReplaceTempView('reserves')

reserves = spark.table('reserves')
reserves = (reserves
    .withColumn('idean_char', F.col('idean').cast('string'))
)
reserves = reserves.drop('idean')
reserves = reserves.withColumnRenamed('_NAME_', 'POSTE')
reserves = reserves.withColumnRenamed('COL1', 'Montant')
reserves = reserves.withColumnRenamed('idean_char', 'idean')
reserves.createOrReplaceTempView('reserves')

# consolidation de la base PY clot cession
donnees_py = spark.table('uep') \
    .union(spark.table('reserves'))
# FORMAT/INFORMAT: format POSTE $15.
donnees_py.createOrReplaceTempView('donnees_py')

# DATA donnees_cy;
# set cqs_out.bgd_idean_cqs_cbp_cy ;
# where poste in("PPNA","PSAP") ;
# run;
# TRAITE CQP PY
def split_cess_cqp(cess, taux):
    _dfs[f'cess_py_cqp_{cess}'] = spark.table('donnees_py')
    _dfs[f'cess_py_cqp_{cess}'] = _dfs[f'cess_py_cqp_{cess}'].filter(F.expr("""2019<=generation<=2022 AND type_pret="CQP" AND gar=10"""))
    _dfs[f'cess_py_cqp_{cess}'] = (_dfs[f'cess_py_cqp_{cess}']
        .withColumn('cess',
            F.when(F.expr("""poste IN ("PPNA";__COMMENT__0;)"""), F.expr(f"""-montant*(1-0.276)*{taux}"""))
             .when(F.expr("""poste IN (;__COMMENT__1;"PSAP")"""), F.expr(f"""-montant*{taux}""")))
        .withColumn('code_cess', F.lit("{cess}"))
    )
    _dfs[f'cess_py_cqp_{cess}'].createOrReplaceTempView(f'cess_py_cqp_{cess}')


split_cess_cqp("RGA", "0.9")
# TRAITE INFORCE PY
# On n'enleve pas les frais des UEP cédés pour le traite inforce, c'est pas du new business, que
# du run-off
# V2 DU CODE INFORCE
def split_cess_inforce(cess, taux):
    _dfs[f'cess_py_traite_inforce_{cess}'] = spark.table('donnees_py')
    _dfs[f'cess_py_traite_inforce_{cess}'] = (_dfs[f'cess_py_traite_inforce_{cess}']
        .withColumn('cess', F.col('NULL'))
        .withColumn('code_cess', F.lit("{cess}"))
    )
        # IF/THEN (manual review needed): if generation <= 2022 and type_pret not in ("CQP") then do ; if poste in("PPNA") then cess = -montant * 0.96 * {taux} ; else if surv > 2022 and poste in ("PSAP") then cess = -montant * {taux} ; output ; end
        # IF/THEN (manual review needed): if generation <= 2018 and type_pret = "CQP" then do ; if poste in("PPNA") then cess = -montant * {taux} * 0.96 ; else if surv > 2022 and poste in ("PSAP") then cess = -montant * {taux} ; output ; end
        # IF/THEN (manual review needed): if 2019 <= generation <= 2022 and type_pret = "CQP" and gar = 30 then do ; if poste in("PPNA") then cess = -montant * {taux} * 0.96 ; else if surv > 2022 and poste in ("PSAP") then cess = -montant * {taux} ; output ; end
    _dfs[f'cess_py_traite_inforce_{cess}'].createOrReplaceTempView(f'cess_py_traite_inforce_{cess}')


split_cess_inforce("MAPFRE", "0.09")
split_cess_inforce("RGA", "0.45")
split_cess_inforce("SCOR", "0.1")
split_cess_inforce("ARCH", "0.05")
# TRAITE REASS GENERATION 2023---> 2025
def split_cess_newb(cess, taux):
    _dfs[f'cess_py_newb_{cess}'] = spark.table('donnees_py')
    _dfs[f'cess_py_newb_{cess}'] = _dfs[f'cess_py_newb_{cess}'].filter(F.expr("""2023<=generation<2026"""))
    _dfs[f'cess_py_newb_{cess}'] = (_dfs[f'cess_py_newb_{cess}']
        .withColumn('cess', F.col('NULL'))
        .withColumn('code_cess', F.lit("{cess}"))
        .withColumn('cess', F.when(F.expr("""poste IN ("PPNA")"""), F.expr(f"""-montant*(1-0.276)*{taux}""")))  # no ELSE: null when condition is false
        .withColumn('cess', F.when(F.expr("""poste IN ("PSAP")"""), F.expr(f"""-montant*{taux}""")))  # no ELSE: null when condition is false
    )
    # ici pas de psap et sinistres surv N = 2026 à rajouter
    _dfs[f'cess_py_newb_{cess}'].createOrReplaceTempView(f'cess_py_newb_{cess}')


split_cess_newb("RGA", "0.4")
split_cess_newb("SCOR", "0.1")
split_cess_newb("MAPFRE", "0.1")
split_cess_newb("GEN_RE", "0.1")
# TRAITE NEW BUSINESS GENERATION 2026
def split_cess_cy(cess, taux):
    _dfs[f'cess_cy_cess_{cess}'] = spark.table('donnees_cy')
    _dfs[f'cess_cy_cess_{cess}'] = _dfs[f'cess_cy_cess_{cess}'].filter(F.expr("""POSTE IN ("PRIMES","SINISTRES","PSAP","PPNA")"""))
    _dfs[f'cess_cy_cess_{cess}'] = (_dfs[f'cess_cy_cess_{cess}']
        .withColumn('cess', F.col('NULL'))
        .withColumn('code_cess', F.lit("{cess}"))
        .withColumn('cess', F.when(F.expr("""poste IN ("PRIMES","SINISTRES","PSAP","PPNA")"""), F.expr(f"""-montant*{taux}""")))  # no ELSE: null when condition is false
        .withColumn('code_cess', F.lit("{cess}"))
    )
    _dfs[f'cess_cy_cess_{cess}'].createOrReplaceTempView(f'cess_cy_cess_{cess}')


split_cess_cy("GEN_RE", "0.35")
split_cess_cy("HANOVER_RE", "0.15")
split_cess_cy("MAPFRE", "0.1")
cess_cy_commacq1 = spark.table('cess_cy_cess_hanover_re') \
    .union(spark.table('cess_cy_cess_gen_re') \
    .union(spark.table('cess_cy_cess_mapfre')))
cess_cy_commacq1 = cess_cy_commacq1.filter(F.expr("""poste ="PRIMES""""))
cess_cy_commacq1 = (cess_cy_commacq1
    .withColumn('COMMACQ_CD', F.expr("""cess*0.2"""))
)
cess_cy_commacq1 = cess_cy_commacq1.select('idean', 'gar', 'surv', 'generation', 'code_cess', 'COMMACQ_CD')
cess_cy_commacq1.createOrReplaceTempView('cess_cy_commacq1')

cess_cy_commacq = spark.table('cess_cy_commacq1')
cess_cy_commacq = (cess_cy_commacq
    .withColumn('POSTE', F.lit("COMMACQ_CD"))
    .withColumn('Montant', F.lit(0))
)
cess_cy_commacq = cess_cy_commacq.withColumnRenamed('COMMACQ_CD', 'cess')
cess_cy_commacq.createOrReplaceTempView('cess_cy_commacq')

cess_cy_far1 = spark.table('cess_cy_commacq')
cess_cy_far1 = (cess_cy_far1
    .withColumn('FAR_CD', F.expr("""(((10-0)*(10-1))/(10*11))*cess"""))
)
cess_cy_far1 = cess_cy_far1.select('idean', 'gar', 'surv', 'generation', 'code_cess', 'FAR_CD')
cess_cy_far1.createOrReplaceTempView('cess_cy_far1')

cess_cy_far = spark.table('cess_cy_far1')
cess_cy_far = (cess_cy_far
    .withColumn('POSTE', F.lit("FAR_CD"))
    .withColumn('Montant', F.lit(0))
)
cess_cy_far = cess_cy_far.withColumnRenamed('FAR_CD', 'cess')
cess_cy_far.createOrReplaceTempView('cess_cy_far')

# CONSOLIDATION DES SORTIES
bgd_cqs_cession1 = spark.table('cess_py_cqp_rga') \
    .union(spark.table('cess_py_traite_inforce_mapfre') \
    .union(spark.table('cess_py_traite_inforce_arch') \
    .union(spark.table('cess_py_traite_inforce_rga') \
    .union(spark.table('cess_py_traite_inforce_scor') \
    .union(spark.table('cess_py_newb_gen_re') \
    .union(spark.table('cess_py_newb_mapfre') \
    .union(spark.table('cess_py_newb_rga') \
    .union(spark.table('cess_py_newb_scor') \
    .union(spark.table('cess_cy_cess_gen_re') \
    .union(spark.table('cess_cy_cess_hanover_re') \
    .union(spark.table('cess_cy_cess_mapfre') \
    .union(spark.table('cess_cy_commacq') \
    .union(spark.table('cess_cy_far'))))))))))))))
# ATTRIB: attrib code_cess length=$20 format=$20. informat=$20.
bgd_cqs_cession1.createOrReplaceTempView('bgd_cqs_cession1')

bgd_cqs_cession = spark.table('bgd_cqs_cession1')
bgd_cqs_cession = (bgd_cqs_cession
    .withColumn('code_cess', F.when(F.expr("""code_cess="MAPFRE""""), F.lit(91)))  # no ELSE: null when condition is false
    .withColumn('code_cess', F.when(F.expr("""code_cess="RGA""""), F.lit(187)))  # no ELSE: null when condition is false
    .withColumn('code_cess', F.when(F.expr("""code_cess="SCOR""""), F.lit(62)))  # no ELSE: null when condition is false
    .withColumn('code_cess', F.when(F.expr("""code_cess="ARCH""""), F.lit(100)))  # no ELSE: null when condition is false
    .withColumn('code_cess', F.when(F.expr("""code_cess="GEN_RE""""), F.lit(75)))  # no ELSE: null when condition is false
    .withColumn('typaff', F.lit(11))
)
# VUE="SSI_CLOT{n}";
# drop montant;
bgd_cqs_cession.createOrReplaceTempView('bgd_cqs_cession')

# DAta bgd_cqs_cession ;
# set bgd_cqs_cession ;
# rename cess=Montant;
# VUE="SSI_CLOT&N.";
# run;
work = spark.table('bgd_cqs_cession')
work = (work
    .withColumn('country', F.lit("IT"))
    .withColumn('TYPIDEAN', F.lit("DI"))
    .withColumn('NIVEAU', F.lit("T"))
    .withColumn('SI', F.lit("CQS"))
    .withColumn('PERIMETRE', F.lit("CQS"))
    .withColumn('SOURCE', F.lit(" NULL "))
    .withColumn('DEVISE', F.lit("EUR"))
    .withColumn('VUE',
        F.when(F.expr(f"""poste IN ("PRIMES","SINISTRES","CANCEL") AND surv={n}"""), F.lit("FAI_CPTA{n}"))
         .when(F.expr(f"""poste IN ("PRIMES","SINISTRES","CANCEL") AND surv<{n}"""), F.lit("RAI_CPTA{n}"))
         .otherwise(F.lit("SSI_CLOT{n}")))
    .withColumn('ENTITY',
        F.when(F.expr("""gar =10"""), F.lit("FICL"))
         .when(F.expr("""gar=30"""), F.lit("FACL")))
)
# cqs_out.
# rename code_cess=CESS;
work.createOrReplaceTempView('work')

bgd_cqs_cession = spark.table('bgd_cqs_cession')
bgd_cqs_cession = (bgd_cqs_cession
    .withColumn('CESS_num', F.col('CESS').cast('double'))
)
    # SAS PUT (debug): CESS_num
bgd_cqs_cession = bgd_cqs_cession.drop('CESS')
bgd_cqs_cession = bgd_cqs_cession.withColumnRenamed('CESS_num', 'CESS')
bgd_cqs_cession.createOrReplaceTempView('bgd_cqs_cession')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_cqs_cession.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_cqs_cession')

# CESSION DES BM DE SINISTRES ET DE PRIMES
# TRAITE NEW BUSINESS
# SINISTRES
def cess_newb_bm_sin(cess, taux):
    _dfs[f'cession_bm_sinistres_{cess}'] = spark.table('set')
    _dfs[f'cession_bm_sinistres_{cess}'] = _dfs[f'cession_bm_sinistres_{cess}'].filter(F.expr("""POSTE="SINISTRES" AND surv IN (2023,2024,2025)"""))
    _dfs[f'cession_bm_sinistres_{cess}'] = (_dfs[f'cession_bm_sinistres_{cess}']
        .withColumn('code_cess', F.lit("{cess}"))
        .withColumn('cess', F.expr(f"""-montant*{taux}"""))
    )
    # cqs_out.
    # attention a changer le nom de la base chaque arrêté
    _dfs[f'cession_bm_sinistres_{cess}'].createOrReplaceTempView(f'cession_bm_sinistres_{cess}')


cess_newb_bm_sin("MAPFRE", "0.1")
cess_newb_bm_sin("RGA", "0.4")
cess_newb_bm_sin("SCOR", "0.1")
cess_newb_bm_sin("GEN_RE", "0.1")
# PRIMES
def cess_newb_bm_primes(cess, taux):
    _dfs[f'cession_bm_primes_{cess}'] = spark.table('bgd_idean_cqs_cbp_py')
    _dfs[f'cession_bm_primes_{cess}'] = _dfs[f'cession_bm_primes_{cess}'].filter(F.expr(f"""surv>=2023 AND surv<{n} AND poste="PRIMES""""))
    _dfs[f'cession_bm_primes_{cess}'] = (_dfs[f'cession_bm_primes_{cess}']
        .withColumn('cess', F.expr(f"""-montant*(1-0.276)*{taux}"""))
        .withColumn('code_cess', F.lit("{cess}"))
    )
    _dfs[f'cession_bm_primes_{cess}'].createOrReplaceTempView(f'cession_bm_primes_{cess}')


cess_newb_bm_primes("RGA", "0.4")
cess_newb_bm_primes("SCOR", "0.1")
cess_newb_bm_primes("MAPFRE", "0.1")
cess_newb_bm_primes("GEN_RE", "0.1")
# CONSOLIDATION DE LA CESSION DES BM
# 3 bases BM et cy anciennes gen et cy nouvelle gen
bgd_cqs_cession_bm = spark.table('cession_bm_sinistres_gen_re') \
    .union(spark.table('cession_bm_sinistres_mapfre') \
    .union(spark.table('cession_bm_sinistres_rga') \
    .union(spark.table('cession_bm_sinistres_scor') \
    .union(spark.table('cession_cy_sinistres_gen_re') \
    .union(spark.table('cession_cy_sinistres_mapfre') \
    .union(spark.table('cession_cy_sinistres_rga') \
    .union(spark.table('cession_cy_sinistres_scor') \
    .union(spark.table('cession_bm_primes_gen_re') \
    .union(spark.table('cession_bm_primes_mapfre') \
    .union(spark.table('cession_bm_primes_rga') \
    .union(spark.table('cession_bm_primes_scor') \
    .union(spark.table('cession_cy_primes_gen_re') \
    .union(spark.table('cession_cy_primes_mapfre') \
    .union(spark.table('cession_cy_primes_rga') \
    .union(spark.table('cession_cy_primes_scor'))))))))))))))))
bgd_cqs_cession_bm.createOrReplaceTempView('bgd_cqs_cession_bm')

bgd_cqs_cession_bm = spark.table('bgd_cqs_cession_bm')
bgd_cqs_cession_bm = (bgd_cqs_cession_bm
    .withColumn('CESS2', F.when(F.expr("""code_cess="MAPFRE""""), F.lit(91)))  # no ELSE: null when condition is false
    .withColumn('CESS2', F.when(F.expr("""code_cess="RGA""""), F.lit(187)))  # no ELSE: null when condition is false
    .withColumn('CESS2', F.when(F.expr("""code_cess="SCOR""""), F.lit(62)))  # no ELSE: null when condition is false
    .withColumn('CESS2', F.when(F.expr("""code_cess="ARCH""""), F.lit(100)))  # no ELSE: null when condition is false
    .withColumn('CESS2', F.when(F.expr("""code_cess="GEN_RE""""), F.lit(75)))  # no ELSE: null when condition is false
    .withColumn('typaff', F.lit(11))
)
bgd_cqs_cession_bm = bgd_cqs_cession_bm.drop('montant')
bgd_cqs_cession_bm = bgd_cqs_cession_bm.withColumnRenamed('cess', 'Montant')
bgd_cqs_cession_bm.createOrReplaceTempView('bgd_cqs_cession_bm')

bgd_cqs_cession_bm = spark.table('bgd_cqs_cession_bm')
bgd_cqs_cession_bm = (bgd_cqs_cession_bm
    .withColumn('country', F.lit("IT"))
    .withColumn('TYPIDEAN', F.lit("DI"))
    .withColumn('NIVEAU', F.lit("T"))
    .withColumn('SI', F.lit("CQS"))
    .withColumn('PERIMETRE', F.lit("CQS"))
    .withColumn('SOURCE', F.lit(" NULL "))
    .withColumn('DEVISE', F.lit("EUR"))
    .withColumn('VUE', F.lit("RAI_CPTA{n}"))
)
bgd_cqs_cession_bm = bgd_cqs_cession_bm.withColumnRenamed('CESS2', 'CESS')
bgd_cqs_cession_bm.createOrReplaceTempView('bgd_cqs_cession_bm')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_cqs_cession_bm.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_cqs_cession_bm')

# CONSOLIDATION DES BASE PY ET CY EN VISION ULTIMATE
# RECUPERATION DES OUV
cqs_ouv_2025 = spark.table('bgd_idean_q425_final_2')
cqs_ouv_2025 = cqs_ouv_2025.filter(F.expr("""VUE="SSI_OUV2025" AND SI="CQS""""))
cqs_ouv_2025 = cqs_ouv_2025.select('TYPAFF', 'TYPIDEAN', 'GAR', 'Niveau', 'VUE', 'SURV', 'CESS', 'country', 'entity', 'Source', 'DEVISE', 'Montant', 'PERIMETRE', 'SI', 'IDEAN', 'POSTE')
cqs_ouv_2025.createOrReplaceTempView('cqs_ouv_2025')

# /
bgd_idean_cqs_cbp_yc_cess = spark.table('bgd_idean_cqs_cbp1') \
    .union(spark.table('bgd_cqs_cession') \
    .union(spark.table('bgd_cqs_cession_bm') \
    .union(spark.table('bgd_idean_cqs_cbp_yc_cess'))))
bgd_idean_cqs_cbp_yc_cess = (bgd_idean_cqs_cbp_yc_cess
    .withColumn('PERIMETRE', F.lit("CQS"))
)
bgd_idean_cqs_cbp_yc_cess = bgd_idean_cqs_cbp_yc_cess.drop('generation', 'type_pret', 'code_cess', 'cess2')
bgd_idean_cqs_cbp_yc_cess.createOrReplaceTempView('bgd_idean_cqs_cbp_yc_cess')
# LIBNAME bgd_q425 -> Unity Catalog: {_catalog}.bgd_q425
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.bgd_q425')
bgd_idean_cqs_cbp_yc_cess.write.mode('overwrite').saveAsTable(f'{{_catalog}}.bgd_q425.bgd_idean_cqs_cbp_yc_cess')
