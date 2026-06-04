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

n = 2025
exer = "Q425"
type_exer = "FY"
# soit FC2, FC3, HY ou FY selon le type d'arrêté:sert à réuperer les colonnes de la TOPLINE
arrete = "2025_09_Q4"
arrete_n_1 = "2025_04_V2"
nom_base_anterio = "base_anteriorite_idean_2025"
arrete_n_2 = "2024_09_Q4"
topline = "092025 DAVC SHUTTLE FILE FY25 V3"
sdb = "Updated SDB Data Files 03.09.2025"
# Renseigner le nom de la SDB
taux_comm_vie = 0.0991
# Taux du CY =taux moyen gen 2025 envoi BU
taux_comm_iard = 0.0998
# Taux du CY =taux moyen gen 2025 envoi BU
lr_vie = 0.970030461860269
# LR N-1 dans l'onglet summary fin qu'on reporte sur le courant
lr_iard = 0.605028051559256
# LR N-1 dans l'onglet summary fin qu'on reporte sur le courant
sin_dc = 0.597752359690615
# Moyenne du ratio prestations/ charge ultime sur les 5 dernières années de l'onglet "DC triangle"
sin_iu = 0.173594390137475
# Moyenne ratio prestations/ charge ultime sur les 5 dernières années de l'onglet "chomage triangle"
anterio_path = "~/NAS/X/08.Progammes/Etablissements financiers/OUTIL INVENTAIRE/Base anteriorite"  # LIBNAME ANTERIO
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.anterio')  # Unity Catalog schema for ANTERIO
cqs_out_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
cqs_base_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP"  # LIBNAME CQS_Base
cqs_sin_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves"  # LIBNAME CQS_SIN
outil_path = "~/NAS/X/08.Progammes/Etablissements financiers/OUTIL INVENTAIRE/Outil Inventaire Final/SAS/Bibliotheques SAS/Outil"  # LIBNAME OUTIL
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.outil')  # Unity Catalog schema for OUTIL
cqshy25_path = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2025_04_V2/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQSHY25
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.cqshy25')  # Unity Catalog schema for CQSHY25
bgd_q424_path = "~/NAS/X/08.Progammes/Etablissements financiers/OUTIL INVENTAIRE/Outil Inventaire Final/SAS/Bibliotheques SAS/Bases Globales Q424"  # LIBNAME BGD_Q424
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.bgd_q424')  # Unity Catalog schema for BGD_Q424
bgd_v225_path = "~/NAS/X/08.Progammes/Etablissements Financiers/OUTIL INVENTAIRE/Outil Inventaire Final/SAS/Bibliotheques SAS/Bases Globales V225"  # LIBNAME BGD_V225
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.bgd_v225')  # Unity Catalog schema for BGD_V225
# /
def import_excel(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


# Topline CY
import_01 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/Topline/{topline}.xlsx"
import_excel(datafile=import_01, out="Topline_Macao", onglet="Combined")
# import de la nouvelle table id
def import_excel(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


id_ = "~/NAS/X/08.Progammes/INTERNATIONAL/10_TABLE_ID/Table_ID.xlsx"
import_excel(datafile=id_, out="tabid_new", onglet="Table ID")
# On ramene les stop loss
import_sl = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/GEP Stop Loss.xlsx"
import_excelx(datafile=import_sl, out="GSL", onglet="Feuil2")
# On ramene les cancellations
import_cxls = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Topline/{topline}.xlsx"
import_excelx(datafile=import_cxls, out="cancelation", onglet="7. Crdr Di CXLS")
# import cle rachat shortcut
import_clerachat = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/1. Travaux/Output_CQS_FY25.xlsx"
import_excelx(datafile=import_clerachat, out="cle_rachat", onglet="cle_rachat")
# on a besoin que du nom de la sdb pour tourner ce bout de code
# %let rep_programme=~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete/02_Elements_techniques/Macao/CQS CBP Process/04 - Construction BGD;
# FILENAME Mapping "&rep_programme./Mapping_CQS_CBP.sas" LRECL =32767;
# %include Mapping;
# /
# Création du flag cbp_cqs
tab_cqs_idean = spark.sql("""select distinct idean, country, 1 as flag_cbp
      from cqs_out.carto_cqs""")
tab_cqs_idean.createOrReplaceTempView('tab_cqs_idean')

_dfs[f'base_anteriorite_idean_{n}'] = spark.sql(f"""select t1.*
                from ANTERIO.base_anteriorite_idean_{n} t1 
                where SI = "Macao" and surv < {n}""")
_dfs[f'base_anteriorite_idean_{n}'].createOrReplaceTempView(f'base_anteriorite_idean_{n}')

_dfs[f'OUV_{n}'] = spark.table(f'base_anteriorite_idean_{n}')
_dfs[f'OUV_{n}'] = _dfs[f'OUV_{n}'].filter(F.expr(f"""CPTA = {n} - 1  AND
poste IN ('PPNA', 'PTEC', 'MARGES', 'PSAP', 'PPRC', 'FAR')"""))
_dfs[f'OUV_{n}'] = (_dfs[f'OUV_{n}']
    .withColumn('VUE', F.lit("SSI_OUV{n}"))
    .withColumn('NIVEAU', F.lit("T"))
    .withColumn('SURV', F.when(F.expr("""POSTE = "PPRC""""), F.expr(f"""{n}""")))  # no ELSE: null when condition is false
    .withColumn('PERIMETRE', F.lit("Macao"))
)
_dfs[f'OUV_{n}'] = _dfs[f'OUV_{n}'].drop('CPTA')
_dfs[f'OUV_{n}'].createOrReplaceTempView(f'OUV_{n}')

_dfs[f'OUV_{n}_2'] = spark.sql(f"""select t1.*, t2.PMP_Sales_Name 
                from OUV_{n} as t1
                left join tabid_new as t2 on t1.typidean = t2.typidean and t1.idean = t2.idean and t1.country = t2.country_code""")
_dfs[f'OUV_{n}_2'].createOrReplaceTempView(f'OUV_{n}_2')

ouv_PPNA_CESS = spark.sql(f"""select t1.*
                from ouv_{n}_2 as t1
                left join tab_cqs_idean as t2 on t1.idean = t2.idean and t1.typidean = "DI" and t1.country = t2.country
                where t2.flag_cbp = 1 and typaff = 11 and poste in ("PPNA","FAR")""")
ouv_PPNA_CESS.createOrReplaceTempView('ouv_PPNA_CESS')

ouv_PSAP_CESS = spark.sql(f"""select t1.*
                from ouv_{n}_2 as t1
                left join tab_cqs_idean as t2 on t1.idean = t2.idean and t1.typidean = "DI" and t1.country = t2.country
                where t2.flag_cbp = 1 and typaff = 11 and poste="PSAP"""")
ouv_PSAP_CESS.createOrReplaceTempView('ouv_PSAP_CESS')

# /
TABID_CBP = spark.table('TABID_NEW')
TABID_CBP = TABID_CBP.filter(F.expr("""Flag_CBP=1"""))
TABID_CBP.createOrReplaceTempView('TABID_CBP')

# data FLAG_Macao_ ;
# set FLAG_Macao;
# attrib IDEAN length= $20 format = $20. informat =$20.;
# IDEAN = Scheme;
# IDEAN_process = substr(RPP,1,5)*1;
# keep  IDEAN IDEAN_process;
# run;
# proc sort data= FLAG_Macao_ nodupkey ; by IDEAN; run ;
cancelation = spark.table('cancelation').orderBy('IDEAN')
cancelation.createOrReplaceTempView('cancelation')

TABID_CBP = spark.table('TABID_CBP').orderBy('IDEAN')
TABID_CBP.createOrReplaceTempView('TABID_CBP')

# data cancelation_ ;
# Merge cancelation (in=a)  FLAG_Macao_ (in =b ) TABID_CBP (in =c);
# if a;
# by IDEAN;
# montant_&N. = _&type_exer._&N._LC  ;
# if RGPT ="" then delete;
# run;
carto_cqs = spark.table('carto_cqs')
carto_cqs = (carto_cqs
    .withColumn('IDEAN_process', F.col('IDEAN').cast('long'))
)
carto_cqs.createOrReplaceTempView('carto_cqs')

# ici je mets distinct pour eviter les doublons
cancelation2 = spark.sql("""select distinct t1.*, t2.idean,t2.IDEAN_process
from cancelation t1
left join carto_cqs  t2 on (t1.IDEAN=t2.idean) 
where t2.idean not in (" ") """)
cancelation2.createOrReplaceTempView('cancelation2')

cancelation_ = spark.table('cancelation2')
    # MANUAL REVIEW: indexed column from macro loop — use df.withColumn(f'montant_{n}', ...) inside a Python for-loop
    # SAS: montant_{n} =_2025_FY_LC
# mettre au format de la topline
# ? *if RGPT ="" then delete
cancelation_.createOrReplaceTempView('cancelation_')

# Ajout FY23 : répartition des cancelations par génération sur la survenance 2023 en utilisant une clé_rachat
# DETERMINATION DE LA CLE RACHAT VIE : Ajout V1_2024
CQS_detail = spark.table('cqs_gep_2_bis')
CQS_detail.createOrReplaceTempView('CQS_detail')

cle_rachat_vie = spark.table('CQS_detail')
cle_rachat_vie = cle_rachat_vie.filter(F.expr(f"""surv ={n} AND gar=10"""))
cle_rachat_vie.createOrReplaceTempView('cle_rachat_vie')

cle_rachat_vie = spark.sql("""select distinct Generation, GAR, SURV, sum(Rachat) as rachat
from cle_rachat_vie 
group by Generation, GAR, SURV """)
cle_rachat_vie.createOrReplaceTempView('cle_rachat_vie')

cle_rachat_vie = spark.sql("""select distinct Generation, GAR, SURV, rachat,sum(Rachat) as total_rachat
from cle_rachat_vie 
group by GAR, SURV """)
cle_rachat_vie.createOrReplaceTempView('cle_rachat_vie')

cle_rachat_vie = spark.table('cle_rachat_vie')
cle_rachat_vie = (cle_rachat_vie
    .withColumn('cle_rachat_vie', F.expr("""rachat/total_rachat"""))
)
cle_rachat_vie.createOrReplaceTempView('cle_rachat_vie')

# DETERMINATION DE LA CLE RACHAT NON VIE
CQS_detail = spark.table('cqs_gep_2_bis')
CQS_detail.createOrReplaceTempView('CQS_detail')

cle_rachat_non_vie = spark.table('CQS_detail')
cle_rachat_non_vie = cle_rachat_non_vie.filter(F.expr(f"""surv ={n} AND gar=30"""))
cle_rachat_non_vie.createOrReplaceTempView('cle_rachat_non_vie')

cle_rachat_non_vie = spark.sql("""select distinct Generation, GAR, SURV, sum(Rachat) as rachat
from cle_rachat_non_vie 
group by Generation, GAR, SURV """)
cle_rachat_non_vie.createOrReplaceTempView('cle_rachat_non_vie')

cle_rachat_non_vie = spark.sql("""select distinct Generation, GAR, SURV, rachat,sum(Rachat) as total_rachat
from cle_rachat_non_vie 
group by GAR, SURV """)
cle_rachat_non_vie.createOrReplaceTempView('cle_rachat_non_vie')

cle_rachat_non_vie = spark.table('cle_rachat_non_vie')
cle_rachat_non_vie = (cle_rachat_non_vie
    .withColumn('cle_rachat_non_vie', F.expr("""rachat/total_rachat"""))
)
cle_rachat_non_vie.createOrReplaceTempView('cle_rachat_non_vie')

cle_rachat_cqs = spark.sql("""select distinct a.Generation,a.cle_rachat_vie , 
b.cle_rachat_non_vie,b.generation
from cle_rachat_vie a left join cle_rachat_non_vie b 
on a.generation=b.generation""")
cle_rachat_cqs.createOrReplaceTempView('cle_rachat_cqs')

# renommer la variable gen en surv ici on fait l'hyp que gen=surv
cle_rachat_cqs = spark.table('cle_rachat')
# rename generation=SURV;
cle_rachat_cqs.createOrReplaceTempView('cle_rachat_cqs')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
cle_rachat_cqs.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.cle_rachat_cqs')

# exporter la table
export_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/Macao/CQS CBP Process/02 - GEP/Output_GEP_Com_new.xlsx"
export_excelx(database=export_02, datatable=cle_rachat_cqs, sheet="cle_rachat )
				

/*********répartition vie*****/
proc sql;
create table cancelation_vie as
select t3.*
from cancelation_ t3
where t3.VIE_IARD in ("VIE")
cancelation_vie2 = spark.sql(f"""select distinct IDEAN,sum(montant_{n}) as Rachat_off_systeme
from cancelation_vie
group by IDEAN""")
cancelation_vie2.createOrReplaceTempView('cancelation_vie2')

cle_rachat_vie = spark.table('cle_rachat_cqs')
cle_rachat_vie = cle_rachat_vie.select('SURV', 'cle_rachat_vie')
cle_rachat_vie.createOrReplaceTempView('cle_rachat_vie')

cancelation_vie3 = spark.sql("""select *
from  cle_rachat_vie, cancelation_vie2""")
cancelation_vie3.createOrReplaceTempView('cancelation_vie3')

cancelation_vie4 = spark.table('cancelation_vie3')
cancelation_vie4 = (cancelation_vie4
    .withColumn('Rachat_off_systeme_2', F.expr("""cle_rachat_vie*Rachat_off_systeme"""))
)
cancelation_vie4.createOrReplaceTempView('cancelation_vie4')

cancelation_vie5 = spark.table('cancelation_vie4')
cancelation_vie5 = (cancelation_vie5
    .withColumn('VIE_IARD', F.lit("VIE"))
)
cancelation_vie5 = cancelation_vie5.drop('Rachat_off_systeme')
cancelation_vie5 = cancelation_vie5.withColumnRenamed('Rachat_off_systeme', 'Rachat_off_systeme_2')
cancelation_vie5.createOrReplaceTempView('cancelation_vie5')

# répartition iard
cancelation_iard = spark.sql("""select t3.*
from cancelation_ t3
where t3.VIE_IARD in ("IARD")""")
cancelation_iard.createOrReplaceTempView('cancelation_iard')

cancelation_iard2 = spark.sql(f"""select distinct IDEAN ,sum(montant_{n}) as Rachat_off_systeme
from cancelation_iard
group by IDEAN""")
cancelation_iard2.createOrReplaceTempView('cancelation_iard2')

cle_rachat_iard = spark.table('cle_rachat_cqs')
cle_rachat_iard = cle_rachat_iard.select('SURV', 'cle_rachat_non_vie')
cle_rachat_iard.createOrReplaceTempView('cle_rachat_iard')

cancelation_iard3 = spark.sql("""select *
from  cle_rachat_iard, cancelation_iard2""")
cancelation_iard3.createOrReplaceTempView('cancelation_iard3')

cancelation_iard4 = spark.table('cancelation_iard3')
cancelation_iard4 = (cancelation_iard4
    .withColumn('Rachat_off_systeme_2', F.expr("""cle_rachat_non_vie*Rachat_off_systeme"""))
)
cancelation_iard4.createOrReplaceTempView('cancelation_iard4')

cancelation_iard5 = spark.table('cancelation_iard4')
cancelation_iard5 = (cancelation_iard5
    .withColumn('VIE_IARD', F.lit("IARD"))
)
cancelation_iard5 = cancelation_iard5.drop('Rachat_off_systeme')
cancelation_iard5 = cancelation_iard5.withColumnRenamed('Rachat_off_systeme', 'Rachat_off_systeme_2')
cancelation_iard5.createOrReplaceTempView('cancelation_iard5')

cancelation_2 = spark.table('cancelation_iard5') \
    .union(spark.table('cancelation_vie5'))
cancelation_2.createOrReplaceTempView('cancelation_2')

cancelation_2bis = spark.table('cancelation_2')
cancelation_2bis = cancelation_2bis.drop('cle_rachat_non_vie', 'cle_rachat_vie')
cancelation_2bis.createOrReplaceTempView('cancelation_2bis')

carto_anterio_cqs = spark.table('carto_cqs')
# PMP_Sales_Name
carto_anterio_cqs = carto_anterio_cqs.select('IDEAN', 'PERIMETRE')
carto_anterio_cqs.createOrReplaceTempView('carto_anterio_cqs')

carto_anterio_cqs = spark.table('carto_anterio_cqs').orderBy('IDEAN')
carto_anterio_cqs = carto_anterio_cqs.dropDuplicates(['IDEAN'])
carto_anterio_cqs.createOrReplaceTempView('carto_anterio_cqs')

_dfs[f'base_anteriorite_CQS_{n}'] = spark.sql(f"""select t1.*
from ANTERIO.{nom_base_anterio} t1 
left join cqs_out.carto_cqs t2 on (t1.IDEAN=t2.IDEAN) and (t1.country=t2.country)
where t2.idean <>""""")
_dfs[f'base_anteriorite_CQS_{n}'].createOrReplaceTempView(f'base_anteriorite_CQS_{n}')

# /
# Récupération des flux d'anterio
# /
Primes_anterio = spark.sql(f"""select IDEAN, GAR, SURV, sum(Montant) as Primes_Anterio
from ANTERIO.base_anteriorite_CQS_{n}
where poste in ("PRIMES","ENT_PTF") and typaff = 0
group by IDEAN, GAR, SURV
order by IDEAN, GAR, SURV""")
Primes_anterio.createOrReplaceTempView('Primes_anterio')

Commission_anterio = spark.sql(f"""select IDEAN, GAR, SURV, sum(Montant) as Comm_Anterio
from ANTERIO.base_anteriorite_CQS_{n}
where poste in ("COMMACQ","COMMSOC","FGA") and typaff = 0
group by IDEAN, GAR, SURV
order by IDEAN, GAR, SURV""")
Commission_anterio.createOrReplaceTempView('Commission_anterio')

Sinistres_anterio = spark.sql(f"""select regexp_replace(IDEAN, ' ', '') as idean, GAR, SURV, sum(Montant) as Sinistres_anterio
from ANTERIO.base_anteriorite_CQS_{n}
where poste in ("SINISTRES") and typaff = 0
group by IDEAN, GAR, SURV
order by IDEAN, GAR, SURV""")
Sinistres_anterio.createOrReplaceTempView('Sinistres_anterio')

# data Sinistres_anterio;
# set Sinistres_anterio ;
# idean2=put(idean,8.);
# drop idean;
# rename idean2=idean;
# run;
# /
# Intégration des stop loss
# /
SL = spark.table('GSL')
SL = (SL
    .withColumn('IDEAN', F.lit(14529))
)
SL = SL.withColumnRenamed('annee', 'surv')
SL = SL.withColumnRenamed('Stop_Loss', 'PRIMES')
SL.createOrReplaceTempView('SL')

SL = spark.table('SL')
SL = (SL
    .withColumn('idean_char', F.col('idean').cast('string'))
)
SL = SL.drop('idean')
SL = SL.withColumnRenamed('idean_char', 'idean')
SL.createOrReplaceTempView('SL')

cqs_gep = spark.table('cqs_gep_3') \
    .union(spark.table('SL'))
cqs_gep = (cqs_gep
    .withColumn('idean2', F.expr("""regexp_replace(idean, ' ', '')"""))
)
cqs_gep = cqs_gep.filter(~F.expr("""PRIMES=0 AND Com=0"""))
cqs_gep = cqs_gep.drop('idean')
cqs_gep = cqs_gep.withColumnRenamed('idean2', 'idean')
cqs_gep.createOrReplaceTempView('cqs_gep')

cqs_gep = spark.sql("""select IDEAN, GAR, SURV, sum(PRIMES) as PRIMES, sum(COM) as COM
from cqs_gep
group by IDEAN, GAR, SURV""")
cqs_gep.createOrReplaceTempView('cqs_gep')

Primes_anterio = spark.table('Primes_anterio')
Primes_anterio = (Primes_anterio
    .withColumn('idean', F.expr("""regexp_replace(idean, ' ', '')"""))
)
Primes_anterio.createOrReplaceTempView('Primes_anterio')

Commission_anterio = spark.table('Commission_anterio')
Commission_anterio = (Commission_anterio
    .withColumn('idean', F.expr("""regexp_replace(idean, ' ', '')"""))
)
Commission_anterio.createOrReplaceTempView('Commission_anterio')

cqs_gep = spark.table('cqs_gep').orderBy('IDEAN', 'GAR', 'SURV')
cqs_gep.createOrReplaceTempView('cqs_gep')

# /
# Calcul des boni mali
# /
# MERGE: FULL OUTER JOIN (if a or b / no condition)
Boni_mali_Primes = spark.table('Primes_anterio').join(spark.table('cqs_gep'), ['IDEAN', 'GAR', 'SURV'], 'full')
Boni_mali_Primes = Boni_mali_Primes.drop('COM')
Boni_mali_Primes.createOrReplaceTempView('Boni_mali_Primes')

# MERGE: FULL OUTER JOIN (if a or b / no condition)
Boni_mali_Commission = spark.table('cqs_gep').join(spark.table('Commission_anterio'), ['IDEAN', 'GAR', 'SURV'], 'full')
Boni_mali_Commission = Boni_mali_Commission.drop('PRIMES')
Boni_mali_Commission.createOrReplaceTempView('Boni_mali_Commission')

claims_actuals = spark.sql("""select  idean ,GAR ,SURV, 
sum(Sinistres) as Sinistres, sum(Reserves) as Reserves
from cqs_sin.claims_actuals_8
group by IDEAN, GAR ,SURV """)
claims_actuals.createOrReplaceTempView('claims_actuals')

claims_actuals = spark.table('claims_actuals')
claims_actuals = (claims_actuals
    .withColumn('idean2', F.col('idean').cast('string'))
)
claims_actuals = claims_actuals.drop('idean')
claims_actuals = claims_actuals.withColumnRenamed('idean2', 'idean')
claims_actuals.createOrReplaceTempView('claims_actuals')

claims_actuals = spark.table('claims_actuals').orderBy('IDEAN', 'GAR', 'SURV')
claims_actuals.createOrReplaceTempView('claims_actuals')

# MERGE: FULL OUTER JOIN (if a or b / no condition)
Boni_mali_Sinistres = spark.table('Sinistres_anterio').join(spark.table('claims_actuals'), ['IDEAN', 'GAR', 'SURV'], 'full')
Boni_mali_Sinistres = Boni_mali_Sinistres.drop('reserves')
Boni_mali_Sinistres.createOrReplaceTempView('Boni_mali_Sinistres')

# /
# Constitution BGD PY
# /
bgd_idean_CQS_CBP_Primes = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean, GAR, "PRIMES" as POSTE, "T" as Niveau, "RAI_CPTA{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(PRIMES,0)-coalesce(Primes_Anterio,0)) as Montant
from Boni_mali_Primes
where surv<{n} and abs(coalesce(PRIMES,0)-coalesce(Primes_Anterio,0))>0.01
group by IDEAN, GAR, SURV""")
bgd_idean_CQS_CBP_Primes.createOrReplaceTempView('bgd_idean_CQS_CBP_Primes')

bgd_idean_CQS_CBP_Primes = spark.table('bgd_idean_CQS_CBP_Primes')
bgd_idean_CQS_CBP_Primes = bgd_idean_CQS_CBP_Primes.filter(~F.expr("""surv IN (2009,2010)"""))
bgd_idean_CQS_CBP_Primes.createOrReplaceTempView('bgd_idean_CQS_CBP_Primes')

# Ne pas oublier les B/M issus des cancelations cf bgd_idean_cqs_cbp_cancel_comm
bgd_idean_CQS_CBP_Commission = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean, GAR, "COMMACQ" as POSTE, "T" as Niveau, "RAI_CPTA{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(COM,0)-coalesce(Comm_Anterio,0)) as Montant
from Boni_mali_Commission
where surv<{n} and abs(coalesce(COM,0)-coalesce(Comm_Anterio,0))>0.01
group by IDEAN, GAR, SURV, (case when gar = 10 then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_Commission.createOrReplaceTempView('bgd_idean_CQS_CBP_Commission')

bgd_idean_CQS_CBP_Commission = spark.table('bgd_idean_CQS_CBP_Commission')
bgd_idean_CQS_CBP_Commission = bgd_idean_CQS_CBP_Commission.filter(~F.expr("""surv IN (2009,2010)"""))
bgd_idean_CQS_CBP_Commission.createOrReplaceTempView('bgd_idean_CQS_CBP_Commission')

bgd_idean_CQS_CBP_PPNA = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean , GAR, "PPNA " as POSTE, "T" as Niveau, "SSI_CLOT{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(UEP,0)) as Montant
from cqs_out.cqs_uep_pl_3
where  abs(coalesce(UEP,0))>0.01
group by IDEAN, GAR, SURV, (case when gar = 10 then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_PPNA.createOrReplaceTempView('bgd_idean_CQS_CBP_PPNA')

bgd_idean_CQS_CBP_FAR = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean, GAR, "FAR " as POSTE, "T" as Niveau, "SSI_CLOT{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(DAC,0))*(-1) as Montant
from cqs_out.cqs_uep_pl_3
where  abs(coalesce(DAC,0))>0.01
group by IDEAN, GAR, SURV, (case when gar = 10 then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_FAR.createOrReplaceTempView('bgd_idean_CQS_CBP_FAR')

bgd_idean_CQS_CBP_Cancel = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN,regexp_replace(IDEAN, ' ', '') as idean, 
(case when VIE_IARD="VIE" then 10 else 30 end) as GAR, 
"PRIMES" as POSTE, "T" as Niveau, "RAI_CPTA{n}"  as VUE,
SURV, 74 as CESS, "IT" as country, 
(case when VIE_IARD="VIE" then "FACL" else "FICL" end) as entity,
"cancel" as Source, "CQS" as SI, "EUR" as DEVISE, 
Rachat_off_systeme_2 as Montant 
from cancelation_2
group by IDEAN, (case when VIE_IARD="VIE" then 10 else 30 end), (case when VIE_IARD="VIE" then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_Cancel.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel')

bgd_idean_CQS_CBP_Cancel = spark.table('bgd_idean_CQS_CBP_Cancel')
bgd_idean_CQS_CBP_Cancel = bgd_idean_CQS_CBP_Cancel.filter(~F.expr("""surv IN (2009,2010)"""))
bgd_idean_CQS_CBP_Cancel.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel')

bgd_idean_CQS_CBP_Cancel_PPNA = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN,regexp_replace(IDEAN, ' ', '') as idean, 
(case when VIE_IARD="VIE" then 10 else 30 end) as GAR, 
"PPNA" as POSTE, "T" as Niveau, "SSI_CLOT{n}"  as VUE,
SURV, 74 as CESS, "IT" as country, 
(case when VIE_IARD="VIE" then "FACL" else "FICL" end) as entity,
"cancel" as Source, "CQS" as SI, "EUR" as DEVISE, 
Rachat_off_systeme_2 as Montant 
from cancelation_2
group by IDEAN, (case when VIE_IARD="VIE" then 10 else 30 end), (case when VIE_IARD="VIE" then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_Cancel_PPNA.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel_PPNA')

bgd_idean_CQS_CBP_Cancel_comm = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN,regexp_replace(IDEAN, ' ', '') as idean, 
(case when VIE_IARD="VIE" then 10 else 30 end) as GAR, 
"COMMACQ" as POSTE, "T" as Niveau, "RAI_CPTA{n}"  as VUE,
SURV, 74 as CESS, "IT" as country, 
(case when VIE_IARD="VIE" then "FACL" else "FICL" end) as entity,
"cancel" as Source, "CQS" as SI, "EUR" as DEVISE, 
0.1 *Rachat_off_systeme_2 as Montant 
from cancelation_2
group by IDEAN, (case when VIE_IARD="VIE" then 10 else 30 end), (case when VIE_IARD="VIE" then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_Cancel_comm.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel_comm')

bgd_idean_CQS_CBP_Cancel_comm = spark.table('bgd_idean_CQS_CBP_Cancel_comm')
bgd_idean_CQS_CBP_Cancel_comm = bgd_idean_CQS_CBP_Cancel_comm.filter(~F.expr("""surv IN (2009,2010)"""))
bgd_idean_CQS_CBP_Cancel_comm.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel_comm')

bgd_idean_CQS_CBP_Cancel_FAR = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN,regexp_replace(IDEAN, ' ', '') as idean, 
(case when VIE_IARD="VIE" then 10 else 30 end) as GAR, 
"FAR" as POSTE, "T" as Niveau, "SSI_CLOT{n}"  as VUE,
SURV, 74 as CESS, "IT" as country, 
(case when VIE_IARD="VIE" then "FACL" else "FICL" end) as entity,
"cancel" as Source, "CQS" as SI, "EUR" as DEVISE, 
-0.1 *Rachat_off_systeme_2 as Montant 
from cancelation_2
group by IDEAN, (case when VIE_IARD="VIE" then 10 else 30 end), (case when VIE_IARD="VIE" then "FACL" else "FICL" end)""")
bgd_idean_CQS_CBP_Cancel_FAR.createOrReplaceTempView('bgd_idean_CQS_CBP_Cancel_FAR')

bgd_idean_CQS_CBP_SINISTRES = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, IDEAN, GAR, "SINISTRES" as POSTE, "T" as Niveau, "RAI_CPTA{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(Sinistres,0)-coalesce(Sinistres_anterio,0)) as Montant
from boni_mali_sinistres
where surv<{n} and abs(coalesce(Sinistres,0)-coalesce(Sinistres_anterio,0))>0.01
group by IDEAN, GAR, SURV""")
bgd_idean_CQS_CBP_SINISTRES.createOrReplaceTempView('bgd_idean_CQS_CBP_SINISTRES')

bgd_idean_CQS_CBP_SINISTRES = spark.table('bgd_idean_CQS_CBP_SINISTRES')
bgd_idean_CQS_CBP_SINISTRES = bgd_idean_CQS_CBP_SINISTRES.filter(~F.expr("""surv IN (2009,2010,2011)"""))
bgd_idean_CQS_CBP_SINISTRES.createOrReplaceTempView('bgd_idean_CQS_CBP_SINISTRES')

bgd_idean_CQS_CBP_Reserves = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, IDEAN, GAR, "PSAP" as POSTE, "T" as Niveau, "SSI_CLOT{n}" as VUE,
SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(coalesce(Reserves,0)) as Montant
from claims_actuals
where surv<{n} and abs(coalesce(Reserves,0))>0.01
group by IDEAN, GAR, SURV""")
bgd_idean_CQS_CBP_Reserves.createOrReplaceTempView('bgd_idean_CQS_CBP_Reserves')

bgd_idean_CQS_CBP_Ouv = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean, GAR, POSTE, "T" as Niveau, "SSI_OUV{n}" as VUE,
SURV, 74 as CESS, "IT" as country, entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
sum(Montant) as Montant
from ANTERIO.base_anteriorite_CQS_{n}
where CPTA={n}-1 and typaff = 0 and poste in ("PSAP","PTEC","PPNA","FAR","Marges","PPRC","PPNA_C","FAR_C")
group by IDEAN, GAR, SURV, POSTE, entity""")
bgd_idean_CQS_CBP_Ouv.createOrReplaceTempView('bgd_idean_CQS_CBP_Ouv')

def mise_en_f(variable):
    _dfs[f'bgd_idean_CQS_CBP_{variable}_'] = spark.table(f'bgd_idean_CQS_CBP_{variable}')
    _dfs[f'bgd_idean_CQS_CBP_{variable}_'] = (_dfs[f'bgd_idean_CQS_CBP_{variable}_']
        .withColumn('POSTE2', F.col('POSTE'))
    )
    # ATTRIB: attrib POSTE2 length = $20 format = $20. informat =$20. label ='POSTE'
    _dfs[f'bgd_idean_CQS_CBP_{variable}_'] = _dfs[f'bgd_idean_CQS_CBP_{variable}_'].drop('POSTE')
    _dfs[f'bgd_idean_CQS_CBP_{variable}_'] = _dfs[f'bgd_idean_CQS_CBP_{variable}_'].withColumnRenamed('POSTE2', 'POSTE')
    _dfs[f'bgd_idean_CQS_CBP_{variable}_'].createOrReplaceTempView(f'bgd_idean_CQS_CBP_{variable}_')


mise_en_f("Primes")
mise_en_f("Commission")
mise_en_f("ppna")
mise_en_f("Cancel_PPNA")
mise_en_f("far")
mise_en_f("Cancel_FAR")
mise_en_f("Cancel")
mise_en_f("Cancel_comm")
mise_en_f("Ouv")
mise_en_f("sinistres")
mise_en_f("reserves")
bgd_idean_CQS_CBP_PY_all = spark.table('bgd_idean_CQS_CBP_Primes_') \
    .union(spark.table('bgd_idean_CQS_CBP_Cancel_') \
    .union(spark.table('bgd_idean_CQS_CBP_Commission_') \
    .union(spark.table('bgd_idean_CQS_CBP_Cancel_comm_') \
    .union(spark.table('bgd_idean_cqs_cbp_ppna_') \
    .union(spark.table('bgd_idean_CQS_CBP_Cancel_PPNA_') \
    .union(spark.table('bgd_idean_CQS_CBP_far_') \
    .union(spark.table('bgd_idean_CQS_CBP_Cancel_FAR_') \
    .union(spark.table('bgd_idean_cqs_cbp_sinistres_') \
    .union(spark.table('bgd_idean_cqs_cbp_reserves_') \
    .union(spark.table('bgd_idean_CQS_CBP_Ouv_')))))))))))
bgd_idean_CQS_CBP_PY_all.createOrReplaceTempView('bgd_idean_CQS_CBP_PY_all')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_CQS_CBP_PY_all.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_CQS_CBP_PY_all')

bgd_idean_CQS_CBP_PY_all = spark.table('bgd_idean_CQS_CBP_PY_all')
bgd_idean_CQS_CBP_PY_all = (bgd_idean_CQS_CBP_PY_all
    .withColumn('poste', F.when(F.expr("""poste IN ("FAR_C")"""), F.lit("FAR")))  # no ELSE: null when condition is false
    .withColumn('poste', F.when(F.expr("""poste IN ("PPNA_C")"""), F.lit("PPNA")))  # no ELSE: null when condition is false
)
bgd_idean_CQS_CBP_PY_all.createOrReplaceTempView('bgd_idean_CQS_CBP_PY_all')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_CQS_CBP_PY_all.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_CQS_CBP_PY_all')

bgd_idean_CQS_CBP_PY_all = spark.sql("""select 
	TYPAFF, TYPIDEAN, regexp_replace(IDEAN, ' ', '') as idean, GAR,POSTE,Niveau,VUE,SURV, CESS, country,  entity,Source,  SI, DEVISE, Montant 
	from cqs_out.bgd_idean_CQS_CBP_PY_all
	group by IDEAN, GAR, SURV, POSTE, entity """)
bgd_idean_CQS_CBP_PY_all.createOrReplaceTempView('bgd_idean_CQS_CBP_PY_all')

# proc sort data = bgd_idean_CQS_CBP out=BGD_&exer..bgd_idean_CQS_CBP; by IDEAN GAR SURV; run;
# Data BGD_&exer..bgd_idean_CQS_CBP;
# set BGD_&exer..bgd_idean_CQS_CBP;
# y_char = compress(put(IDEAN,21.0));
# drop IDEAN;
# rename y_char = IDEAN;
# if MONTANT  = 0 then delete;
# run;
# Définition scope CQS CBP dans Topline courante yc NB
Topline_Macao2 = spark.table('Topline_Macao')
Topline_Macao2 = Topline_Macao2.filter(F.expr("""CY_PY IN ("CY")"""))
Topline_Macao2 = (Topline_Macao2
    .withColumn('GAR',
        F.when(F.expr("""VIE_IARD ="VIE""""), F.lit(10))
         .otherwise(F.lit(30)))
    .withColumn('idean', F.expr("""regexp_replace(idean, ' ', '')"""))
)
    # MANUAL REVIEW: indexed column from macro loop — use df.withColumn(f'montant_{n}', ...) inside a Python for-loop
    # SAS: montant_{n} =_2025_FY_LC
# Ajout provisoire "V1" suite format topline du 19/09/23
Topline_Macao2.createOrReplaceTempView('Topline_Macao2')

# t2.RPP as RPP_Macao ,t2.Scheme,
Topline_cqs_CBP_stock = spark.sql("""select distinct t1.*, t2.IDEAN   , "STOCK" as source_2
from Topline_Macao2 t1 
left join cqs_out.carto_cqs t2 on (t1.Country_Code=t2.Country) and  (t1.IDEAN=t2.idean)
where t2.idean not in (" ") """)
Topline_cqs_CBP_stock.createOrReplaceTempView('Topline_cqs_CBP_stock')

# Topline New Business
Topline_cqs_CBP_NB = spark.sql("""select distinct t1.*, FLAG_CBP
from Topline_Macao2 t1 
Left join tabid_new t3 on (t1.Country_Code=t3.COUNTRY_CODE)and (t1.Micro_Prod=t3.PRODUIT_PRTG)
where Micro_Prod in ("CQS")and FLAG_CBP=1 and t1.IDEAN in (" ")""")
Topline_cqs_CBP_NB.createOrReplaceTempView('Topline_cqs_CBP_NB')

Topline_cqs_CBP_NB = spark.table('Topline_cqs_CBP_NB')
Topline_cqs_CBP_NB = (Topline_cqs_CBP_NB
    .withColumn('IDEAN', F.col('Line_number'))
    .withColumn('IDEAN_Macao', F.col('Line_number'))
    .withColumn('source_2', F.lit("NB"))
)
Topline_cqs_CBP_NB = Topline_cqs_CBP_NB.drop('FLAG_CQS_CBP')
Topline_cqs_CBP_NB.createOrReplaceTempView('Topline_cqs_CBP_NB')

Topline_cqs_CBP = spark.table('Topline_cqs_CBP_stock') \
    .union(spark.table('Topline_cqs_CBP_NB'))
Topline_cqs_CBP = (Topline_cqs_CBP
    .withColumn('idean', F.expr("""regexp_replace(idean, ' ', '')"""))
)
Topline_cqs_CBP.createOrReplaceTempView('Topline_cqs_CBP')

# /
# Constitution BGD CY
# /
bgd_idean_CQS_CBP_PrimesCY = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, IDEAN as Idean, GAR, "PRIMES" as POSTE, "T" as Niveau, "FAI_CPTA{n}" as VUE,
{n} as SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, 
 montant_{n} as Montant
from Topline_CQS_CBP
where montant_{n} <> 0
group by IDEAN, GAR, SURV""")
bgd_idean_CQS_CBP_PrimesCY.createOrReplaceTempView('bgd_idean_CQS_CBP_PrimesCY')

bgd_idean_CQS_CBP_CommCY = spark.table('bgd_idean_CQS_CBP_PrimesCY')
bgd_idean_CQS_CBP_CommCY = (bgd_idean_CQS_CBP_CommCY
    .withColumn('montant_Commission',
        F.when(F.expr("""gar = 10"""), F.expr(f"""{taux_comm_vie} * Montant"""))
         .otherwise(F.expr(f"""{taux_comm_iard} * Montant""")))
    .withColumn('POSTE2', F.lit("COMMACQ"))
)
# ATTRIB: attrib POSTE2 length = $20 format = $20. informat =$20. label ='POSTE'
bgd_idean_CQS_CBP_CommCY = bgd_idean_CQS_CBP_CommCY.drop('Montant', 'POSTE')
bgd_idean_CQS_CBP_CommCY = bgd_idean_CQS_CBP_CommCY.withColumnRenamed('montant_Commission', 'montant')
bgd_idean_CQS_CBP_CommCY = bgd_idean_CQS_CBP_CommCY.withColumnRenamed('POSTE2', 'POSTE')
bgd_idean_CQS_CBP_CommCY.createOrReplaceTempView('bgd_idean_CQS_CBP_CommCY')

bgd_idean_CQS_CBP_PPNACY = spark.table('bgd_idean_CQS_CBP_PrimesCY')
bgd_idean_CQS_CBP_PPNACY = (bgd_idean_CQS_CBP_PPNACY
    .withColumn('montant_PPNA', F.expr("""Montant * (((10-0)*(10-1))/(10*11))"""))
    .withColumn('POSTE', F.lit("PPNA"))
    .withColumn('VUE', F.lit("SSI_CLOT{n}"))
)
# Regle 78 retenu avec une duration de 10 ans pour les cqs CBP
bgd_idean_CQS_CBP_PPNACY = bgd_idean_CQS_CBP_PPNACY.drop('Montant')
bgd_idean_CQS_CBP_PPNACY = bgd_idean_CQS_CBP_PPNACY.withColumnRenamed('montant_PPNA', 'montant')
bgd_idean_CQS_CBP_PPNACY.createOrReplaceTempView('bgd_idean_CQS_CBP_PPNACY')

# On Ramène le stock de PPNA et les FAR associés obtenu à partir du programme de calcul des PPNA et DAC pour l'année N
cqs_uep_pl_3_ = spark.table('cqs_uep_pl_3')
cqs_uep_pl_3_ = (cqs_uep_pl_3_
    .withColumn('IDEAN2', F.col('IDEAN'))
)
# ATTRIB: attrib IDEAN2 length = $14 format = $14. informat =$14. label ='IDEAN'
cqs_uep_pl_3_ = cqs_uep_pl_3_.drop('IDEAN')
cqs_uep_pl_3_ = cqs_uep_pl_3_.withColumnRenamed('IDEAN2', 'IDEAN')
cqs_uep_pl_3_.createOrReplaceTempView('cqs_uep_pl_3_')

_dfs[f'base_idean_PPNA_{n}'] = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, IDEAN, GAR, "PPNA" as POSTE, "T" as Niveau, "SSI_CLOT{n}" as VUE,
   SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
	"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE, UEP as Montant
	from cqs_uep_pl_3_
	where SURV ={n} 
	group by IDEAN, GAR, SURV""")
_dfs[f'base_idean_PPNA_{n}'].createOrReplaceTempView(f'base_idean_PPNA_{n}')

_dfs[f'base_idean_far_{n}'] = spark.sql(f"""select 0 as TYPAFF, "DI" as TYPIDEAN, IDEAN, GAR, "FAR" as POSTE, "T" as Niveau, "SSI_CLOT{n}" as VUE,
   SURV, 74 as CESS, "IT" as country, (case when gar = 10 then "FACL" else "FICL" end) as entity,
	"CQS CBP" as Source, "CQS" as SI, "EUR" as DEVISE,  DAC as Montant
	from cqs_uep_pl_3_
	where SURV ={n} 
	group by IDEAN, GAR, SURV""")
_dfs[f'base_idean_far_{n}'].createOrReplaceTempView(f'base_idean_far_{n}')

bgd_idean_CQS_CBP_FARCY = spark.table('bgd_idean_CQS_CBP_PPNACY')
bgd_idean_CQS_CBP_FARCY = (bgd_idean_CQS_CBP_FARCY
    .withColumn('montant_FAR',
        F.when(F.expr("""gar = 10"""), F.expr(f"""-{taux_comm_vie} * Montant"""))
         .otherwise(F.expr(f"""-{taux_comm_iard} * Montant""")))
    .withColumn('POSTE', F.lit("FAR"))
)
bgd_idean_CQS_CBP_FARCY = bgd_idean_CQS_CBP_FARCY.drop('Montant')
bgd_idean_CQS_CBP_FARCY = bgd_idean_CQS_CBP_FARCY.withColumnRenamed('montant_FAR', 'montant')
bgd_idean_CQS_CBP_FARCY.createOrReplaceTempView('bgd_idean_CQS_CBP_FARCY')

bgd_idean_cqs_cbp_CY = spark.table('bgd_idean_CQS_CBP_CommCY') \
    .union(spark.table('bgd_idean_CQS_CBP_PrimesCY') \
    .union(spark.table('bgd_idean_CQS_CBP_PPNACY') \
    .union(spark.table('bgd_idean_CQS_CBP_FARCY'))))
bgd_idean_cqs_cbp_CY.createOrReplaceTempView('bgd_idean_cqs_cbp_CY')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_cqs_cbp_CY.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_cqs_cbp_CY')

bgd_idean_cqs_all = spark.table('bgd_idean_cqs_cbp_CY') \
    .union(spark.table('bgd_idean_cqs_cbp_PY_all'))
bgd_idean_cqs_all = (bgd_idean_cqs_all
    .withColumn('poste', F.when(F.expr("""poste IN ("PPNA_C")"""), F.lit("PPNA")))  # no ELSE: null when condition is false
    .withColumn('poste', F.when(F.expr("""poste IN ("FAR_C")"""), F.lit("FAR")))  # no ELSE: null when condition is false
)
bgd_idean_cqs_all.createOrReplaceTempView('bgd_idean_cqs_all')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_cqs_all.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_cqs_all')

# Calcul des GEPs pour le courant en prenant les primes, ppna de la topeline et le viellissement des ppna
Primes_Acq_CY = spark.table('bgd_idean_CQS_CBP_PrimesCY') \
    .union(spark.table('bgd_idean_CQS_CBP_PPNACY') \
    .union(spark.table(f'base_idean_PPNA_{n}')))
Primes_Acq_CY.createOrReplaceTempView('Primes_Acq_CY')

Sinistralite_CY = spark.sql("""select TYPAFF, TYPIDEAN,  IDEAN, GAR, Niveau, SURV, CESS, country, entity, Source, SI, DEVISE, 
sum((case when POSTE ="PRIMES" then Montant else 0 end)-(case when POSTE ="PPNA" then Montant else 0 end))  as Acq
from Primes_Acq_CY 
group by TYPAFF, TYPIDEAN, IDEAN, GAR, Niveau, SURV, CESS, country, entity, Source, SI, DEVISE""")
Sinistralite_CY.createOrReplaceTempView('Sinistralite_CY')

bgd_idean_CQS_CBP_sinistres_CY = spark.table('Sinistralite_CY')
bgd_idean_CQS_CBP_sinistres_CY = (bgd_idean_CQS_CBP_sinistres_CY
    .withColumn('POSTE', F.lit("SINISTRES"))
    .withColumn('VUE', F.lit("FAI_CPTA{n}"))
    .withColumn('Montant',
        F.when(F.expr("""gar = 10"""), F.expr(f"""Acq * {lr_vie} * {sin_dc}"""))
         .otherwise(F.expr(f"""Acq * {lr_iard} * {sin_iu}""")))
)
bgd_idean_CQS_CBP_sinistres_CY = bgd_idean_CQS_CBP_sinistres_CY.drop('Acq')
bgd_idean_CQS_CBP_sinistres_CY.createOrReplaceTempView('bgd_idean_CQS_CBP_sinistres_CY')

# On Ramène les sinistres payés de l'année N
bgd_idean_CQS_CBP_PSAP_CY = spark.table('Sinistralite_CY')
bgd_idean_CQS_CBP_PSAP_CY = (bgd_idean_CQS_CBP_PSAP_CY
    .withColumn('POSTE', F.lit("PSAP"))
    .withColumn('VUE', F.lit("SSI_CLOT{n}"))
    .withColumn('Montant',
        F.when(F.expr("""gar = 10"""), F.expr(f"""Acq * {lr_vie} * (1-{sin_dc})"""))
         .otherwise(F.expr(f"""Acq * {lr_iard} * (1-{sin_iu})""")))
)
bgd_idean_CQS_CBP_PSAP_CY = bgd_idean_CQS_CBP_PSAP_CY.drop('Acq')
bgd_idean_CQS_CBP_PSAP_CY.createOrReplaceTempView('bgd_idean_CQS_CBP_PSAP_CY')

mise_en_f("PrimesCY")
mise_en_f("CommCY")
mise_en_f("PPNACY")
mise_en_f("FARCY")
mise_en_f("sinistres_CY")
mise_en_f("PSAP_CY")
bgd_idean_CQS_CBP_CY = spark.table('bgd_idean_CQS_CBP_sinistres_CY_') \
    .union(spark.table('bgd_idean_cqs_cbp_primescy_') \
    .union(spark.table('bgd_idean_CQS_CBP_CommCY') \
    .union(spark.table('bgd_idean_CQS_CBP_PPNACY_') \
    .union(spark.table('bgd_idean_CQS_CBP_FARCY_') \
    .union(spark.table('bgd_idean_CQS_CBP_PSAP_CY_'))))))
bgd_idean_CQS_CBP_CY.createOrReplaceTempView('bgd_idean_CQS_CBP_CY')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_CQS_CBP_CY.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_CQS_CBP_CY')

bgd_idean_cqs_cbp_cy = spark.sql("""select 
	TYPAFF, TYPIDEAN, IDEAN, GAR,POSTE,Niveau,VUE,SURV, CESS, country,  entity,Source,  SI, DEVISE, Montant 
	from cqs_out.bgd_idean_cqs_cbp_cy
	group by IDEAN, GAR, SURV, POSTE, entity """)
bgd_idean_cqs_cbp_cy.createOrReplaceTempView('bgd_idean_cqs_cbp_cy')

bgd_idean_cqs_all = spark.table('bgd_idean_cqs_cbp_CY') \
    .union(spark.table('bgd_idean_cqs_cbp_PY_all'))
bgd_idean_cqs_all.createOrReplaceTempView('bgd_idean_cqs_all')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_cqs_all.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_cqs_all')

bgd_idean_cqs_cbp_cy = spark.table('bgd_idean_cqs_cbp_cy')
bgd_idean_cqs_cbp_cy = (bgd_idean_cqs_cbp_cy
    .withColumn('y_char', F.expr("""trim(cast(IDEAN as string))"""))
)
bgd_idean_cqs_cbp_cy = bgd_idean_cqs_cbp_cy.filter(~F.expr("""MONTANT  = 0"""))
bgd_idean_cqs_cbp_cy = bgd_idean_cqs_cbp_cy.drop('IDEAN')
bgd_idean_cqs_cbp_cy = bgd_idean_cqs_cbp_cy.withColumnRenamed('y_char', 'IDEAN')
bgd_idean_cqs_cbp_cy.createOrReplaceTempView('bgd_idean_cqs_cbp_cy')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_cqs_cbp_cy.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_cqs_cbp_cy')

bgd_idean_CQS_CBP1 = spark.table('bgd_idean_cqs_cbp_py_all') \
    .union(spark.table('bgd_idean_cqs_cbp_cy'))
# if surv<{n}-13 and poste not in ("PPNA","FAR") and Vue not="SSI_OUV2024" then delete ;
bgd_idean_CQS_CBP1.createOrReplaceTempView('bgd_idean_CQS_CBP1')
# LIBNAME CQS_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_CQS_CBP1.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_CQS_CBP1')

bgd_idean_CQS_CBP1 = spark.table('bgd_idean_CQS_CBP1')
bgd_idean_CQS_CBP1.createOrReplaceTempView('bgd_idean_CQS_CBP1')
# LIBNAME BGD_{exer} -> Unity Catalog: {_catalog}.bgd_{exer}
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.bgd_{exer}')
bgd_idean_CQS_CBP1.write.mode('overwrite').saveAsTable(f'{{_catalog}}.bgd_{exer}.bgd_idean_CQS_CBP1')

# ----------------------- BGD 2 ----------------------------------------
# ---------------------------------------------------------------------
bgd = spark.table('bgd_idean_CQS_CBP1')
bgd.createOrReplaceTempView('bgd')

stock_n = spark.sql(f"""SELECT typaff, typidean, idean, gar, poste, niveau, cess, sum(montant) as stock_n, country,Entity,DEVISE
FROM bgd
WHERE surv = {n} and poste in ("PPNA", "FAR")  and vue = "SSI_CLOT{n}" 
group by typaff, typidean, idean, gar, poste, niveau, cess, entity ,country,DEVISE""")
stock_n.createOrReplaceTempView('stock_n')

stock_ant = spark.sql("""SELECT typaff, typidean, idean, gar, poste, niveau, cess, vue, sum(montant) as montant,country,Entity,DEVISE
FROM bgd
WHERE poste in ("PPNA", "FAR") 
group by typaff, typidean, idean, gar, poste, niveau, cess, vue, country,entity,DEVISE""")
stock_ant.createOrReplaceTempView('stock_ant')

stock_ant = spark.table('stock_ant')
stock_ant = (stock_ant
    .withColumn('vue', F.when(F.col('vue') == f"SSI_CLOT{n}", F.lit("CLOT")))  # no ELSE: null when condition is false
    .withColumn('vue', F.when(F.col('vue') == f"SSI_OUV{n}", F.lit("OUV")))  # no ELSE: null when condition is false
)
stock_ant.createOrReplaceTempView('stock_ant')

stock_ant = spark.table('stock_ant').orderBy('typaff', 'typidean', 'idean', 'gar', 'poste', 'niveau', 'cess', 'country', 'Entity', 'DEVISE')
stock_ant.createOrReplaceTempView('stock_ant')

# PROC TRANSPOSE
# ID present → long-to-wide pivot
t_stock_ant = stock_ant.groupBy('typaff', 'typidean', 'idean', 'gar', 'poste', 'niveau', 'cess', 'country', 'Entity', 'DEVISE').pivot('vue').agg(F.first(F.col('montant')))
t_stock_ant.createOrReplaceTempView('t_stock_ant')

t_stock_ant = spark.table('t_stock_ant')
t_stock_ant = t_stock_ant.drop('_Name_')
t_stock_ant.createOrReplaceTempView('t_stock_ant')

t_stock_ant = spark.table('t_stock_ant').orderBy('typaff', 'typidean', 'idean', 'gar', 'poste', 'niveau', 'cess', 'country', 'Entity', 'DEVISE')
t_stock_ant.createOrReplaceTempView('t_stock_ant')

stock_n = spark.table('stock_n').orderBy('typaff', 'typidean', 'idean', 'gar', 'poste', 'niveau', 'cess', 'country', 'Entity', 'DEVISE')
stock_n.createOrReplaceTempView('stock_n')

t_stock = spark.table('t_stock_ant')
t_stock.createOrReplaceTempView('t_stock')

t_stock = spark.table('t_stock')
t_stock = (t_stock
    .withColumn('clot', F.when(F.expr("""clot =".""""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('ouv', F.when(F.expr("""ouv =".""""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('stock_n', F.when(F.expr("""stock_n =".""""), F.lit(0)))  # no ELSE: null when condition is false
    .withColumn('courant', F.col('ouv') + F.col('stock_n'))
    .withColumn('ca', F.col('clot') - F.col('courant'))
)
# drop ouv clot stock_n;
t_stock.createOrReplaceTempView('t_stock')

# PROC TRANSPOSE
# wide-to-long: 2 columns -> 2 rows (_NAME_ = column name, COL1 = value)
stock = t_stock.select('typaff', 'typidean', 'idean', 'gar', 'poste', 'niveau', 'cess', 'country', 'Entity', 'DEVISE', F.expr("""stack(2, 'courant', `courant`, 'ca', `ca`) as (_NAME_, COL1)"""))
stock.createOrReplaceTempView('stock')

stock = spark.table('stock')
stock = (stock
    .withColumn('VUE', F.lit("SSI_CLOT{n}"))
    .withColumn('SURV',
        F.when(F.expr("""_Name_ = "courant""""), F.expr(f"""{n}"""))
         .otherwise(F.expr(f"""{n}-1""")))
    .withColumn('NIVEAU', F.lit("T"))
    .withColumn('CPTA', F.expr(f"""{n}"""))
    .withColumn('SOURCE', F.lit("CQS CBP"))
)
stock = stock.drop('_Name_')
stock = stock.withColumnRenamed('COL1', 'MONTANT')
stock.createOrReplaceTempView('stock')

stock = spark.sql("""Select TYPAFF,TYPIDEAN, IDEAN,GAR, POSTE , NIVEAU, VUE,SURV, CPTA,CESS,Sum(MONTANT) As MONTANT,country,entity,SOURCE,DEVISE
From stock
Group By TYPAFF,TYPIDEAN,IDEAN,GAR,POSTE,NIVEAU,VUE,SURV,CESS,country,entity,POSTE, DEVISE,SOURCE""")
stock.createOrReplaceTempView('stock')

bgd = spark.table('bgd_idean_cqs_cbp1')
bgd = (bgd
    .withColumn('SURV', F.when(F.expr(f"""poste IN ("PPNA","FAR") AND vue = "SSI_OUV{n}""""), F.expr(f"""{n}""")))  # no ELSE: null when condition is false
)
bgd = bgd.filter(~F.expr(f"""poste IN ("PPNA","FAR") AND vue = "SSI_CLOT{n}""""))
bgd.createOrReplaceTempView('bgd')

bgd = spark.sql("""Select DISTINCT TYPAFF,TYPIDEAN, IDEAN,GAR, POSTE , NIVEAU, VUE,SURV, CESS,Sum(MONTANT) As MONTANT,country,entity,SOURCE,DEVISE
From bgd
Group By TYPAFF,TYPIDEAN,IDEAN,GAR,POSTE,NIVEAU,VUE,SURV,CESS,country,entity,POSTE, DEVISE,SOURCE""")
bgd.createOrReplaceTempView('bgd')

bgd_idean_CQS_CBP = spark.table('bgd') \
    .union(spark.table('stock'))
bgd_idean_CQS_CBP = (bgd_idean_CQS_CBP
    .withColumn('SOURCE2', F.lit(None).cast(StringType()))  # LENGTH SOURCE2 $40
)
bgd_idean_CQS_CBP.createOrReplaceTempView('bgd_idean_CQS_CBP')

bgd_idean_CQS_CBP_2 = spark.sql("""Select DISTINCT TYPAFF,TYPIDEAN, IDEAN,GAR,POSTE,NIVEAU, VUE,SURV, CESS,Sum(MONTANT) As MONTANT,country,entity,"CQS CBP" as SOURCE,DEVISE, "CQS" as SI
From bgd_idean_CQS_CBP
Group By TYPAFF,TYPIDEAN,IDEAN,GAR,POSTE,NIVEAU,VUE,SURV,CESS,country,entity,POSTE, DEVISE""")
bgd_idean_CQS_CBP_2.createOrReplaceTempView('bgd_idean_CQS_CBP_2')

bgd_idean_CQS_CBP_3 = spark.sql(f"""Select t1.*, t2.PMP_Sales_Name 
From BGD_{exer}.bgd_idean_CQS_CBP_2 as t1
left join tabid_new as t2 on t1.typidean = t2.typidean and t1.idean = t2.idean and t1.country = t2.country_code""")
bgd_idean_CQS_CBP_3.createOrReplaceTempView('bgd_idean_CQS_CBP_3')

bgd_idean_CQS_CBP_3bis = spark.table('bgd_idean_CQS_CBP_3')
bgd_idean_CQS_CBP_3bis = bgd_idean_CQS_CBP_3bis.filter(~F.expr("""MONTANT=0"""))
bgd_idean_CQS_CBP_3bis.createOrReplaceTempView('bgd_idean_CQS_CBP_3bis')
# LIBNAME BGD_{exer} -> Unity Catalog: {_catalog}.bgd_{exer}
bgd_idean_CQS_CBP_3bis.write.mode('overwrite').saveAsTable(f'{{_catalog}}.bgd_{exer}.bgd_idean_CQS_CBP_3bis')

# EXPORT DE LA DATA EN TYPAFF 0
bgd_idean_cqs_cbp1 = spark.table('bgd_idean_cqs_cbp1')
bgd_idean_cqs_cbp1.createOrReplaceTempView('bgd_idean_cqs_cbp1')
# LIBNAME cqs_out -> Unity Catalog: {_catalog}.cqs_out
bgd_idean_cqs_cbp1.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_out.bgd_idean_cqs_cbp1')
