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

# /
# Programme qui sert à splitter les réserves de sinistres par Contrat au prorata des payés
# /
# Répertoire de travail des CQSs CBP
lreseau = "X"
arrete = "2025_09_Q4"
n = 2025
year_lim = 2025
fichier_input = "CQS_Etude_reserve_sinistres_DAAP_2025_09_Q4_V2"
table_claims = "CBP_ITALY_POLICIES_CLAIMS"
adresse_mail = "iheb.karoui@axa.fr"
cqs_sin_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves"  # LIBNAME CQS_SIN
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {{_catalog}}.cqs_sin')  # Unity Catalog schema for CQS_SIN
cqs_base_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP"  # LIBNAME CQS_Base
# Import des Charges Ultimes du bootstrap
def import_excel(file, onglet, out):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


chemin_imp = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves/1. Travaux/{fichier_input}.xlsx"
input_ = chemin_imp
import_excel(file=input_, onglet="Charge_Ultime recov_new_vision", out="CHP_CQS")
import_excel(file=input_, onglet="Tx_reserving", out="tx_reserving")
# Sinistres réels
Claims_actuals_0 = spark.table('CBP_ITALY_POLICIES_CLAIMS')
Claims_actuals_0 = Claims_actuals_0.filter(F.expr(f"""(Montant_Sin IS NOT NULL AND Etat IN ("DA LIQUIDARE", "LIQUIDATO", "liquidato", "IN LAVORAZIONE") AND Annee_Sin<{year_lim})"""))
Claims_actuals_0 = (Claims_actuals_0
    .withColumn('Annee_Sin', F.expr("""year(to_date(Date_Sin))"""))
    .withColumn('Annee_Sin', F.when(F.expr("""Annee_Sin IS NULL"""), F.expr("""year(to_date(date_liquid))""")))  # no ELSE: null when condition is false
)
# Attention, à modifier à chaque arrêté
# Modification AJ V220 : ajout du "datepart" à cause du changement de format. Vérifier qu'il y en a toujours besoin, sinon supprimer.
Claims_actuals_0.createOrReplaceTempView('Claims_actuals_0')

Claims_actuals = spark.table('Claims_actuals_0')
Claims_actuals = Claims_actuals.filter(F.col('date_liquid').isNotNull())
Claims_actuals.createOrReplaceTempView('Claims_actuals')

# Situation professionnelle : Pensionato ; Privato ; Pubblico
sit_prof = spark.sql("""select distinct sit_prof_1
	from claims_actuals""")
sit_prof.createOrReplaceTempView('sit_prof')

Claims_actuals_2 = spark.sql("""select 
Financiere_adh,
ID_Police,
Generation as gen,
Annee_Sin As Annee_Sin2,
Type_Sin,
type_pret,
sit_prof_1 ,
sum(Montant_Sin) as Montant
from Claims_actuals 
group by Financiere_adh,gen,type_pret,Annee_Sin,sit_prof_1 ,ID_Police,Type_Sin""")
Claims_actuals_2.createOrReplaceTempView('Claims_actuals_2')

Claims_actuals_3 = spark.table('Claims_actuals_2')
Claims_actuals_3 = (Claims_actuals_3
    .withColumn('GAR', F.when(F.expr("""type_sin IN ('DC','dc')"""), F.lit(10)).otherwise(F.lit(30)))
    .withColumn('IDEAN', F.when(F.expr("""type_sin IN ('DC','dc')"""), F.expr("""cast(concat("1", substring(ID_Police,1,4)) as long)""")).otherwise(F.expr("""cast(concat("1", substring(ID_Police,6,4)) as long)""")))
)
# nouvel ajout au préclose 24
Claims_actuals_3 = Claims_actuals_3.filter(~F.expr("""Annee_Sin2 IN (2009,2010)"""))
Claims_actuals_3.createOrReplaceTempView('Claims_actuals_3')

# *****Création de la CLE de répartition + prorata AJOUT PRECLOSING 24
Claims_actuals_GB = spark.sql("""select 
Annee_Sin2,
GAR,
sum(Montant) as Montant_GB
from Claims_actuals_3
group by Annee_Sin2,GAR """)
Claims_actuals_GB.createOrReplaceTempView('Claims_actuals_GB')

Claims_actuals_4 = spark.sql("""select 
A.*,
B.Montant_GB
from Claims_actuals_3 A left join Claims_actuals_GB B on A.Annee_Sin2=B.Annee_Sin2 and A.GAR=B.GAR """)
Claims_actuals_4.createOrReplaceTempView('Claims_actuals_4')

Claims_actuals_5 = spark.table('Claims_actuals_4')
Claims_actuals_5 = (Claims_actuals_5
    .withColumn('Prorata', F.expr("""Montant/Montant_GB"""))
)
Claims_actuals_5.createOrReplaceTempView('Claims_actuals_5')

Claims_actuals_6 = spark.sql("""select 
A.*,
b.tx_reserving
from Claims_actuals_5 A left join tx_reserving B on A.Annee_Sin2=B.surv and A.GAR=B.GAR """)
Claims_actuals_6.createOrReplaceTempView('Claims_actuals_6')

Claims_actuals_7 = spark.sql("""select 
A.*, 
B.Ultimate 
from Claims_actuals_6 A left join CHP_CQS B on A.Annee_Sin2 = B.SURV and A.GAR=B.GAR """)
Claims_actuals_7.createOrReplaceTempView('Claims_actuals_7')

Claims_actuals_8 = spark.table('Claims_actuals_7')
Claims_actuals_8 = (Claims_actuals_8
    .withColumn('Reserves', F.expr("""Ultimate*tx_reserving*prorata"""))
    .withColumn('Sinistres', F.expr("""greatest(Ultimate*prorata- Reserves,0)"""))
)
Claims_actuals_8.createOrReplaceTempView('Claims_actuals_8')

# data Claims_actuals_8 ;
# set Claims_actuals_8 ;
# keep Financiere_Adh gen Annee_Sin2 GAR IDEAN Reserves Sinistres;
# run;
Claims_actuals_8 = spark.table('Claims_actuals_8')
Claims_actuals_8 = (Claims_actuals_8
    .withColumn('reserves', F.when(F.expr("""reserves IS NULL"""), F.lit(0)))  # no ELSE: null when condition is false
)
Claims_actuals_8.createOrReplaceTempView('Claims_actuals_8')

Claims_actuals_8 = spark.table('Claims_actuals_8')
Claims_actuals_8 = Claims_actuals_8.filter(~F.expr("""reserves=0 AND sinistres=0"""))
Claims_actuals_8.createOrReplaceTempView('Claims_actuals_8')

Claims_actuals_8 = spark.table('Claims_actuals_8')
Claims_actuals_8 = Claims_actuals_8.withColumnRenamed('Annee_sin2', 'SURV')
Claims_actuals_8.createOrReplaceTempView('Claims_actuals_8')

# Sauvegarde de la table
Claims_actuals_8 = spark.table('Claims_actuals_8')
Claims_actuals_8.createOrReplaceTempView('Claims_actuals_8')
# LIBNAME CQS_SIN -> Unity Catalog: {_catalog}.cqs_sin
Claims_actuals_8.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_sin.Claims_actuals_8')

Claims_actuals_9 = spark.sql("""select 
SURV,
GAR,

IDEAN,
type_pret,
gen as generation,
sum(Reserves) as Reserves,
sum(Sinistres) as Sinistres 
from Claims_actuals_8
group by IDEAN, GAR,SURV,type_pret,generation """)
Claims_actuals_9.createOrReplaceTempView('Claims_actuals_9')

Claims_actuals_9 = spark.table('Claims_actuals_9')
Claims_actuals_9 = (Claims_actuals_9
    .withColumn('idean_char', F.col('idean').cast('string'))
)
Claims_actuals_9 = Claims_actuals_9.drop('idean')
Claims_actuals_9 = Claims_actuals_9.withColumnRenamed('idean_char', 'idean')
Claims_actuals_9.createOrReplaceTempView('Claims_actuals_9')

Claims_actuals_9 = spark.table('Claims_actuals_9')
Claims_actuals_9.createOrReplaceTempView('Claims_actuals_9')
# LIBNAME CQS_SIN -> Unity Catalog: {_catalog}.cqs_sin
Claims_actuals_9.write.mode('overwrite').saveAsTable(f'{{_catalog}}.cqs_sin.Claims_actuals_9')

Claims_actuals_Verif_Ult_0 = spark.sql("""select
SURV,
GAR,
sum(Reserves) as Reserves,
sum(Sinistres) as Sinistres
from Claims_actuals_9 
group by SURV,GAR""")
Claims_actuals_Verif_Ult_0.createOrReplaceTempView('Claims_actuals_Verif_Ult_0')

Claims_actuals_Verif_Ult_1 = spark.table('Claims_actuals_Verif_Ult_0')
Claims_actuals_Verif_Ult_1 = (Claims_actuals_Verif_Ult_1
    .withColumn('Charge_ult_calculee', F.col('Reserves') + F.col('Sinistres'))
)
Claims_actuals_Verif_Ult_1.createOrReplaceTempView('Claims_actuals_Verif_Ult_1')

Controle_Charge_Ultime_0 = spark.sql("""select 
A.*, 
B.Ultimate 
from Claims_actuals_Verif_Ult_1 A left join CHP_CQS B on A.SURV = B.SURV and A.GAR=B.GAR 
group by SURV,GAR""")
Controle_Charge_Ultime_0.createOrReplaceTempView('Controle_Charge_Ultime_0')

Controle_Charge_Ultime_0 = spark.table('Controle_Charge_Ultime_0')
Controle_Charge_Ultime_0 = (Controle_Charge_Ultime_0
    .withColumn('Ultimate_new', F.col('Ultimate').cast('double'))
)
    # SAS PUT (debug): Ultimate_new
Controle_Charge_Ultime_0 = Controle_Charge_Ultime_0.drop('Ultimate')
Controle_Charge_Ultime_0 = Controle_Charge_Ultime_0.withColumnRenamed('Ultimate_new', 'Ultimate')
Controle_Charge_Ultime_0.createOrReplaceTempView('Controle_Charge_Ultime_0')

Controle_Charge_Ultime = spark.table('Controle_Charge_Ultime_0')
Controle_Charge_Ultime = (Controle_Charge_Ultime
    .withColumn('Test',
        F.when(F.expr("""round(Charge_ult_calculee,0.01)=round(Ultimate,0.01)"""), F.lit("Ok"))
         .otherwise(F.lit("Ko")))
)
Controle_Charge_Ultime.createOrReplaceTempView('Controle_Charge_Ultime')

def export_excelx(datatable, database, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


export_excelx(datatable=claims_actuals_9, database=export_01, sheet="Reserves - Claims")
export_excelx(datatable=Controle_Charge_Ultime, database=export_02, sheet="Controle-Charge_Ultime")
# FILENAME DOEMAIL EMAIL → use smtplib or yagmail
# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.base import MIMEBase
# smtp = smtplib.SMTP('smtp.your-server.com', 587)
# DATA _NULL_ email → smtplib scaffold
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
_msg = MIMEMultipart()
_msg['From']    = "iheb.karoui@axa.fr"
_msg['To']      = f"{adresse_mail}"
_msg['Cc']      = f"{adresse_mail}"
_msg['Subject'] = "Validation de la repart claims"
_msg.attach(MIMEText("Bonjour,\nCi-joint le controle sur la sinistralité de CQS.\nVerifier que la dernère colonne est OK avant de passer à l'analyse des B/M.".\nCordialement,", 'plain'))
with open(f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves/Test_CU_CQS.xlsx", 'rb') as _f:
    _part = MIMEBase('application', 'octet-stream')
    _part.set_payload(_f.read())
    encoders.encode_base64(_part)
    _part.add_header('Content-Disposition', f'attachment; filename="f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves/Test_CU_CQS.xlsx""')
    _msg.attach(_part)
# TODO: configure your SMTP server
# with smtplib.SMTP('smtp.your-server.com', 587) as _smtp:
#     _smtp.starttls()
#     _smtp.login('user', 'password')
#     _smtp.sendmail("iheb.karoui@axa.fr", [f"{adresse_mail}"], _msg.as_string())
