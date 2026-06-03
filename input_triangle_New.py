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

# Sinistres réels
arrete = "2025_09_Q4"
lreseau = "X"
mois_cqs = 7
# données à fin juillet utilisées pour FY22
# Données à fin juin uilisées pour FY23
# Note :
# - pour la V2, nous avons généralement les données à fin février (2)
# - pour la V3, nous avons généralement les données à fin juin (6)
# - pour le Q4, nous avons généralement les données à fin août (8)
year_lim = 2025
nom_fichier_export = f"CQS_Etude_reserve_sinistres_DAAP_{arrete}"
import_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/GEP Stop Loss.xlsx"
import_xx1 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP/Output_GEP_Com_new.xlsx"
export_xx = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/03 - Claims + Reserves/{nom_fichier_export}.xlsx"
cqs_base_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP"  # LIBNAME CQS_Base
cqs_out_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/Macao/CQS CBP Process/02 - GEP"  # LIBNAME CQS_Out
# LIBNAME CQS_Q423 "~/NAS/&LReseau/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2023_09_Q4/02_Elements_techniques/Macao/CQS CBP Process/01 - Base CBP" ;
def import_excelx(datafile, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(datafile))
        _df_tmp.createOrReplaceTempView(out)


def export_excelx(database, datatable, sheet):
    datatable.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(database)


import_excelx(datafile=import_xx1, out="COM", onglet="COM")
import_excelx(datafile=import_xx1, out="GEP", onglet="GEP")
Claims_actuals = spark.table('cbp_italy_policies_claims_1125')
Claims_actuals = Claims_actuals.filter(F.expr(f"""(Montant_Sin IS NOT NULL AND Etat IN ("DA LIQUIDARE","LIQUIDATO","liquidato","IN LAVORAZIONE") AND Annee_Sin<{year_lim})"""))
Claims_actuals = (Claims_actuals
    .withColumn('Annee_Sin', F.when(F.expr("""Annee_Sin IS NULL"""), F.expr("""year(to_date(date_liquid))""")))  # no ELSE: null when condition is false
)
# Attention, à modifier à chaque arrêté
# Modification AJ V220 : ajout du "datepart" à cause du changement de format. Vérifier qu'il y en a toujours besoin, sinon supprimer.
Claims_actuals.createOrReplaceTempView('Claims_actuals')

sit_prof = spark.sql("""select distinct sit_prof_1
	from claims_actuals""")
sit_prof.createOrReplaceTempView('sit_prof')

# Situation professionnelle : Pensionato ; Privato ; Pubblico
Claims_actuals_2 = spark.sql("""select 
Financiere_adh,
Generation as gen,
Annee_Sin As Surv,
Type_Sin,
date_liquid,
type_pret,
Date_Recep,
sum(Montant_Sin) as Montant
from Claims_actuals 
group by Financiere_adh,Gen, type_pret,Annee_Sin,Type_Sin,date_liquid , Date_Recep """)
Claims_actuals_2.createOrReplaceTempView('Claims_actuals_2')

Claims_actuals_3 = spark.table('Claims_actuals_2')
Claims_actuals_3 = (Claims_actuals_3
    .withColumn('GAR', F.when(F.expr("""type_sin IN ('DC','dc')"""), F.lit(10)).otherwise(F.lit(30)))
)
Claims_actuals_3.createOrReplaceTempView('Claims_actuals_3')

Claims_actuals_4 = spark.table('Claims_actuals_3')
Claims_actuals_4 = (Claims_actuals_4
    .withColumn('CPTA', F.expr("""year(to_date(date_liquid))"""))
    .withColumn('Mois_cpta', F.expr("""month(to_date(date_liquid))"""))
)
# Modification AJ V220 : ajout du "datepart" à cause du changement de format. Vérifier qu'il y en a toujours besoin, sinon supprimer.
Claims_actuals_4 = Claims_actuals_4.filter(F.col('date_liquid').isNotNull())
Claims_actuals_4 = Claims_actuals_4.select('Financiere_Adh', 'Surv', 'Montant', 'GAR', 'CPTA', 'Mois_cpta', 'type_pret', 'gen')
Claims_actuals_4.createOrReplaceTempView('Claims_actuals_4')

Claims_actuals_5 = spark.sql("""select financiere_adh,''as Annee_Sin,'' as date_liquid,'' as Type_Sin,Montant,
GAR,CPTA,Surv,Mois_cpta,(cpta-surv)*12+Mois_cpta as mois_dev,type_pret,gen
from Claims_actuals_4 
group by Financiere_adh,Surv,GAR,CPTA,Mois_cpta,type_pret,gen """)
Claims_actuals_5.createOrReplaceTempView('Claims_actuals_5')

work = spark.table('Claims_actuals_5')
# cqs_out.
work = work.select('Financiere_Adh', 'Montant', 'GAR', 'CPTA', 'Surv', 'Type_Pret', 'gen')
work.createOrReplaceTempView('work')

Claims_actuals_6 = spark.table('Claims_actuals_5')
Claims_actuals_6 = (Claims_actuals_6
    .withColumn('annee_dev2',
        F.when(F.expr(f"""Mois_cpta> {mois_cqs}"""), F.col('cpta') - F.col('surv'))
         .otherwise(F.expr("""greatest(cpta-surv-1,0)""")))
)
Claims_actuals_6.createOrReplaceTempView('Claims_actuals_6')

cqs_uep_pl = spark.table('cqs_uep_pl_3')
cqs_uep_pl.createOrReplaceTempView('cqs_uep_pl')

cqs_uep_pl = spark.table('cqs_uep_pl').orderBy('GAR', 'SURV')
cqs_uep_pl.createOrReplaceTempView('cqs_uep_pl')

# PROC SUMMARY: SUM of ['UEP', 'DAC'] grouped by ['GAR', 'SURV']
UEP = cqs_uep_pl.groupBy('GAR', 'SURV').agg(F.sum('UEP').alias('UEP'), F.sum('DAC').alias('DAC'))
UEP.createOrReplaceTempView('UEP')

export_excelx(datatable=Claims_actuals_6, database=export_xx, sheet="SINISTRES_CBP")
export_excelx(datatable=com, database=export_xx, sheet="Com")
export_excelx(datatable=gep, database=export_xx, sheet="GEP")
export_excelx(datatable=UEP, database=export_xx, sheet="UEP")
