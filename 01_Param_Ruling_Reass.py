from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# /
# /
# A compléter par le user
# /
arrete = "2026_06_Prov"
n = 2026
version_ricp = "RICP 20260626"
quarter = "Q22026"
def export_excel(data, outfile, sheet):
    data.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(outfile)


def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)

    _dfs[f'{out}'] = spark.table(f'{out}')
    _dfs[f'{out}'] = _dfs[f'{out}'].filter(~F.expr("""COUNTRY= ''	AND COVER = '' AND  LANGUAGE =''"""))
    _dfs[f'{out}'].createOrReplaceTempView(f'{out}')


importation = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling/UEP Rules Production (All Countries).xlsx"
import_excel(file=importation, out="UEP_RULES_PROD", onglet="Sheet 1")
uep_rules = spark.table('uep_rules_prod')
uep_rules = uep_rules.select('DIM08')
uep_rules.createOrReplaceTempView('uep_rules')

uep_rules = spark.table('uep_rules').orderBy('DIM08')
uep_rules = uep_rules.dropDuplicates(['DIM08'])
uep_rules.createOrReplaceTempView('uep_rules')

uep_rules_prod_ = spark.table('uep_rules_prod')
uep_rules_prod_ = (uep_rules_prod_
    .withColumn('DIM08',
        F.when(F.expr("""DIM08 IN ('r_12')"""), F.lit('r12'))
         .when(F.expr("""DIM08 IN ('r_78p')"""), F.lit('r78'))
         .when(F.expr("""DIM08 IN ('r_78m')"""), F.lit('r78m'))
         .when(F.expr("""DIM08 IN ('(1-v)*r_12+v*r_78p')"""), F.lit('V'))
         .when(F.expr("""DIM08 IN ('1.5*r_12-0.5*r_78p')"""), F.lit('r45m')))
    .withColumn('DIM11_', F.expr("""DIM11*1"""))
    .withColumn('DIM07_', F.expr("""DIM07*1"""))
    .withColumn('DIM20_jour', F.expr("""split(DIM20, '\\.')[0]"""))
    .withColumn('DIM20_mois', F.expr("""split(DIM20, '\\.')[1]"""))
    .withColumn('DIM20_an', F.expr("""split(DIM20, '\\.')[2]"""))
    .withColumn('DIM20_new', F.expr("""make_date(DIM20_mois,DIM20_jour,DIM20_an)"""))
    .withColumn('DIM19_jour', F.expr("""split(DIM19, '\\.')[0]"""))
    .withColumn('DIM19_mois', F.expr("""split(DIM19, '\\.')[1]"""))
    .withColumn('DIM19_an', F.expr("""split(DIM19, '\\.')[2]"""))
    .withColumn('DIM19_new', F.expr("""make_date(DIM19_mois,DIM19_jour,DIM19_an)"""))
)
# FORMAT/INFORMAT: format DIM20_an  $4.
# FORMAT/INFORMAT: format DIM20_jour  DIM20_mois  $2.
# FORMAT/INFORMAT: format DIM20_new  DATE9.
# FORMAT/INFORMAT: format  DIM19_an $4.
# FORMAT/INFORMAT: format  DIM19_jour  DIM19_mois  $2.
# FORMAT/INFORMAT: format  DIM19_new DATE9.
uep_rules_prod_ = uep_rules_prod_.drop('DIM11')
uep_rules_prod_.createOrReplaceTempView('uep_rules_prod_')

uep_rules_prod_2 = spark.sql("""select COUNTRY,COVER,TABLE_NAME,LANGUAGE,DIM01,DIM02,DIM03,DIM04,DIM05,DIM06,DIM07_ as DIM07,DIM08,DIM09,DIM10,DIM11_ as DIM11,DIM12,DIM13,DIM14,DIM15,DIM16,DIM17,DIM18,DIM19_new as DIM19,DIM20_new as DIM20,START_DATE,END_DATE,SORT_NO,TIMESTAMP,USERID,RECORD_VERSION
		from uep_rules_prod_ """)
uep_rules_prod_2.createOrReplaceTempView('uep_rules_prod_2')

Ruling_Forecast = spark.table('uep_rules_prod_2')
Ruling_Forecast = Ruling_Forecast.filter(F.expr("""DIM09= 'F'"""))
Ruling_Forecast.createOrReplaceTempView('Ruling_Forecast')

Ruling_PS = spark.table('uep_rules_prod_2')
Ruling_PS = Ruling_PS.filter(F.expr("""DIM09= 'P'"""))
Ruling_PS.createOrReplaceTempView('Ruling_PS')

Ruling_L = spark.table('uep_rules_prod_2')
Ruling_L = Ruling_L.filter(F.expr("""DIM09= 'L'"""))
Ruling_L.createOrReplaceTempView('Ruling_L')

NBR1 = spark.table('uep_rules_prod').count()

NBR2 = spark.table('Ruling_Forecast').count()

NBR3 = spark.table('Ruling_PS').count()

NBR4 = spark.table('Ruling_L').count()

print(f"NOTE: la table uep_rules_prod contient {nbr1} lignes")
print(f"NOTE: la table Ruling_Forecast contient {nbr2}")
print(f"NOTE: la table Ruling_PS contient {nbr3}")
print(f"NOTE: la table Ruling_L contient {nbr4}")
print(f"NOTE: la table spliter contient %eval({nbr1} - ({nbr3} + {nbr2} + {nbr4}))")
chemin_output_ruling = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling"
export_excel(data=Ruling_PS, outfile=f"{chemin_output_ruling}/Ruling_{quarter}.xlsx", sheet="PS")
export_excel(data=Ruling_Forecast, outfile=f"{chemin_output_ruling}/Ruling_{quarter}.xlsx", sheet=f"Forecast")



/*########################################################## Parametre Reassurance ##########################################################################*/
/**** Import Carto reass*/
%let Reass="~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Ruling/{version_ricp}.xlsx";

%MACRO IMP_EXCEL2(FILE=", out="")
_dfs[f'{out}'] = (spark.read.format('com.crealytics.spark.excel')
    .option('dataAddress', '0!A1')
    .option('header', 'true')
    .load(file))
_dfs[f'{out}'].createOrReplaceTempView(f'{out}')

mend()
imp_excel2(file=reass, out="cartographie_reassurance")
cartographie_reassurance = spark.table('cartographie_reassurance')
# ATTRIB: attrib   COUNTRY  label="All schemes there are RI schemes / all remaining ones are direct"
# ATTRIB: attrib   DIM01  label="Product + Product Version"
# ATTRIB: attrib   DIM02  label="Cover"
# ATTRIB: attrib   DIM03  label="Original underwritter"
# ATTRIB: attrib   DIM04  label="AXA Reinsurer entity"
# ATTRIB: attrib   DIM05  label="Irrelevant - no longer used"
# ATTRIB: attrib   DIM06  label="Premium rate of RI / UEP"
# ATTRIB: attrib   DIM07  label="Commission / DAC"
# ATTRIB: attrib   DIM08  label="Claim + Claim reserves"
# ATTRIB: attrib   DIM10  label="AXA Reinsurer entity (SAG = P{c}  SAL = Life Z16 =P{c} for Spain  Z15 = Life for Spain Z11 = P{c}  CH = Country R59 = Life)"
# ATTRIB: attrib   DIM16  label="AXA Reinsurer entity except R59 has a G at the end of coding not relevant in the future"
# ATTRIB: attrib   DIM17  label="Product + Cover"
# ATTRIB: attrib   DIM18  label="Product_version"
# ATTRIB: attrib   DIM19  label="different from 0 & 100 & empty : local branches related - Mark confirms"
cartographie_reassurance.createOrReplaceTempView('cartographie_reassurance')

cartographie_reassurance_1 = spark.table('cartographie_reassurance')
cartographie_reassurance_1 = (cartographie_reassurance_1
    .withColumn('DIM06_', F.expr("""DIM06*1"""))
    .withColumn('DIM07_', F.expr("""DIM07*1"""))
    .withColumn('DIM08_', F.expr("""DIM08*1"""))
    .withColumn('DIM18_', F.expr("""DIM18*1"""))
)
cartographie_reassurance_1.createOrReplaceTempView('cartographie_reassurance_1')

cartographie_reassurance_2 = spark.sql("""select *, COUNTRY as country,DIM01 as  SCHEME1,	DIM02 as Cover,  DIM03 as Original_underwritter, DIM06_ as DIM06 ,DIM07_ as DIM07, DIM08_ as DIM08 
		 from cartographie_reassurance_1 """)
cartographie_reassurance_2.createOrReplaceTempView('cartographie_reassurance_2')

cartographie_reassurance_3 = spark.table('cartographie_reassurance_2')
cartographie_reassurance_3 = (cartographie_reassurance_3
    .withColumn('taille', F.expr("""length(SCHEME1)"""))
    .withColumn('SCHEME2',
        F.when(F.expr("""length(SCHEME1) = 3"""), F.expr("""substring(SCHEME1,1,2)"""))
         .otherwise(F.expr("""substring(SCHEME1,1,3)""")))
    .withColumn('Product', F.col('SCHEME2'))
    .withColumn('Version', F.col('DIM18'))
    .withColumn('SCHEME', F.expr("""regexp_replace(Product||"."||DIM18, ' ', '')"""))
    .withColumn('CLE', F.expr("""regexp_replace(Product||'_'||Cover||'_'||COUNTRY||'_'||Original_underwritter, ' ', '')"""))
    .withColumn('QP_rei_PREMIUM', F.expr("""DIM06/100"""))
    .withColumn('QP_rei_COMM', F.expr("""DIM07/100"""))
    .withColumn('QP_rei_CLAIM', F.expr("""DIM08/100"""))
    .withColumn('gl_type_no', F.expr("""Original_underwritter *1"""))
)
# ATTRIB: attrib SCHEME2 informat =$10.  format =$10.  label = "SCHEME2"
# ATTRIB: attrib Product  informat =$10.  format =$10.  label = "Product"
# ATTRIB: attrib Version   informat =$10.  format =$10.  label = "Version"
# ATTRIB: attrib SCHEME  informat  =$10. format =$10.  label = "SCHEME"
# ATTRIB: attrib CLE informat =$20. format =$20. label = "CLE"
# SCHEME2= substr(SCHEME1, 1, ifn(length(SCHEME1) = 3, 2, 3));
cartographie_reassurance_3.createOrReplaceTempView('cartographie_reassurance_3')

Parametres_Reas_new = spark.sql("""select country,Product,Version,SCHEME,CLE,Cover,gl_type_no as Original_underwritter,QP_rei_PREMIUM,QP_rei_COMM,QP_rei_CLAIM
		from cartographie_reassurance_3 """)
Parametres_Reas_new.createOrReplaceTempView('Parametres_Reas_new')

Entity_Mappingsv2_test = spark.sql("""select country,TABLE_NAME,LANGUAGE,SCHEME1 as SCHEME2,cover,DIM03 as entity_cd,DIM04,DIM05,DIM06,DIM07,DIM08,DIM09,DIM10,DIM11,DIM12,DIM13,DIM14,DIM15,DIM16,DIM17,DIM18,DIM19,DIM20,TIMESTAMP,USERID,SCHEME as SCHEME3 
		from cartographie_reassurance_3 """)
Entity_Mappingsv2_test.createOrReplaceTempView('Entity_Mappingsv2_test')

Export_SAS_2 = spark.sql("""select country,TABLE_NAME,LANGUAGE,SCHEME1 as SCHEME2, Product, Version ,SCHEME, cover,DIM03 as entity_cd,DIM04,DIM05,DIM06,DIM07,DIM08,DIM09,DIM10,DIM11,DIM12,DIM13,DIM14,DIM15,DIM16,DIM17,DIM18,DIM19,DIM20,TIMESTAMP,USERID
		from cartographie_reassurance_3 """)
Export_SAS_2.createOrReplaceTempView('Export_SAS_2')

chemin_output_reass = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties"
# export fichier Carto reass
export_excel(data=cartographie_reassurance, outfile=f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", sheet="Sheet 1")
%EXPORT_EXCEL(data = Export_SAS_2", outfile=f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", sheet="Export_SAS_2")
%EXPORT_EXCEL(data = Entity_Mappingsv2_test", outfile=f"{chemin_output_reass}/Cartographie_Reassurance.xlsx", sheet="Export_SAS")

/* export fichier Reass*/
%EXPORT_EXCEL(data = Parametres_Reas_new", outfile=f"{chemin_output_reass}/Reassurance.xlsx", sheet="Parametres_Reas")