from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #####################################################################################################################################################################################
# ##########################################################   EXTRACTION CLAIM PAID & RISK EARNED PREMIUM : NON-CORE COVER    ######################################################################################
# #####################################################################################################################################################################################
# ######## Name: EXTRACTION NON-CORE COVER
# ######## Author: ALSENY SOW
# ######## Date started :15/11/2018
# ######## Date finished:
# ######## Context: RESERVING NON-CORE COVER LPI
arrete_carto = "2026_04_V2"
# Mettre l'arreté le plus récent si on est hors arreté
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
dt_arrete_reel = "26JUN2026"d"
cr_q226_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"  # LIBNAME CR_Q226
spark.sql('CREATE SCHEMA IF NOT EXISTS cr_q226')  # base Spark pour LIBNAME CR_Q226
out_gep_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/GEP/Output/DAAP"  # LIBNAME Out_GEP
tia_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
tia2_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2021_11_Q4/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA2
spark.sql('CREATE SCHEMA IF NOT EXISTS tia2')  # base Spark pour LIBNAME TIA2
tia2_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete_carto}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA2
balancedate = "26/06/2026"
balancequarter = "Q22026"
quarter = "2026Q2"
ouput = "CR_Q226"
import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_02, out="SDB", onglet="SDB2")
import_excel(file=import_02, out="Carto_TIA", onglet="Carto_TIA")
# RETAIN variables (initial values): {'country': '0', 'scheme': '0', 'agent_name': '0', 'Products': '0'}
scheme_databse = spark.table('SDB')
scheme_databse = (scheme_databse
    .withColumn('Products', F.lit(None).cast(StringType()))  # LENGTH Products $50
    .withColumn('Products', F.when(F.expr("""Sub_Product='AUTFIN'"""), F.lit('Auto Finance')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='INCPRO'"""), F.lit('Income Protection')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='MORTGA'"""), F.lit('Mortgage')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='PERACC'"""), F.lit('Personal Accident')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='PERLOA'"""), F.lit('Personal Loan')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='INSPRE'"""), F.lit('WOP')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='CCANPP'"""), F.lit('Credit Card')))
    .withColumn('Products', F.when(F.expr("""Sub_Product IN ('GAPCAR','GUAASS','GAPEQT')"""), F.lit('GAP')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='EXPPRO'"""), F.lit('Expense Protection')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='SCIINS'"""), F.lit('Standalone Critical Illness')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='LUMPSU'"""), F.lit('Lump Sum')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='FIPINS'"""), F.lit('Family Income Plan')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='MISCPL'"""), F.lit('Misc Pecuniary')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='MISCLE'"""), F.lit('Miscellaneous Life Events')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='MISCPR'"""), F.lit('Miscellaneous Price Protection')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='LIFINS'"""), F.lit('Standalone Life')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='SSIINS'"""), F.lit('Standalone Serious Illness')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='MISCSW'"""), F.lit('Miscellaneous Wallet Protection')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='KEYMAN'"""), F.lit('Standalone Keyman')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='WARRAN'"""), F.lit('Warranty')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='TRAVEL'"""), F.lit('TRAVEL')))
    .withColumn('Products', F.when(F.expr("""Sub_Product='PRPROT'"""), F.lit('Price Protection')))
)
# FORMAT/INFORMAT: format Products $50.
# FORMAT/INFORMAT: format agent_name $25.
scheme_databse = scheme_databse.select('country', 'scheme', 'agent_name', 'Products', 'Sub_Product')
scheme_databse = scheme_databse.withColumnRenamed('agent_name', 'Agent')
scheme_databse.createOrReplaceTempView('scheme_databse')
# LIBNAME {ouput} -> base Spark: {ouput}.scheme_databse
scheme_databse.write.mode('overwrite').saveAsTable(f'{ouput}.scheme_databse')

scheme_databse = spark.table(f'{ouput}.scheme_databse')
scheme_databse = scheme_databse.withColumnRenamed('Products', 'Product')
scheme_databse.createOrReplaceTempView('scheme_databse')
# LIBNAME {ouput} -> base Spark: {ouput}.scheme_databse
scheme_databse.write.mode('overwrite').saveAsTable(f'{ouput}.scheme_databse')

carto_TIA2 = spark.table('tia2.CARTO_TIA')
# TIA.carto_TIA
carto_TIA2.createOrReplaceTempView('carto_TIA2')

carto_TIA2 = spark.table('carto_TIA2').orderBy('_all_')
carto_TIA2 = carto_TIA2.dropDuplicates(['_all_'])
carto_TIA2.createOrReplaceTempView('carto_TIA2')

def extraction(pays, country_name):
    # %LET pays =ES ;
    # %LET country_name =SPAIN;
    # IMPORTATION DE LA LISTE DES CLIENTS TIA
    _dfs[f'CLAIM_PAID_{pays}'] = spark.table(f'{ouput}.CLMHDR_ALL_{pays}')
    _dfs[f'CLAIM_PAID_{pays}'] = (_dfs[f'CLAIM_PAID_{pays}']
        .withColumn('Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat("Q1",Acc_Yr)""")))
        .withColumn('Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat("Q2",Acc_Yr)""")))
        .withColumn('Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat("Q3",Acc_Yr)""")))
        .withColumn('Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat("Q4",Acc_Yr)""")))
        .withColumn('Month', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,Acc_Mnth)""")))
        .withColumn('Month', F.when(F.expr("""Acc_Mnth IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(Acc_Yr,0,Acc_Mnth)""")))
        .withColumn('Cohort', F.expr("""year(Incptn_Dt)"""))
    )
    _dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].select('Country', 'Rsrv_Grp', 'Clm_Nmbr', 'Schm', 'Cvr_Typ', 'Quarter', 'Cohort', 'Undrwrtng_Cmpny', 'Totl_Amnt_Pd', 'Rsrv_Amt', 'Month')
    _dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].withColumnRenamed('Schm', 'scheme')
    _dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].withColumnRenamed('Cvr_Typ', 'cover')
    _dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'CLAIM_PAID_{pays}'].createOrReplaceTempView(f'CLAIM_PAID_{pays}')

    _dfs[f'CLAIM_PAID_{pays}'] = spark.sql(f"""SELECT DISTINCT country,
                        Rsrv_Grp,
                        COVER,
                        scheme ,
                        Entity_CD ,
                        Cohort,
                        Quarter,
                        Month,
                        sum(Totl_Amnt_Pd) as Claim_Paid
                                                           
         FROM    CLAIM_PAID_{pays}
         group by Country,COVER,scheme,Entity_CD,Cohort, Quarter,Month 
          """)
    _dfs[f'CLAIM_PAID_{pays}'].createOrReplaceTempView(f'CLAIM_PAID_{pays}')

    # UPF
    # DATA GEP_UPF_&pays.;
    # keep country scheme cover Cohort gl_type_no GEP QTR_Period Month  ;
    # set OUT_GEP.&country_name._UPF;
    # where EXP_PERIOD <= &DT_Arrete_Reel. ;
    # rename QTR_Period=Quarter  ;
    # Mois=month(EXP_PERIOD) ;
    # year=year(EXP_PERIOD) ;
    # if Mois in (10,11,12) then Month=cats(year,Mois) ;
    # if Mois in (1,2,3,4,5,6,7,8,9) then  Month=cats(year,0,Mois) ;
    # if GEP=. then GEP=0;
    # run;
    # BLK
    _dfs[f'GEP_BLK_{pays}'] = spark.table(f'out_gep.{country_name}_BLK')
    _dfs[f'GEP_BLK_{pays}'] = _dfs[f'GEP_BLK_{pays}'].filter(F.expr(f"""EXP_PERIOD <= {dt_arrete_reel}"""))
    _dfs[f'GEP_BLK_{pays}'] = (_dfs[f'GEP_BLK_{pays}']
        .withColumn('Mois', F.expr("""month(EXP_PERIOD)"""))
        .withColumn('year', F.expr("""year(EXP_PERIOD)"""))
        .withColumn('Month', F.when(F.expr("""Mois IN (10,11,12)"""), F.expr("""concat(year,Mois)""")))
        .withColumn('Month', F.when(F.expr("""Mois IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(year,0,Mois)""")))
        .withColumn('GEP', F.when(F.expr("""GEP IS NULL"""), F.lit(0)))
    )
    _dfs[f'GEP_BLK_{pays}'] = _dfs[f'GEP_BLK_{pays}'].select('country', 'scheme', 'cover', 'Cohort', 'gl_type_no', 'GEP', 'QTR_Period', 'Month')
    _dfs[f'GEP_BLK_{pays}'] = _dfs[f'GEP_BLK_{pays}'].withColumnRenamed('QTR_Period', 'Quarter')
    _dfs[f'GEP_BLK_{pays}'].createOrReplaceTempView(f'GEP_BLK_{pays}')

    # MF
    _dfs[f'GEP_MF_{pays}'] = spark.table(f'out_gep.{country_name}_MF')
    _dfs[f'GEP_MF_{pays}'] = _dfs[f'GEP_MF_{pays}'].filter(F.expr(f"""EXP_PERIOD <= {dt_arrete_reel}"""))
    _dfs[f'GEP_MF_{pays}'] = (_dfs[f'GEP_MF_{pays}']
        .withColumn('Mois', F.expr("""month(EXP_PERIOD)"""))
        .withColumn('year', F.expr("""year(EXP_PERIOD)"""))
        .withColumn('Month', F.when(F.expr("""Mois IN (10,11,12)"""), F.expr("""concat(year,Mois)""")))
        .withColumn('Month', F.when(F.expr("""Mois IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(year,0,Mois)""")))
        .withColumn('GEP', F.when(F.expr("""GEP IS NULL"""), F.lit(0)))
    )
    _dfs[f'GEP_MF_{pays}'] = _dfs[f'GEP_MF_{pays}'].select('country', 'scheme', 'Cohort', 'cover', 'gl_type_no', 'GEP', 'QTR_Period', 'Month')
    _dfs[f'GEP_MF_{pays}'] = _dfs[f'GEP_MF_{pays}'].withColumnRenamed('QTR_Period', 'Quarter')
    _dfs[f'GEP_MF_{pays}'].createOrReplaceTempView(f'GEP_MF_{pays}')

    # MR
    _dfs[f'GEP_MTRANS_{pays}'] = spark.table(f'out_gep.{country_name}_MTRANS')
    _dfs[f'GEP_MTRANS_{pays}'] = _dfs[f'GEP_MTRANS_{pays}'].filter(F.expr(f"""EXP_PERIOD <= {dt_arrete_reel}"""))
    _dfs[f'GEP_MTRANS_{pays}'] = (_dfs[f'GEP_MTRANS_{pays}']
        .withColumn('Mois', F.expr("""month(EXP_PERIOD)"""))
        .withColumn('year', F.expr("""year(EXP_PERIOD)"""))
        .withColumn('Month', F.when(F.expr("""Mois IN (10,11,12)"""), F.expr("""concat(year,Mois)""")))
        .withColumn('Month', F.when(F.expr("""Mois IN (1,2,3,4,5,6,7,8,9)"""), F.expr("""concat(year,0,Mois)""")))
        .withColumn('GEP', F.when(F.expr("""GEP IS NULL"""), F.lit(0)))
    )
    _dfs[f'GEP_MTRANS_{pays}'] = _dfs[f'GEP_MTRANS_{pays}'].select('country', 'scheme', 'cover', 'Cohort', 'gl_type_no', 'GEP', 'QTR_Period', 'Month')
    _dfs[f'GEP_MTRANS_{pays}'] = _dfs[f'GEP_MTRANS_{pays}'].withColumnRenamed('QTR_Period', 'Quarter')
    _dfs[f'GEP_MTRANS_{pays}'].createOrReplaceTempView(f'GEP_MTRANS_{pays}')

    # concat
    from functools import reduce
    _dfs[f'GEP_{pays}'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'GEP_MF_{pays}'), spark.table(f'GEP_MTRANS_{pays}'), spark.table(f'GEP_BLK_{pays}')])
    # GEP_UPF_{pays}
    _dfs[f'GEP_{pays}'].createOrReplaceTempView(f'GEP_{pays}')

    _dfs[f'GEP_{pays}'] = spark.sql(f"""SELECT DISTINCT country,
                        scheme,
                        cover,
                        gl_type_no AS Entity_CD,
                        Cohort,
                        Quarter,
                        Month ,
                        sum(GEP) as GEP
                                                           
         FROM    GEP_{pays}
         group by Country,scheme,cover, gl_type_no,Cohort, Quarter, Month 
          """)
    _dfs[f'GEP_{pays}'].createOrReplaceTempView(f'GEP_{pays}')

    _dfs[f'GEP_{pays}_0'] = spark.sql(f"""select DISTINCT
    t1.*,
    t2.Taux_Comm as Taux_Comm,
    t2.retention_1_num as retention_1_num
    from GEP_{pays} t1  
    left join  CARTO_TIA2  t2 on (t1.country=t2.country AND t1.Scheme=t2.Scheme AND t1.COVER=t2.COVER)""")
    _dfs[f'GEP_{pays}_0'].createOrReplaceTempView(f'GEP_{pays}_0')

    _dfs[f'GEP_{pays}_1'] = spark.table(f'GEP_{pays}_0')
    _dfs[f'GEP_{pays}_1'] = (_dfs[f'GEP_{pays}_1']
        .withColumn('Taux_Comm', F.when(F.expr("""Taux_Comm IS NULL"""), F.lit(0)))
        .withColumn('retention_1_num', F.when(F.expr("""retention_1_num IS NULL"""), F.lit(0)))
        .withColumn('comm_amount', F.expr("""(Taux_Comm/100)*GEP"""))
        .withColumn('ret_amount', F.expr("""retention_1_num*GEP"""))
        .withColumn('REP', F.expr("""GEP-comm_amount-ret_amount"""))
    )
    _dfs[f'GEP_{pays}_1'].createOrReplaceTempView(f'GEP_{pays}_1')

    do()
    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/{pays} Model Properties.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_02, out=f"{pays}_RESERVE_GROUP_SPEC", onglet="RESERVE_GROUP_SPEC3")
_dfs[f'GEP_{pays}_2'] = spark.sql(f"""SELECT
			h.*,
			Rsrv_Grp
	FROM GEP_{pays}_1 h
	LEFT JOIN {pays}_RESERVE_GROUP_SPEC s ON (h.Cover = s.Cvr_Typ )
	""")
_dfs[f'GEP_{pays}_2'].createOrReplaceTempView(f'GEP_{pays}_2')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'scheme': '0', 'cover': '0', 'Entity_CD': '0', 'Cohort': '0', 'Quarter': '0', 'Month': '0', 'GEP': '0', 'comm_amount': '0', 'ret_amount': '0', 'REP': '0'}
_dfs[f'GEP_{pays}_all'] = spark.table(f'GEP_{pays}_2')
_dfs[f'GEP_{pays}_all'] = (_dfs[f'GEP_{pays}_all']
    .withColumn('Rsrv_Grp', F.when(F.expr("""Rsrv_Grp=''"""), F.lit('ZZ1')))
    .withColumn('GEP', F.when(F.expr("""GEP <0"""), F.expr("""-GEP""")))
    .withColumn('REP', F.when(F.expr("""REP <0"""), F.expr("""-REP""")))
)
_dfs[f'GEP_{pays}_all'] = _dfs[f'GEP_{pays}_all'].select('country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month', 'GEP', 'comm_amount', 'ret_amount', 'REP')
_dfs[f'GEP_{pays}_all'].createOrReplaceTempView(f'GEP_{pays}_all')

end()
if {pays}=FR:
    %let Import_02="~/NAS/&LReseau./08.Progammes/INTERNATIONAL/06_Inventaire CLP/&Arrete./02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/&pays. Model Properties.xlsx" ;

    
%MACRO IMPORT_EXCEL(FILE=,OUT=,ONGLET=,);
PROC IMPORT                 
DATAFILE= &FILE.
OUT= WORK.&OUT. 
DBMS=xlsx REPLACE

;
SHEET=&ONGLET. ;
RUN;
%MEND;

%IMPORT_EXCEL(FILE=&Import_02.,OUT=&pays._RESERVE_GROUP_SPEC,ONGLET=RESERVE_GROUP_SPEC3);
%IMPORT_EXCEL(FILE=&Import_02.,OUT=&pays._SCHEME_DATABASE,ONGLET=SCHEME_DATABASE);

PROC SQL;
	CREATE TABLE &pays._SCHEME_DATABASE as 
	SELECT DISTINCT
			Schm,
			COVER_TYPE,
			SUB_PRODUCT ,
			PAYMENT_BENEFIT as Clm_Pymnt_Basis,
			PRODUCT_TYPE
	FROM &pays._SCHEME_DATABASE 

	;
QUIT;


PROC SQL;
	CREATE TABLE GEP_&pays._11 as 
	SELECT DISTINCT
			h.*,
			s.SUB_PRODUCT ,
			s.Clm_Pymnt_Basis as Clm_Pymnt_Basis,
			s.PRODUCT_TYPE
	FROM GEP_&pays._1 h
	LEFT JOIN &pays._SCHEME_DATABASE s ON (h.SCHEME=s.Schm and  h.cover = s.COVER_TYPE )
	;
QUIT;

proc sort data=GEP_&pays._11 nodupkey out=GEP_&pays._11; 
 by country scheme cover Entity_CD Cohort Quarter Month ; 
run;

data GEP_&pays._1 ;
set GEP_&pays._11 ;
if SUB_PRODUCT not in ("MORTGAGE") then Sub_Prdct ="NMORTGAGE" ;
if SUB_PRODUCT     in ("MORTGAGE") then Sub_Prdct ="MORTGAGE" ;
run;


PROC SQL;
	CREATE TABLE GEP_&pays._2 as 
	SELECT DISTINCT 
			h.*,
			Rsrv_Grp
	FROM GEP_&pays._1 h
	LEFT JOIN &pays._RESERVE_GROUP_SPEC s ON (h.cover = s.Cvr_Typ and h.Sub_Prdct=s.Sub_Prdct and h.Clm_Pymnt_Basis=s.Clm_Pymnt_Basis )
	;
QUIT;

data GEP_&pays._2;

	set GEP_&pays._2;
	if Rsrv_Grp = "" and cover in ('DA','DB','DS','DC') then Rsrv_Grp = "GD1";
    if Rsrv_Grp = "" and cover in ('DI','DJ') then Rsrv_Grp = "GD3";
    if Rsrv_Grp = "" and cover in ('RR','RU') then Rsrv_Grp = "GR1";
    if Rsrv_Grp = "" and cover in ('GP') then Rsrv_Grp = "GP1";
    if Rsrv_Grp = "" and cover in ('LA','LL','LR','DY','DZ') then Rsrv_Grp = "GL1";
	if Rsrv_Grp = ""  then Rsrv_Grp = "ZZ1";
run;

data GEP_&pays._all ;
retain country Rsrv_Grp scheme cover Entity_CD Cohort Quarter Month GEP comm_amount ret_amount REP  ;
keep country Rsrv_Grp scheme cover Entity_CD Cohort Quarter Month GEP  comm_amount ret_amount REP ;
set GEP_&pays._2 ;
if Rsrv_Grp="" then Rsrv_Grp="ZZ1" ;
run;
# #####################################################  FILTRE DES SCHEMES QUI NE FONT PAS PARTI DU ON-SYSTEM  #########################################################################################
# The following codes put any claims which we want to filter out and not hold reserves for into group ZZ2.
# This replaces the previous deletion of these claims from &pays._clmhdr_all in A2 Filter. DP 23/07/2014
# S1-S7 are Santander business that is now reserved off system using bordereau.
# H1-H6, HPA are Hispamer business that is now reserved off system using bordereau
if {pays} = ES:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "H1%"
OR Scheme Like "H2%"
OR Scheme Like "H3%"
OR Scheme Like "H4%"
OR Scheme Like "H5%"
OR Scheme Like "H6%"
OR Scheme Like "HPA%"
OR Scheme Like "S1%"
OR Scheme Like "S2%"
OR Scheme Like "S3%"
OR Scheme Like "S4%"
OR Scheme Like "S5%"
OR Scheme Like "S6%"
OR Scheme Like "S7%";
Quit;
# Linea Schemes are reserved by loss ratio
if {pays} =IT:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "LN1%";
Quit;
# Norway TERRA Schemes are excluded
if {pays} = NO:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme IN("TA.1","TB.1","TC.1","TD.1","TE.1","TF.1","TG.1","TH.1","TI.1","TJ.1");
Quit;
# Added 08/06/2017 to deal with 501/502 uw codes.
# Germany and turkey are only country which have these codes at time of writing, Underwriting company 501/502 need not be evaluated, so remove.
if {pays} = DE OR {pays} = TR:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Entity_CD IN (501,502);
Quit;
# removing of CNP Santander TPA Schemes for DK, FI, NO, SE
if {pays} = DK:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "5B%" or Scheme like "5C%";
Quit;
if {pays} = FI:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "SN%" ;
Quit;
if {pays} = NO:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "ED.%" or Scheme Like "EE.%" or Scheme Like "EG.%" or Scheme Like "EH.%" or Scheme Like "EI.%" or Scheme Like "EJ.%" or Scheme Like "EK.%" or Scheme Like "EL.%" or Scheme Like "EM.%";
Quit;
if {pays}= SE:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "ED.%" or Scheme Like "EE.%" or Scheme Like "EF.%" or Scheme Like "EG.%" or Scheme Like "EH.%" or Scheme Like "EI.%" or Scheme Like "EJ.%";
Quit;
# From Q3 2012 some claims started being classified under dummy Schemes 8A.1 and ZA.1 due to them being in bulk and not having an
# identifiable Scheme.  These should not have reserves.
if {pays} =SE:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "ZA.%" ;
Quit;
# %DO block (non-iterative): %DO;
Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "8A.%";
Quit;
%END;
# Capital One UK contract and run-off period ended on 27th November 2013
if {pays} = UK:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "CFA%" OR Scheme Like "CFN%";
Quit;
# Ceasing business with the client DLFA in Denmark from 01/04/14 -
# All claims then paid by client including those which are outstanding
if {pays} = DK:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme Like "Q%";
Quit;
# Some Greece Schemes have run off more than 12 months ago and terms and conditions
# donï¿½t allow for claims 12 month after insurance period. Added by DP 23/07/2014
if {pays} = GR:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme IN ("BPI.1","BPJ.1","BPK.1","BPL.1","BPM.1","EM1.1","EM2.1","GM1.1");
Quit;
# We had some German contracts that were terminated as part of project bounce.
# We were required to pay claims up to a certain period. That period has now lapsed. Added by DP 23/07/2014
if {pays} = DE:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme IN ("P4.2","P4.3");
Quit;
# Irish Santander Schemes nearly run-off. Added by DP 03/11/2014
if {pays} = IE:
    Proc Sql;
Update GEP_&pays._all 
Set Rsrv_Grp = "ZZ2"
WHERE Scheme IN ("EV.3","EV.4");
Quit;
# Some France UGIP claims on TIA but full history not loaded so continue reserving off-system . Added by DP 18/09/2014
# %IF &pays. = FR %THEN %DO;
# Proc Sql;
# Update GEP_&pays._all
# Set Rsrv_Grp = "ZZ2"
# WHERE Scheme like "UG1%" or Scheme like "UG6%" or Scheme like "UG7%" or Scheme like "UG8%"
# or Scheme like "UG9%" or Scheme like "UGC%" or Scheme like "UGD%" or Scheme like "UGG%"
# or Scheme like "UGH%" or Scheme like "UGI%" or Scheme like "UGN%" or Scheme like "UGX%";
# Quit;
# %END;
# Some France GMAC policies needed to be switched from GL Type 101 to 102 (therefore underwritten by FACL) as part of the
# AS60 transfer. Added by TD 09/09/2015
if {pays} = FR:
    Proc Sql;
Update GEP_&pays._all
Set Entity_CD = 102
Where Scheme like "1%" and cover like "D%";
Quit;
# Merge GEP & claims
_dfs[f'CLAIM_PAID_{pays}'] = spark.table(f'CLAIM_PAID_{pays}')
_dfs[f'CLAIM_PAID_{pays}'] = (_dfs[f'CLAIM_PAID_{pays}']
    .withColumn('Entity_CD2', F.expr("""Entity_CD*1"""))
)
_dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].drop('Entity_CD')
_dfs[f'CLAIM_PAID_{pays}'].createOrReplaceTempView(f'CLAIM_PAID_{pays}')

_dfs[f'CLAIM_PAID_{pays}'] = spark.table(f'CLAIM_PAID_{pays}')
_dfs[f'CLAIM_PAID_{pays}'] = _dfs[f'CLAIM_PAID_{pays}'].withColumnRenamed('Entity_CD2', 'Entity_CD')
_dfs[f'CLAIM_PAID_{pays}'].createOrReplaceTempView(f'CLAIM_PAID_{pays}')

_dfs[f'GEP_{pays}_all'] = spark.table(f'GEP_{pays}_all').orderBy('country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month')
_dfs[f'GEP_{pays}_all'].createOrReplaceTempView(f'GEP_{pays}_all')

_dfs[f'CLAIM_PAID_{pays}'] = spark.table(f'CLAIM_PAID_{pays}').orderBy('country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month')
_dfs[f'CLAIM_PAID_{pays}'].createOrReplaceTempView(f'CLAIM_PAID_{pays}')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'scheme': '0', 'cover': '0', 'Entity_CD': '0', 'Cohort': '0', 'Quarter': '0', 'Month': '0', 'GEP': '0', 'comm_amount': '0', 'ret_amount': '0', 'REP': '0', 'Claim_Paid': '0'}
# MERGE: FULL OUTER JOIN (if a or b / no condition)
_dfs[f'GEP_CLAIM_{pays}_all'] = spark.table('GEP_').join(spark.table('CLAIM_PAID_'), ['country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month'], 'full')
_dfs[f'GEP_CLAIM_{pays}_all'] = (_dfs[f'GEP_CLAIM_{pays}_all']
    .withColumn('GEP', F.when(F.expr("""GEP IS NULL"""), F.lit(0)))
    .withColumn('comm_amount', F.when(F.expr("""comm_amount IS NULL"""), F.lit(0)))
    .withColumn('ret_amount', F.when(F.expr("""ret_amount IS NULL"""), F.lit(0)))
    .withColumn('REP', F.when(F.expr("""REP IS NULL"""), F.lit(0)))
    .withColumn('Claim_Paid', F.when(F.expr("""Claim_Paid IS NULL"""), F.lit(0)))
)
_dfs[f'GEP_CLAIM_{pays}_all'] = _dfs[f'GEP_CLAIM_{pays}_all'].select('country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month', 'GEP', 'comm_amount', 'ret_amount', 'REP', 'Claim_Paid')
_dfs[f'GEP_CLAIM_{pays}_all'].createOrReplaceTempView(f'GEP_CLAIM_{pays}_all')

_dfs[f'GEP_CLAIM_{pays}_all'] = spark.table(f'GEP_CLAIM_{pays}_all').orderBy('country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month')
_dfs[f'GEP_CLAIM_{pays}_all'] = _dfs[f'GEP_CLAIM_{pays}_all'].dropDuplicates(['country', 'Rsrv_Grp', 'scheme', 'cover', 'Entity_CD', 'Cohort', 'Quarter', 'Month'])
_dfs[f'GEP_CLAIM_{pays}_all'].createOrReplaceTempView(f'GEP_CLAIM_{pays}_all')

_dfs[f'GEP_CLAIM_{pays}_all'] = spark.sql(f"""SELECT distinct
			h.*,
			s.Agent,
			s.Product
	FROM GEP_CLAIM_{pays}_all h
	left JOIN {ouput}.SCHEME_DATABSE s ON (h.Country=s.Country  and h.scheme=s.scheme)
	
	""")
_dfs[f'GEP_CLAIM_{pays}_all'].createOrReplaceTempView(f'GEP_CLAIM_{pays}_all')

_dfs[f'HISTO_FLUX_{pays}'] = spark.table(f'GEP_CLAIM_{pays}_all')
_dfs[f'HISTO_FLUX_{pays}'].createOrReplaceTempView(f'HISTO_FLUX_{pays}')
# LIBNAME Out_GEP -> base Spark: out_gep.HISTO_FLUX_{pays}
_dfs[f'HISTO_FLUX_{pays}'].write.mode('overwrite').saveAsTable(f'out_gep.HISTO_FLUX_{pays}')

_dfs[f'HISTO_FLUX_{pays}'] = spark.sql(f"""SELECT DISTINCT country,
                    Rsrv_Grp,
                    scheme,
                    cover,
                    Entity_CD AS Entity_CD,
                    Cohort,
                    Quarter,
                    Month ,
                    sum(GEP) as GEP,
                    sum(REP) as REP,
                    sum(Claim_Paid) as Claim_Paid,
                    Agent,
                    Product
                                                      
     FROM    Out_GEP.HISTO_FLUX_{pays}
     group by Country,Rsrv_Grp,scheme,cover, Entity_CD,Cohort, Quarter, Month, Agent,Product
                    
      """)
_dfs[f'HISTO_FLUX_{pays}'].createOrReplaceTempView(f'HISTO_FLUX_{pays}')

mend()
# les 4 type de primes
extraction(pays="DE", country_name="GERMANY")
extraction(pays="ES", country_name="SPAIN")
extraction(pays="PT", country_name="PORTUGAL")
extraction(pays="GR", country_name="GREECE")
extraction(pays="DK", country_name="DENMARK")
extraction(pays="IE", country_name="IRELAND")
extraction(pays="TR", country_name="TURKEY")
extraction(pays="CH", country_name="SWITZERLAND")
extraction(pays="IT", country_name="ITALY")
extraction(pays="UK", country_name="UK")
extraction(pays="AT", country_name="AUSTRIA")
# faut enlever le MF
extraction(pays="NO", country_name="NORWAY")
extraction(pays="FI", country_name="FINLAND")
extraction(pays="SE", country_name="SWEDEN")
extraction(pays="PL", country_name="POLAND")
extraction(pays="CO", country_name="COLOMBIA")
extraction(pays="MX", country_name="MEXICO")
extraction(pays="BE", country_name="BELGIUM")
# Faut enlever le BLK et le MF
extraction(pays="NL", country_name="NETHERLANDS")
# Il faut enlever le MR et le MF
extraction(pays="NI", country_name="NORTHERNIRELAND")
# faut enlever le UPF
extraction(pays="FR", country_name="FRANCE")
# Check pour voir s'il y a des agents manquants
def manquante(pays):
    _dfs[f'schim_manq_{pays}'] = spark.table(f'out_gep.HISTO_FLUX_{pays}')
    _dfs[f'schim_manq_{pays}'] = _dfs[f'schim_manq_{pays}'].filter(F.expr("""Agent =''"""))
    _dfs[f'schim_manq_{pays}'].createOrReplaceTempView(f'schim_manq_{pays}')


manquante("FR")
manquante("IE")
manquante("DE")
manquante("BE")
manquante("AT")
manquante("MX")
manquante("NL")
manquante("NI")
manquante("NO")
manquante("PT")
manquante("ES")
manquante("SE")
manquante("PL")
manquante("CO")
manquante("FI")
manquante("UK")
manquante("IT")
manquante("CH")
manquante("GR")
manquante("DK")