from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #####################################################################################################################################################################################
# ##########################################################       MODUL CALCUL CASE RESERVES    ######################################################################################
# #####################################################################################################################################################################################
# ######## Name: MODUL RESERVING CLP
# ######## Author: ALSENY SOW
# ######## Date started :26/06/2018
# ######## Date finished:10/07/2018
# ######## Context: CALCUL DES CASES RESERVES ON-SYSTEME: ICOP & RBNP
# ######## Name: MODUL RESERVING CLP UPDATED
# ######## Author: GNANISSO SARE
# ######## Date started :02/01/2024
# ######## Date finished:05/01/2024
# ######## Context: CALCUL DES CASES RESERVES ON-SYSTEME: ICOP & RBNP
# #####################################################  CREATION DES LIBRARY  #########################################################################################
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
exer = "Q226"
input_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Input"  # LIBNAME input
balancedate = "26/06/2026"
# ######### Modification à faire:
month_reserving = 06
day_reserving = 26
yr_reserving = 2026
input_ = "input"
ouput = "CR_Q226"
# #########################################################################################################################################################################################
# ######################################################  MACRO DE CALCUL DES RESERVES DE TOUT LES PAYS HORS FRANCE    ####################################################################
# #########################################################################################################################################################################################
def acr(yr_of_calculation, cover_typ, pays, guideline):
    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Mapping Cover Initial.xlsx"
    import_022 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Mapping Cover Updated.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


# REGROUPEMENT DES COUVERTURES
import_excel(file=import_02, out="Mapping_cover_1", onglet="Mapping_cover")
import_excel(file=import_022, out="Mapping_cover_2", onglet="Mapping_cover")
_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{input}.{pays}_CLMHDR_all')
_dfs[f'{pays}_CLMHDR_ALL'] = (_dfs[f'{pays}_CLMHDR_ALL']
    .withColumn('Prod_type', F.lit(None).cast(StringType()))  # LENGTH Prod_type $40
    .withColumn('Prod_type',
        F.when(F.expr("""Rsrv_Grp IN ('GD1','GL1','GR1')"""), F.lit('Mortgage'))
         .otherwise(F.lit('Non_Mortgage')))
)
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

# new
_dfs[f'{pays}_CLMHDR_ALL'] = spark.sql(f"""select t1.*, t2.Cover
     from {pays}_CLMHDR_ALL t1
     %if {pays} = GR or {pays} = SE  or {pays} = NO or {pays} = IT %then %do;
         left join MAPPING_COVER_2 t2
     %end;
     %else %do;
        left join MAPPING_COVER_1 t2
     %end;
     on t1.Cvr_Typ = t2.Cvr_Typ""")
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

# old
# Proc SQL; Create Table &pays._CLMHDR_ALL As
# Select distinct t1.*,t2.Cover
# From &pays._CLMHDR_ALL t1 Left Join  MAPPING_COVER_1 t2 on (t1.Cvr_Typ = t2.Cvr_Typ)
# ;
# quit;
_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{pays}_CLMHDR_ALL')
_dfs[f'{pays}_CLMHDR_ALL'] = (_dfs[f'{pays}_CLMHDR_ALL']
    .withColumn('Cover', F.when(F.expr("""Country='SE' AND Cvr_Typ IN ('DK')"""), F.lit('DIS')))
)
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

_dfs[f'Reserves_all_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*
From {pays}_CLMHDR_ALL t1 
where  t1.cover='{cover_typ}' and t1.Rsrv_Grp not in ('ZZ1','ZZ2')  
""")
_dfs[f'Reserves_all_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{pays}_V{yr_of_calculation}')

# RECUPERATION DE LA DERNIERE DATE DE TRANSACTION POUR CHAQUE CLAIMS
_dfs[f'{pays}_FIRSTLASTBENEFIT'] = spark.sql(f"""SELECT Clm_Nmbr,

				
				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(min(Trns_Dt))=12 then Year(min(Trns_Dt)) else Year(min(Trns_Dt)) end 
				else Year(min(Trns_Dt)) end as Frst_Bnft_Pd_Yr,

				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(min(Trns_Dt))=12 then 1 else Month(min(Trns_Dt)) end 
				else Month(min(Trns_Dt)) end as Frst_Bnft_Pd_Mnth,

                case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(min(Trns_Dt))=12 then 1 else day(min(Trns_Dt)) end 
				else day(min(Trns_Dt)) end as Frst_Bnft_Pd_Dy,


				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(max(Trns_Dt))=12 then Year(max(Trns_Dt)) else Year(max(Trns_Dt))end 
				else Year(max(Trns_Dt)) end as latst_Bnft_Pd_Yr,

				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(max(Trns_Dt))=12 then 1 else Month(max(Trns_Dt)) end 
				else Month(max(Trns_Dt)) end as latst_Bnft_Pd_Mnth,

                case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(max(Trns_Dt))=12 then day(max(Trns_Dt)) else day(max(Trns_Dt))end 
				else day(max(Trns_Dt)) end as latst_Bnft_Pd_Dy

		FROM INPUT.{pays}_CLMTRNS_ALL
		WHERE Amt>1 AND Trns_Type <> 'O'
		GROUP BY Clm_Nmbr
		""")
_dfs[f'{pays}_FIRSTLASTBENEFIT'].createOrReplaceTempView(f'{pays}_FIRSTLASTBENEFIT')

_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = spark.table(f'{pays}_FIRSTLASTBENEFIT')
_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}']
    .withColumn('trns_dt', F.expr("""make_date(latst_Bnft_Pd_Mnth, latst_Bnft_Pd_Dy, latst_Bnft_Pd_Yr)"""))
)
# FORMAT/INFORMAT: FORMAT trns_dt DDMMYY10.
_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'].select('country', 'clm_nmbr', 'trns_dt')
_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}')

# CLASSIFICATION DES CLAIMS PAR TYPE DE RESERVE:ICOP ET RBNP
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{pays}_V{yr_of_calculation}')
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = (_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}']
    .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
    .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
    .withColumn('Date_of_reserving', F.expr(f"""make_date({month_reserving}, {day_reserving}, {yr_reserving})"""))
    .withColumn('Totl_Bnfts_Amnt_Pd', F.when(F.expr("""Totl_Bnfts_Amnt_Pd IS NULL"""), F.lit(0)))
    .withColumn('Mnthly_Bnft', F.when(F.expr("""Mnthly_Bnft IS NULL"""), F.lit(0)))
    .withColumn('Otstndng_Balnc', F.when(F.expr("""Otstndng_Balnc IS NULL"""), F.lit(0)))
    .withColumn('Age_surv', F.expr("""floor(yrdif(Dt_of_Brth,Accdnt_Dt,ACTUAL))"""))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('CL','DC')"""), F.lit('CLOSE')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')     AND STATUS IN ('CL','DC')"""), F.lit('NON-CORE CLOSE')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date <=Date_of_reserving AND Totl_Bnfts_Amnt_Pd > 1"""), F.lit('ICOP')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date < Date_of_reserving AND Totl_Bnfts_Amnt_Pd < 1"""), F.lit('RBNP')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date > Date_of_reserving"""), F.lit('RBNP')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND Totl_Bnfts_Amnt_Pd = 0"""), F.lit('RBNP')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND cover IN ('CI','LIFE','PTD','GAP') AND Totl_Bnfts_Amnt_Pd > 0"""), F.lit('RBNP')))
    .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO')"""), F.lit('NON-CORE OPEN')))
)
# FORMAT/INFORMAT: FORMAT Date_of_reserving DDMMYY10.
# Cas des claims CL et DC
# Cas des claims OP et RO
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = _dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].drop('ACTUAL')
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')

# CALCUL DE L'AGE MOYEN PAR COUNTRY & COVER
_dfs[f'AGE_ACCDT_AVRG_{cover_typ}_{pays}'] = spark.sql(f"""SELECT h.Country, h.Cover as Cover, int(MEAN(h.Age_surv)) as AVG_AGE
		FROM Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation} h
		GROUP BY h.Country, h.Cover""")
_dfs[f'AGE_ACCDT_AVRG_{cover_typ}_{pays}'].createOrReplaceTempView(f'AGE_ACCDT_AVRG_{cover_typ}_{pays}')

_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*,t2.trns_dt
From Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation} t1 Left Join  CLMTRNS_samp_all_{pays}_V{yr_of_calculation} t2 on (t1.Clm_Nmbr = t2.Clm_Nmbr)""")
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')

_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*,t2.AVG_AGE
From Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation} t1 Left Join  AGE_ACCDT_AVRG_{cover_typ}_{pays} t2 on (t1.Cover = t2.Cover)""")
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')

_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}').orderBy('clm_nmbr')
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')

_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'] = (_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}']
    .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
    .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""((year(Date_of_reserving )-year(trns_dt))*12 + (month(Date_of_reserving)-month(trns_dt)))""")))
    .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
    .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
    .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
    .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""((year(Date_of_reserving )-year(trns_dt))*12 + (month(Date_of_reserving)-month(trns_dt)))""")))
    .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
    .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
    .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ  NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
    .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'CLOSE'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
    .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'CLOSE'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Nmbr_Bnfts_Pd IS NULL"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Nmbr_Bnfts_Pd2 IS NULL"""), F.lit(0)))
)
# Calcul des variables : Nmbr_Mnths_Pndng, Mnths_lag & Nmbr_Bnfts_Pd
# nb de mois écoulé entre la date d'enregistrement et la date de  calcul
# nb de mois écoulé entre la date de la dernière transaction et la date de  calcul
# nb de mois écoulé entre la date d'enregistrement et la date de  vision
_dfs[f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')

# CORRECTION DE L'AGE ET DU SEXE MANQUANT
_dfs[f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F'] = spark.table(f'Reserves_all_{cover_typ}_{pays}_V{yr_of_calculation}')
_dfs[f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F'] = (_dfs[f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F']
    .withColumn('Gndr', F.when(F.expr("""Gndr =''"""), F.lit('M')))
    .withColumn('Gndr', F.when(F.expr("""Gndr ='X'"""), F.lit('M')))
    .withColumn('Age_surv', F.when(F.expr("""Age_surv IN (.,0)"""), F.col('AVG_AGE')))
    .withColumn('Age_surv', F.when(F.expr("""Age_surv <= 0"""), F.col('AVG_AGE')))
    .withColumn('potential_clm_amt', F.when(F.expr("""potential_clm_amt=0"""), F.lit(0)))
    .withColumn('Otstndng_Balnc', F.when(F.expr("""Otstndng_Balnc=0"""), F.lit(0)))
    .withColumn('Mnthly_Bnft', F.when(F.expr("""Mnthly_Bnft=0"""), F.lit(0)))
    .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Nmbr_Mnths_Pndng > 12"""), F.lit(12)))
    .withColumn('Mnths_lag', F.when(F.expr("""Mnths_lag > 12"""), F.lit(12)))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Nmbr_Bnfts_Pd > 60"""), F.lit(60)))
    .withColumn('Age_surv', F.when(F.expr("""cover='IU' AND Age_surv > 80"""), F.lit(70)))
    .withColumn('Age_surv', F.when(F.expr("""cover IN ('DIS','LIFE','CI','PTD','GAP') AND Age_surv > 100"""), F.lit(100)))
)
_dfs[f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F'].createOrReplaceTempView(f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F')

# IMPORTATION DES TABLES DE DURATION ET D'ACCEPTATION
import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Reserving tables/{cover_typ}/Tables_{pays}_{cover_typ}.xlsx"
if {cover_typ}=IU or {cover_typ}=DIS:
    %IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_duration_&cover_typ._&pays.,ONGLET=Duration);
%IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_acceptation_&cover_typ._&pays.,ONGLET=Acceptation);

/*  MISE EN FORME DES FACTEURS DE LA TABLE DURATION ET DE LA TABLE ACCEPTATION */

data TABLE_DURATION_&cover_typ._&pays.;set TABLE_DURATION_&cover_typ._&pays.;run;

data facteur_age_D;keep Age_surv factor_age_D;set TABLE_DURATION_&cover_typ._&pays.;run;

data facteur_gender_D;keep Gndr factor_sexe_D Intercept_D;set TABLE_DURATION_&cover_typ._&pays.;run;

data facteur_Number_D;keep Nmbr_Bnfts_Pd factor_numbr_D;set TABLE_DURATION_&cover_typ._&pays.;run;

data facteur_Inactivity_D;keep Mnths_lag factor_lag_D ;set TABLE_DURATION_&cover_typ._&pays.;run;

data facteur_MAX_D;keep Max_Nmbr_Bnfts factor_max_D ;set TABLE_DURATION_&cover_typ._&pays.;run;

data TABLE_ACCEPTATION_&cover_typ._&pays.;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_age_A;keep Age_surv factor_age_A;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_gender_A;keep Gndr factor_sexe_A Intercept_A;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_Waiting_A;keep Nmbr_Mnths_Pndng factor_month_A ;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;
if {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP:
    %IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_acceptation_&cover_typ._&pays.,ONGLET=Acceptation);

/*  MISE EN FORME DES FACTEURS DE LA TABLE ACCEPTATION */

data TABLE_ACCEPTATION_&cover_typ._&pays.;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_age_A;keep Age_surv factor_age_A;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_gender_A;keep Gndr factor_sexe_A Intercept_A;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;

data facteur_Waiting_A;keep Nmbr_Mnths_Pndng factor_month_A ;set TABLE_ACCEPTATION_&cover_typ._&pays.;run;
# MERGE DES FACTEURS DE LA TABLE DURATION ET DE TABLE ACCEPTATION
if {cover_typ}=IU or {cover_typ}=DIS:
    Proc SQL; Create Table Reserves_&cover_typ._&pays._V&yr_of_calculation._F As
Select distinct t1.*,t2.factor_age_D,t3.factor_numbr_D,t4.factor_max_D,t5.factor_sexe_D,t5.Intercept_D,t6.factor_lag_D,t7.factor_age_A,t8.factor_sexe_A,t8.Intercept_A,t9.factor_month_A
From Reserves_&cover_typ._&pays._V&yr_of_calculation._F t1 Left Join  facteur_age_D t2    on (t1.Age_surv = t2.Age_surv)
                                                           Left Join  facteur_Number_D t3 on (t1.Nmbr_Bnfts_Pd = t3.Nmbr_Bnfts_Pd)
                                                           Left Join  facteur_Max_D t4    on (t1.Max_Nmbr_Bnfts = t4.Max_Nmbr_Bnfts)
                                                           Left Join  facteur_gender_D t5 on (t1.Gndr = t5.Gndr)
                                                           Left Join  facteur_Inactivity_D t6 on (t1.Mnths_lag = t6.Mnths_lag)
                                                           Left Join  FACTEUR_AGE_A t7 on (t1.Age_surv = t7.Age_surv)
                                                           Left Join  FACTEUR_GENDER_A t8 on (t1.Gndr = t8.Gndr)
                                                           Left Join  FACTEUR_WAITING_A t9 on (t1.Nmbr_Mnths_Pndng = t9.Nmbr_Mnths_Pndng)
           
;
Quit;
if {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP:
    Proc SQL; Create Table Reserves_&cover_typ._&pays._V&yr_of_calculation._F As
Select distinct t1.*,0 as factor_age_D,0 as factor_numbr_D,0 as factor_max_D,0 as factor_sexe_D,0 as Intercept_D,0 as factor_lag_D,t7.factor_age_A,t8.factor_sexe_A,t8.Intercept_A,t9.factor_month_A
From Reserves_&cover_typ._&pays._V&yr_of_calculation._F t1 Left Join  FACTEUR_AGE_A t7 on (t1.Age_surv = t7.Age_surv)
                                                           Left Join  FACTEUR_GENDER_A t8 on (t1.Gndr = t8.Gndr)
                                                           Left Join  FACTEUR_WAITING_A t9 on (t1.Nmbr_Mnths_Pndng = t9.Nmbr_Mnths_Pndng)
           
;
Quit;
# CALCUL DE LA PROBABILITE D'ACCEPTATION ET DU NOMBRE DE BENEFIT OUSTANDING
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_{cover_typ}_{pays}_V{yr_of_calculation}_F')
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}']
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""exp(Intercept_D + factor_age_D + factor_sexe_D + factor_max_D+ factor_numbr_D + factor_lag_D)""")))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_D + factor_age_D + factor_sexe_D + factor_max_D+ factor_numbr_D + factor_lag_D)""")))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_A + factor_age_A + factor_sexe_A + factor_month_A)/(1+exp(Intercept_A+factor_age_A +factor_sexe_A+ factor_month_A))""")))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ IN ('CLOSE','NON-CORE OPEN','NON-CORE CLOSE')"""), F.lit(0)))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ IN ('CLOSE','ICOP','NON-CORE OPEN','NON-CORE CLOSE')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'ICOP' AND cover IN ('IU','DIS')"""), F.expr("""Mnthly_Bnft*Nmbr_Bnfts_Otstndng""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('IU','DIS')"""), F.expr("""Probablty_Otstndng*Mnthly_Bnft*Nmbr_Bnfts_Otstndng""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Otstndng', F.lit(0))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_A + factor_age_A + factor_sexe_A + factor_month_A)/(1+exp(Intercept_A+factor_age_A +factor_sexe_A+ factor_month_A))""")))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ  NOT IN ('RBNP')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('CI','LIFE','PTD')"""), F.expr("""Probablty_Otstndng*Otstndng_Balnc""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('GAP')"""), F.expr("""Probablty_Otstndng*potential_clm_amt""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ  NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
)
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %IF {cover_typ}=IU or {cover_typ}=DIS %THEN %DO
    # ==============================================================
# CALCUL DES RESERVES
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %END
    # ==============================================================
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %IF {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP %THEN %DO
    # ==============================================================
# CALCUL DES RESERVES
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %END
    # ==============================================================
# AUTO CLOSE SUR LE CALCUL DES RESERVES
# /
# if Rsrv_Typ = "ICOP"  and Nmbr_Bnfts_Pd > MAX(Max_Nmbr_Bnfts,{guideline}) then Rsrv_Amt=0;
# if Rsrv_Typ = "ICOP"  and Mnths_lag2 > {guideline} then Rsrv_Amt=0;
# if Rsrv_Typ = "RBNP"  and Nmbr_Mnths_Pndng2 > {guideline} then Rsrv_Amt=0;
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'].withColumnRenamed('Nmbr_Mnths_Pndng2', 'Nmbr_Mnths_Pndng')
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'].withColumnRenamed('Nmbr_Bnfts_Pd2', 'Nmbr_Bnfts_Pd')
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}')

_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""SELECT distinct 
		 Date_of_reserving,		
		 country,
		 Rsrv_Grp,
		 Clm_Nmbr,
		 Policy_Line_No,
		 Policy_Line_Seq_No,
		 Cvr_Typ,
		 cover,
		 Schm,
		 Accdnt_Dt,
		 Acc_yr,
		 Acc_Mnth,
		 Rgstrtn_Dt,
		 Rgstrtn_Yr,
		 Rgstrtn_Mnth,
		 Cls_Dt,
		 STATUS,
		 potential_clm_amt,
		 Otstndng_Balnc,
		 Undrwrtng_Cmpny,
		 Max_Nmbr_Bnfts,
		 Expry_dt,
		 Totl_Amnt_Pd,
		 Totl_Bnfts_Amnt_Pd,
		 Frst_Bnft_Pd_Yr,
		 Frst_Bnft_Pd_Mnth,
		 Latst_Bnft_Pd_Yr,
		 Latst_Bnft_Pd_Mnth,
		 Incptn_Dt,
		 Insrnc_Trm,
		 Mnthly_Bnft,
		 Prdct,
		 Gndr,
		 Dt_of_Brth,
		 Nmbr_Mnths_Pndng,
		 Nmbr_Bnfts_Pd,
		 Nmbr_Bnfts_Otstndng,
		 Probablty_Otstndng,
		 Rsrv_Typ,
		 Rsrv_Amt,
		 informer_type,
		 Legal_Entity 
		FROM CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation} 
		GROUP BY Clm_Nmbr """)
_dfs[f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_{cover_typ}_{pays}_V{yr_of_calculation}')

mend()
# #################################################################  MACRO DE CONCATENATION DE TOUTES LES COUVERTURES #########################################################################################
def fusion(pays, yr_of_calculation):
    # %let yr_of_calculation=2021 ;
    # %let pays=SE ;
    # FILTRE SUR LES COUVERTURES NON-CORE HORS CASE RESERVES
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = spark.table(f'{pays}_CLMHDR_ALL')
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].filter(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""))
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}']
        .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO')"""), F.lit('NON-CORE OPEN')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('CL','DC')"""), F.lit('NON-CORE CLOSE')))
        .withColumn('Date_of_reserving', F.expr(f"""make_date({month_reserving}, {day_reserving}, {yr_reserving})"""))
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
    )
    # FORMAT/INFORMAT: FORMAT Date_of_reserving DDMMYY10.
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')

    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = spark.sql(f"""SELECT distinct 
    		 Date_of_reserving ,		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Policy_Line_No,
    		 Policy_Line_Seq_No,
    		 Cvr_Typ,
    		 cover,
    		 Schm,
    		 Accdnt_Dt,
    		 Acc_yr,
    		 Acc_Mnth,
    		 Rgstrtn_Dt,
    		 Rgstrtn_Yr,
    		 Rgstrtn_Mnth,
    		 Cls_Dt,
    		 STATUS,
    		 potential_clm_amt,
    		 Otstndng_Balnc,
    		 Undrwrtng_Cmpny,
    		 Max_Nmbr_Bnfts,
    		 Expry_dt,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Frst_Bnft_Pd_Yr,
    		 Frst_Bnft_Pd_Mnth,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 Incptn_Dt,
    		 Insrnc_Trm,
    		 Mnthly_Bnft,
    		 Prdct,
    		 Gndr,
    		 Dt_of_Brth,
    		 Nmbr_Mnths_Pndng,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Probablty_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type,
    		 Legal_Entity 
    		FROM CLMHDR_Hors_p_{pays}_V{yr_of_calculation}
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')

    # FUSION DES COUVERTURES: CREATION DE LA BASE CLMHDR
    from functools import reduce
    _dfs[f'CLMHDR_all_{pays}'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'CLMHDR_IU_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_DIS_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_LIFE_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_PTD_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_CI_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_GAP_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')])
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    _dfs[f'CLMHDR_all_{pays}'] = spark.table(f'CLMHDR_all_{pays}').orderBy('Clm_Nmbr', 'Policy_Line_No', 'Policy_Line_Seq_No', 'Schm', 'Cvr_Typ')
    _dfs[f'CLMHDR_all_{pays}'] = _dfs[f'CLMHDR_all_{pays}'].dropDuplicates(['Clm_Nmbr', 'Policy_Line_No', 'Policy_Line_Seq_No', 'Schm', 'Cvr_Typ'])
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    _dfs[f'CLMHDR_all_{pays}'] = spark.sql(f"""SELECT 
    		 Date_of_reserving,		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Policy_Line_No,
    		 Policy_Line_Seq_No,
    		 Cvr_Typ,
    		 cover,
    		 Schm,
    		 Accdnt_Dt,
    		 Acc_yr,
    		 Acc_Mnth,
    		 Rgstrtn_Dt,
    		 Rgstrtn_Yr,
    		 Rgstrtn_Mnth,
    		 Cls_Dt,
    		 STATUS,
    		 potential_clm_amt,
    		 Otstndng_Balnc,
    		 Undrwrtng_Cmpny,
    		 Max_Nmbr_Bnfts,
    		 Expry_dt,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Frst_Bnft_Pd_Yr,
    		 Frst_Bnft_Pd_Mnth,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 Incptn_Dt,
    		 Insrnc_Trm,
    		 Mnthly_Bnft,
    		 Prdct,
    		 Gndr,
    		 Dt_of_Brth,
    		 Nmbr_Mnths_Pndng,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Probablty_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type,
    		 Legal_Entity 
    		FROM CLMHDR_all_{pays} 
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    # CREATION DE LA BSE CLMHDR POUR LE DATA LAKE
    _dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""SELECT 		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Schm as SCHEME ,
    		 Cvr_Typ as cover,
    		 Undrwrtng_Cmpny as Entity_CD,
    		 Legal_Entity as Entity,
    		 Accdnt_Dt as Incident_date,
    		 Rgstrtn_Dt,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 year(Incptn_Dt) as Vintage_year,
    		 Date_of_reserving,
    		 STATUS,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Probablty_Otstndng as Probablty_Accptd,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type
    		 
    		FROM {ouput}.CLMHDR_all_{pays} 
    		
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Reassurance.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_02, out="Parametres_Reas", onglet="Parametres_Reas")
Parametres_Reas = spark.table('Parametres_Reas')
Parametres_Reas = (Parametres_Reas
    .withColumn('y_char', F.expr("""trim(cast(Original_underwritter as string))"""))
)
Parametres_Reas = Parametres_Reas.drop('Original_underwritter')
Parametres_Reas = Parametres_Reas.withColumnRenamed('y_char', 'Original_underwritter')
Parametres_Reas.createOrReplaceTempView('Parametres_Reas')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""select distinct
t1.*,
t8.QP_rei_CLAIM AS QP_rei_CLAIM 
from CLMHDR_all_{pays}_CR  t1 
left join Parametres_Reas t8 on (t1.country=t8.country AND t1.scheme=t8.scheme AND t1.cover=t8.cover AND t1.Entity_CD=t8.Original_underwritter) 
 """)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.table(f'CLMHDR_all_{pays}_CR')
_dfs[f'CLMHDR_all_{pays}_CR'] = (_dfs[f'CLMHDR_all_{pays}_CR']
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM IS NULL"""), F.lit(0)))
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM NOT IN (.)"""), F.lit(4)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Probablty_Accptd', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('QP_rei_CLAIM', F.when(F.expr("""Type_Insurance=0"""), F.lit(1)))
)
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Totl_Bnfts_Amnt_Pd', 'Totl_Bnfts_Amnt_Pd_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Totl_Amnt_Pd', 'Totl_Amnt_Pd_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.table(f'CLMHDR_all_{pays}_CR')
_dfs[f'CLMHDR_all_{pays}_CR'] = (_dfs[f'CLMHDR_all_{pays}_CR']
    .withColumn('Rsrv_Amt_Net', F.expr("""Rsrv_Amt_Gross*QP_rei_CLAIM"""))
    .withColumn('Totl_Amnt_Pd_Net', F.expr("""Totl_Amnt_Pd_Gross*QP_rei_CLAIM"""))
    .withColumn('Totl_Bnfts_Amnt_Pd_Net', F.expr("""Totl_Bnfts_Amnt_Pd_Gross*QP_rei_CLAIM"""))
)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""SELECT 		
		 country,
		 Rsrv_Grp,
		 Clm_Nmbr,
		 Type_Insurance,
		 SCHEME ,
		 cover,
		 Entity_CD,
		 Entity,
		 Incident_date,
		 Rgstrtn_Dt,
		 Latst_Bnft_Pd_Yr,
		 Latst_Bnft_Pd_Mnth,
		 Vintage_year,
		 Date_of_reserving,
		 STATUS,
		 Totl_Amnt_Pd_Gross,
		 Totl_Amnt_Pd_Net,
		 Totl_Bnfts_Amnt_Pd_Gross,
		 Totl_Bnfts_Amnt_Pd_Net,
		 Probablty_Accptd,
		 Nmbr_Bnfts_Pd,
		 Nmbr_Bnfts_Otstndng,
		 Rsrv_Typ,
		 Rsrv_Amt_Gross,
		 Rsrv_Amt_Net,
		 informer_type
		 		 
		FROM CLMHDR_all_{pays}_CR 
		
		GROUP BY Clm_Nmbr """)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

# proc datasets lib=work memtype=DATA;   delete CLMHDR_all_&pays._CR ;      run;
# proc datasets lib=work memtype=DATA;   delete CLMHDR_all_&pays.  ;      run;
# proc datasets lib=work memtype=DATA;   delete CLMHDR_&cover_typ._&pays._V&yr_of_calculation.;      run;
# proc datasets lib=work memtype=DATA;   delete &pays._FIRSTLASTBENEFIT ;      run;
# proc datasets lib=work memtype=DATA;   delete &pays._CLMHDR_ALL ;      run;
# proc datasets lib=work memtype=DATA;   delete Reserves_all_&pays._V&yr_of_calculation.  ;      run;
# proc datasets lib=work memtype=DATA;   delete Reserves_&cover_typ._&pays._V&yr_of_calculation._F;      run;
# proc datasets lib=work memtype=DATA;   delete &pays._FIRSTLASTBENEFIT ;      run;
mend()
# GERMANY
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="DE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="DE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="DE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="DE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="DE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="DE", guideline="12")
fusion(pays="DE", yr_of_calculation=yr_reserving)
# DENMARK
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="DK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="DK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="DK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="DK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="DK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="DK", guideline="12")
fusion(pays="DK", yr_of_calculation=yr_reserving)
# SWITZERLAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="CH", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="CH", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="CH", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="CH", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="CH", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="CH", guideline="12")
fusion(pays="CH", yr_of_calculation=yr_reserving)
# NORWAY
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="NO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="NO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="NO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="NO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="NO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="NO", guideline="12")
fusion(pays="NO", yr_of_calculation=yr_reserving)
# FINLAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="FI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="FI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="FI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="FI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="FI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="FI", guideline="12")
fusion(pays="FI", yr_of_calculation=yr_reserving)
# SWEDEN
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="SE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="SE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="SE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="SE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="SE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="SE", guideline="12")
fusion(pays="SE", yr_of_calculation=yr_reserving)
# SPAIN
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="ES", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="ES", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="ES", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="ES", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="ES", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="ES", guideline="12")
fusion(pays="ES", yr_of_calculation=yr_reserving)
# POLAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="PL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="PL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="PL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="PL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="PL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="PL", guideline="12")
fusion(pays="PL", yr_of_calculation=yr_reserving)
# UK
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="UK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="UK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="UK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="UK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="UK", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="UK", guideline="12")
fusion(pays="UK", yr_of_calculation=yr_reserving)
# ITALY
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="IT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="IT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="IT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="IT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="IT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="IT", guideline="12")
fusion(pays="IT", yr_of_calculation=yr_reserving)
# PORTUGAL
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="PT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="PT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="PT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="PT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="PT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="PT", guideline="12")
fusion(pays="PT", yr_of_calculation=yr_reserving)
# IRELAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="IE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="IE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="IE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="IE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="IE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="IE", guideline="12")
fusion(pays="IE", yr_of_calculation=yr_reserving)
# GREECE
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="GR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="GR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="GR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="GR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="GR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="GR", guideline="12")
fusion(pays="GR", yr_of_calculation=yr_reserving)
# Traitements sur la grèce suite à une demande de reduire les réserves à Zéro
CLMHDR_all_GR_CR = spark.table(f'{ouput}.CLMHDR_all_GR_CR')
CLMHDR_all_GR_CR = (CLMHDR_all_GR_CR
    .withColumn('Rsrv_Amt_Gross', F.when(F.expr("""Country = 'GR' AND SCHEME IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
    .withColumn('Rsrv_Amt_Net', F.when(F.expr("""Country = 'GR' AND SCHEME IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
)
CLMHDR_all_GR_CR.createOrReplaceTempView('CLMHDR_all_GR_CR')
# LIBNAME {ouput} -> base Spark: {ouput}.CLMHDR_all_GR_CR
CLMHDR_all_GR_CR.write.mode('overwrite').saveAsTable(f'{ouput}.CLMHDR_all_GR_CR')

CLMHDR_all_GR = spark.table(f'{ouput}.CLMHDR_all_GR')
CLMHDR_all_GR = (CLMHDR_all_GR
    .withColumn('Rsrv_Amt', F.when(F.expr("""Country = 'GR' AND Schm IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
)
CLMHDR_all_GR.createOrReplaceTempView('CLMHDR_all_GR')
# LIBNAME {ouput} -> base Spark: {ouput}.CLMHDR_all_GR
CLMHDR_all_GR.write.mode('overwrite').saveAsTable(f'{ouput}.CLMHDR_all_GR')

# /
# NETHERLAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="NL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="NL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="NL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="NL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="NL", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="NL", guideline="12")
fusion(pays="NL", yr_of_calculation=yr_reserving)
# NORTHEN IRELAND
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="NI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="NI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="NI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="NI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="NI", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="NI", guideline="12")
fusion(pays="NI", yr_of_calculation=yr_reserving)
# TURKEY
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="TR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="TR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="TR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="TR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="TR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="TR", guideline="12")
fusion(pays="TR", yr_of_calculation=yr_reserving)
# #################################################################################################################################################################################
# ######################################################  Macro CALCUL DES RESERVES PAR COUVERTURE : FRANCE    ########################################################################################
# ################################################################################################################################################################################
def acr_fr(yr_of_calculation, cover_typ, pays, prod, guideline, type_prod):
    # REGROUPEMENT DES COUVERTURES
    # %let pays=FR ;
    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Mapping Cover Initial.xlsx"
    import_excel(file=import_02, out="Mapping_cover_1", onglet="Mapping_cover")
    _dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{input}.{pays}_CLMHDR_all')
    _dfs[f'{pays}_CLMHDR_ALL'] = (_dfs[f'{pays}_CLMHDR_ALL']
        .withColumn('Prod_type', F.lit(None).cast(StringType()))  # LENGTH Prod_type $40
        .withColumn('Prod_type',
            F.when(F.expr("""Rsrv_Grp IN ('GD1','GL1','GR1')"""), F.lit('Mortgage'))
             .otherwise(F.lit('Non_Mortgage')))
    )
    _dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

    _dfs[f'{pays}_CLMHDR_ALL'] = spark.sql(f"""Select  t1.*,t2.Cover
    From {pays}_CLMHDR_ALL t1 Left Join  MAPPING_COVER_1 t2 on (t1.Cvr_Typ = t2.Cvr_Typ)  
    """)
    _dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

    _dfs[f'Reserves_all_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*
    From {pays}_CLMHDR_ALL t1 
    where  t1.cover='{cover_typ}' and t1.Prod_type='{prod}' and t1.Rsrv_Grp not in ('ZZ1','ZZ2')  
    """)
    _dfs[f'Reserves_all_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{pays}_V{yr_of_calculation}')

    # RECUPERATION DE LA DERNIERE DATE DE TRANSACTION POUR CHAQUE CLAIMS
    _dfs[f'{pays}_FIRSTLASTBENEFIT'] = spark.sql(f"""SELECT Clm_Nmbr,
    
    				
    				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(min(Trns_Dt))=12 then Year(min(Trns_Dt)) else Year(min(Trns_Dt)) end 
    				else Year(min(Trns_Dt)) end as Frst_Bnft_Pd_Yr,
    
    				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(min(Trns_Dt))=12 then 1 else Month(min(Trns_Dt)) end 
    				else Month(min(Trns_Dt)) end as Frst_Bnft_Pd_Mnth,
    
                    case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(min(Trns_Dt))=12 then 1 else day(min(Trns_Dt)) end 
    				else day(min(Trns_Dt)) end as Frst_Bnft_Pd_Dy,
    
    
    				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(max(Trns_Dt))=12 then Year(max(Trns_Dt)) else Year(max(Trns_Dt))end 
    				else Year(max(Trns_Dt)) end as latst_Bnft_Pd_Yr,
    
    				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(max(Trns_Dt))=12 then 1 else Month(max(Trns_Dt)) end 
    				else Month(max(Trns_Dt)) end as latst_Bnft_Pd_Mnth,
    
                    case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
    				when Month(max(Trns_Dt))=12 then day(max(Trns_Dt)) else day(max(Trns_Dt))end 
    				else day(max(Trns_Dt)) end as latst_Bnft_Pd_Dy
    
    		FROM INPUT.{pays}_CLMTRNS_ALL
    		WHERE Amt>1 AND Trns_Type <> 'O'
    		GROUP BY Clm_Nmbr
    		""")
    _dfs[f'{pays}_FIRSTLASTBENEFIT'].createOrReplaceTempView(f'{pays}_FIRSTLASTBENEFIT')

    _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = spark.table(f'{pays}_FIRSTLASTBENEFIT')
    _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}']
        .withColumn('trns_dt', F.expr("""make_date(latst_Bnft_Pd_Mnth, latst_Bnft_Pd_Dy, latst_Bnft_Pd_Yr)"""))
    )
    # FORMAT/INFORMAT: FORMAT trns_dt DDMMYY10.
    _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'].select('country', 'clm_nmbr', 'trns_dt')
    _dfs[f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMTRNS_samp_all_{pays}_V{yr_of_calculation}')

    # CLASSIFICATION DES CLAIMS PAR TYPE DE RESERVE:ICOP ET RBNP
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{pays}_V{yr_of_calculation}')
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = (_dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}']
        .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
        .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
        .withColumn('Date_of_reserving', F.expr(f"""make_date({month_reserving}, {day_reserving}, {yr_reserving})"""))
        .withColumn('Totl_Bnfts_Amnt_Pd', F.when(F.expr("""Totl_Bnfts_Amnt_Pd IS NULL"""), F.lit(0)))
        .withColumn('Mnthly_Bnft', F.when(F.expr("""Mnthly_Bnft IS NULL"""), F.lit(0)))
        .withColumn('Otstndng_Balnc', F.when(F.expr("""Otstndng_Balnc IS NULL"""), F.lit(0)))
        .withColumn('Age_surv', F.expr("""floor(yrdif(Dt_of_Brth,Accdnt_Dt,ACTUAL))"""))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('CL','DC')"""), F.lit('CLOSE')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')     AND STATUS IN ('CL','DC')"""), F.lit('NON-CORE CLOSE')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date <=Date_of_reserving AND Totl_Bnfts_Amnt_Pd > 1"""), F.lit('ICOP')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date < Date_of_reserving AND Totl_Bnfts_Amnt_Pd < 1"""), F.lit('RBNP')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND first_pd_date > Date_of_reserving"""), F.lit('RBNP')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND Totl_Bnfts_Amnt_Pd = 0"""), F.lit('RBNP')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp NOT IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO') AND cover IN ('CI','LIFE','PTD','GAP') AND Totl_Bnfts_Amnt_Pd > 0"""), F.lit('RBNP')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO')"""), F.lit('NON-CORE OPEN')))
    )
    # FORMAT/INFORMAT: FORMAT Date_of_reserving DDMMYY10.
    # Cas des claims CL et DC
    # Cas des claims OP et RO
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].drop('ACTUAL')
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

    # CALCUL DE L'AGE MOYEN PAR COUNTRY & COVER
    _dfs[f'AGE_ACCDT_AVRG_{cover_typ}_{type_prod}_{pays}'] = spark.sql(f"""SELECT h.Country, h.Cover as Cover, int(MEAN(h.Age_surv)) as AVG_AGE
    		FROM Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation} h
    		GROUP BY h.Country, h.Cover""")
    _dfs[f'AGE_ACCDT_AVRG_{cover_typ}_{type_prod}_{pays}'].createOrReplaceTempView(f'AGE_ACCDT_AVRG_{cover_typ}_{type_prod}_{pays}')

    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*,t2.trns_dt
    From Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation} t1 Left Join  CLMTRNS_samp_all_{pays}_V{yr_of_calculation} t2 on (t1.Clm_Nmbr = t2.Clm_Nmbr)""")
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""Select distinct t1.*,t2.AVG_AGE
    From Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation} t1 Left Join  AGE_ACCDT_AVRG_{cover_typ}_{type_prod}_{pays} t2 on (t1.Cover = t2.Cover)""")
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}').orderBy('clm_nmbr')
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = (_dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}']
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
        .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""((year(Date_of_reserving )-year(trns_dt))*12 + (month(Date_of_reserving)-month(trns_dt)))""")))
        .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
        .withColumn('Mnths_lag', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
        .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
        .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""((year(Date_of_reserving )-year(trns_dt))*12 + (month(Date_of_reserving)-month(trns_dt)))""")))
        .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""((year(Date_of_reserving )-year(Rgstrtn_Dt))*12 + (month(Date_of_reserving)-month(Rgstrtn_Dt)))+1""")))
        .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
        .withColumn('Nmbr_Mnths_Pndng2', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
        .withColumn('Mnths_lag2', F.when(F.expr("""Rsrv_Typ  NOT IN ('ICOP','RBNP')"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
        .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Typ = 'CLOSE'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
        .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Rsrv_Typ = 'CLOSE'"""), F.expr("""floor(Totl_Bnfts_Amnt_Pd/Mnthly_Bnft+0.5)""")))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Nmbr_Bnfts_Pd IS NULL"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd2', F.when(F.expr("""Nmbr_Bnfts_Pd2 IS NULL"""), F.lit(0)))
    )
    # Calcul des variables : Nmbr_Mnths_Pndng, Mnths_lag & Nmbr_Bnfts_Pd
    # nb de mois écoulé entre la date d'enregistrement et la date de  calcul
    # nb de mois écoulé entre la date de la dernière transaction et la date de  calcul
    # nb de mois écoulé entre la date d'enregistrement et la date de  vision
    _dfs[f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

    # CORRECTION DE L'AGE ET DU SEXE MANQUANT
    _dfs[f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F'] = spark.table(f'Reserves_all_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')
    _dfs[f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F'] = (_dfs[f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F']
        .withColumn('Gndr', F.when(F.expr("""Gndr =''"""), F.lit('M')))
        .withColumn('Gndr', F.when(F.expr("""Gndr ='X'"""), F.lit('M')))
        .withColumn('Age_surv', F.when(F.expr("""Age_surv IN (.,0)"""), F.col('AVG_AGE')))
        .withColumn('Age_surv', F.when(F.expr("""Age_surv <= 0"""), F.col('AVG_AGE')))
        .withColumn('potential_clm_amt', F.when(F.expr("""potential_clm_amt=0"""), F.lit(0)))
        .withColumn('Otstndng_Balnc', F.when(F.expr("""Otstndng_Balnc=0"""), F.lit(0)))
        .withColumn('Mnthly_Bnft', F.when(F.expr("""Mnthly_Bnft=0"""), F.lit(0)))
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Nmbr_Mnths_Pndng > 12"""), F.lit(12)))
        .withColumn('Mnths_lag', F.when(F.expr("""Mnths_lag > 12"""), F.lit(12)))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Nmbr_Bnfts_Pd > 60"""), F.lit(60)))
        .withColumn('Age_surv', F.when(F.expr("""cover='IU' AND Age_surv > 80"""), F.lit(70)))
        .withColumn('Age_surv', F.when(F.expr("""cover IN ('DIS','LIFE','CI','PTD','GAP') AND Age_surv > 100"""), F.lit(100)))
    )
    _dfs[f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F'].createOrReplaceTempView(f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F')

    # IMPORTATION DES TABLES DE DURATION ET D'ACCEPTATION
    import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Reserving tables/{cover_typ}/Tables_{pays}_{type_prod}_{cover_typ}.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


if {cover_typ}=IU or {cover_typ}=DIS:
    %IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_duration_&cover_typ._&type_prod._&pays.,ONGLET=Duration);
%IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_acceptation_&cover_typ._&type_prod._&pays.,ONGLET=Acceptation);

/*  MISE EN FORME DES FACTEURS DE LA TABLE DURATION ET DE LA TABLE ACCEPTATION */

data TABLE_DURATION_&cover_typ._&type_prod._&pays.;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data facteur_age_D;keep Age_surv factor_age_D;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data facteur_gender_D;keep Gndr factor_sexe_D Intercept_D;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data facteur_Number_D;keep Nmbr_Bnfts_Pd factor_numbr_D;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data facteur_Inactivity_D;keep Mnths_lag factor_lag_D ;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data facteur_MAX_D;keep Max_Nmbr_Bnfts factor_max_D ;set TABLE_DURATION_&cover_typ._&type_prod._&pays.;run;

data TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_age_A;keep Age_surv factor_age_A;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_gender_A;keep Gndr factor_sexe_A Intercept_A;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_Waiting_A;keep Nmbr_Mnths_Pndng factor_month_A ;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;
if {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP:
    %IMPORT_EXCEL(FILE=&Import_01.,OUT=Table_acceptation_&cover_typ._&type_prod._&pays.,ONGLET=Acceptation);

/*  MISE EN FORME DES FACTEURS DE LA TABLE ACCEPTATION */

data TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_age_A;keep Age_surv factor_age_A;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_gender_A;keep Gndr factor_sexe_A Intercept_A;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;

data facteur_Waiting_A;keep Nmbr_Mnths_Pndng factor_month_A ;set TABLE_ACCEPTATION_&cover_typ._&type_prod._&pays.;run;
# MERGE DES FACTEURS DE LA TABLE DURATION ET DE TABLE ACCEPTATION
if {cover_typ}=IU or {cover_typ}=DIS:
    Proc SQL; Create Table Reserves_&cover_typ._&type_prod._&pays._V&yr_of_calculation._F As
Select distinct t1.*,t2.factor_age_D,t3.factor_numbr_D,t4.factor_max_D,t5.factor_sexe_D,t5.Intercept_D,t6.factor_lag_D,t7.factor_age_A,t8.factor_sexe_A,t8.Intercept_A,t9.factor_month_A
From Reserves_&cover_typ._&type_prod._&pays._V&yr_of_calculation._F t1 Left Join  facteur_age_D t2    on (t1.Age_surv = t2.Age_surv)
                                                           Left Join  facteur_Number_D t3 on (t1.Nmbr_Bnfts_Pd = t3.Nmbr_Bnfts_Pd)
                                                           Left Join  facteur_Max_D t4    on (t1.Max_Nmbr_Bnfts = t4.Max_Nmbr_Bnfts)
                                                           Left Join  facteur_gender_D t5 on (t1.Gndr = t5.Gndr)
                                                           Left Join  facteur_Inactivity_D t6 on (t1.Mnths_lag = t6.Mnths_lag)
                                                           Left Join  FACTEUR_AGE_A t7 on (t1.Age_surv = t7.Age_surv)
                                                           Left Join  FACTEUR_GENDER_A t8 on (t1.Gndr = t8.Gndr)
                                                           Left Join  FACTEUR_WAITING_A t9 on (t1.Nmbr_Mnths_Pndng = t9.Nmbr_Mnths_Pndng)
           
;
Quit;
if {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP:
    Proc SQL; Create Table Reserves_&cover_typ._&type_prod._&pays._V&yr_of_calculation._F As
Select distinct t1.*,0 as factor_age_D,0 as factor_numbr_D,0 as factor_max_D,0 as factor_sexe_D,0 as Intercept_D,0 as factor_lag_D,t7.factor_age_A,t8.factor_sexe_A,t8.Intercept_A,t9.factor_month_A
From Reserves_&cover_typ._&type_prod._&pays._V&yr_of_calculation._F t1 Left Join  FACTEUR_AGE_A t7 on (t1.Age_surv = t7.Age_surv)
                                                           Left Join  FACTEUR_GENDER_A t8 on (t1.Gndr = t8.Gndr)
                                                           Left Join  FACTEUR_WAITING_A t9 on (t1.Nmbr_Mnths_Pndng = t9.Nmbr_Mnths_Pndng)
           
;
Quit;
# CALCUL DE LA PROBABILITE D'ACCEPTATION ET DU NOMBRE DE BENEFIT OUSTANDING
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.table(f'Reserves_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}_F')
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}']
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.expr("""exp(Intercept_D + factor_age_D + factor_sexe_D + factor_max_D+ factor_numbr_D + factor_lag_D)""")))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_D + factor_age_D + factor_sexe_D + factor_max_D+ factor_numbr_D + factor_lag_D)""")))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_A + factor_age_A + factor_sexe_A + factor_month_A)/(1+exp(Intercept_A+factor_age_A +factor_sexe_A+ factor_month_A))""")))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Typ IN ('CLOSE','NON-CORE OPEN','NON-CORE CLOSE')"""), F.lit(0)))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ IN ('CLOSE','ICOP','NON-CORE OPEN','NON-CORE CLOSE')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'ICOP' AND cover IN ('IU','DIS')"""), F.expr("""Mnthly_Bnft*Nmbr_Bnfts_Otstndng""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('IU','DIS')"""), F.expr("""Probablty_Otstndng*Mnthly_Bnft*Nmbr_Bnfts_Otstndng""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Otstndng', F.lit(0))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'RBNP'"""), F.expr("""exp(Intercept_A + factor_age_A + factor_sexe_A + factor_month_A)/(1+exp(Intercept_A+factor_age_A +factor_sexe_A+ factor_month_A))""")))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ = 'ICOP'"""), F.lit(0)))
    .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Typ  NOT IN ('RBNP')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('CI','LIFE','PTD')"""), F.expr("""Probablty_Otstndng*Otstndng_Balnc""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ = 'RBNP' AND cover IN ('GAP')"""), F.expr("""Probablty_Otstndng*potential_clm_amt""")))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Typ  NOT IN ('ICOP','RBNP')"""), F.lit(0)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
)
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %IF {cover_typ}=IU or {cover_typ}=DIS %THEN %DO
    # ==============================================================
# CALCUL DES RESERVES
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %END
    # ==============================================================
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %IF {cover_typ}=LIFE or {cover_typ}=CI or {cover_typ}=PTD or {cover_typ}=GAP %THEN %DO
    # ==============================================================
# CALCUL DES RESERVES
    # ===== MANUAL REVIEW REQUIRED: macro code inside DATA step =====
    # The following SAS uses a macro %do/%let loop to generate
    # indexed columns at compile time. Translate by hand using a
    # Python for-loop with df.withColumn(f'col_{i}', ...).
    # SAS: %END
    # ==============================================================
# AUTO CLOSE SUR LE CALCUL DES RESERVES
# if Rsrv_Typ = "ICOP"  and Nmbr_Bnfts_Pd > MAX(Max_Nmbr_Bnfts,{guideline}) then Rsrv_Amt=0;
# if Rsrv_Typ = "ICOP"  and Mnths_lag2 > {guideline} then Rsrv_Amt=0;
# if Rsrv_Typ = "RBNP"  and Nmbr_Mnths_Pndng2 > {guideline} then Rsrv_Amt=0;
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].withColumnRenamed('Nmbr_Mnths_Pndng2', 'Nmbr_Mnths_Pndng')
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].withColumnRenamed('Nmbr_Bnfts_Pd2', 'Nmbr_Bnfts_Pd')
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'] = spark.sql(f"""SELECT 
		 Date_of_reserving,		
		 country,
		 Rsrv_Grp,
		 Clm_Nmbr,
		 Policy_Line_No,
		 Policy_Line_Seq_No,
		 Cvr_Typ,
		 cover,
		 Schm,
		 Accdnt_Dt,
		 Acc_yr,
		 Acc_Mnth,
		 Rgstrtn_Dt,
		 Rgstrtn_Yr,
		 Rgstrtn_Mnth,
		 Cls_Dt,
		 STATUS,
		 potential_clm_amt,
		 Otstndng_Balnc,
		 Undrwrtng_Cmpny,
		 Max_Nmbr_Bnfts,
		 Expry_dt,
		 Totl_Amnt_Pd,
		 Totl_Bnfts_Amnt_Pd,
		 Frst_Bnft_Pd_Yr,
		 Frst_Bnft_Pd_Mnth,
		 Latst_Bnft_Pd_Yr,
		 Latst_Bnft_Pd_Mnth,
		 Incptn_Dt,
		 Insrnc_Trm,
		 Mnthly_Bnft,
		 Prdct,
		 Gndr,
		 Dt_of_Brth,
		 Nmbr_Mnths_Pndng,
		 Nmbr_Bnfts_Pd,
		 Nmbr_Bnfts_Otstndng,
		 Probablty_Otstndng,
		 Rsrv_Typ,
		 Rsrv_Amt,
		 informer_type,
		 Legal_Entity 
		FROM CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation} 
		GROUP BY Clm_Nmbr """)
_dfs[f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_{cover_typ}_{type_prod}_{pays}_V{yr_of_calculation}')

mend()
# #################################################################  MACRO DE CONCATENATION DE TOUTES LES COUVERTURES #########################################################################################
def fusion_fr(pays, yr_of_calculation):
    # FILTRE SUR LES COUVERTURES NON-CORE HORS CASE RESERVES
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = spark.table(f'{pays}_CLMHDR_ALL')
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].filter(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""))
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = (_dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}']
        .withColumn('Rsrv_Typ', F.lit(None).cast(StringType()))  # LENGTH Rsrv_Typ $40
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('OP','RO')"""), F.lit('NON-CORE OPEN')))
        .withColumn('Rsrv_Typ', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2') AND STATUS IN ('CL','DC')"""), F.lit('NON-CORE CLOSE')))
        .withColumn('Date_of_reserving', F.expr(f"""make_date({month_reserving}, {day_reserving}, {yr_reserving})"""))
        .withColumn('Nmbr_Mnths_Pndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Probablty_Otstndng', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
        .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp  IN ('ZZ1','ZZ2')"""), F.lit(0)))
    )
    # FORMAT/INFORMAT: FORMAT Date_of_reserving DDMMYY10.
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')

    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'] = spark.sql(f"""SELECT distinct 
    		 Date_of_reserving ,		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Policy_Line_No,
    		 Policy_Line_Seq_No,
    		 Cvr_Typ,
    		 cover,
    		 Schm,
    		 Accdnt_Dt,
    		 Acc_yr,
    		 Acc_Mnth,
    		 Rgstrtn_Dt,
    		 Rgstrtn_Yr,
    		 Rgstrtn_Mnth,
    		 Cls_Dt,
    		 STATUS,
    		 potential_clm_amt,
    		 Otstndng_Balnc,
    		 Undrwrtng_Cmpny,
    		 Max_Nmbr_Bnfts,
    		 Expry_dt,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Frst_Bnft_Pd_Yr,
    		 Frst_Bnft_Pd_Mnth,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 Incptn_Dt,
    		 Insrnc_Trm,
    		 Mnthly_Bnft,
    		 Prdct,
    		 Gndr,
    		 Dt_of_Brth,
    		 Nmbr_Mnths_Pndng,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Probablty_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type,
    		 Legal_Entity 
    		FROM CLMHDR_Hors_p_{pays}_V{yr_of_calculation}
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}'].createOrReplaceTempView(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')

    # FUSION DES COUVERTURES: CREATION DE LA BASE CLMHDR
    from functools import reduce
    _dfs[f'CLMHDR_all_{pays}'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'CLMHDR_IU_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_DIS_M_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_DIS_NM_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_LIFE_M_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_LIFE_NM_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_PTD_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_CI_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_GAP_{pays}_V{yr_of_calculation}'), spark.table(f'CLMHDR_Hors_p_{pays}_V{yr_of_calculation}')])
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    _dfs[f'CLMHDR_all_{pays}'] = spark.table(f'CLMHDR_all_{pays}').orderBy('Clm_Nmbr')
    _dfs[f'CLMHDR_all_{pays}'] = _dfs[f'CLMHDR_all_{pays}'].dropDuplicates(['Clm_Nmbr'])
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    _dfs[f'CLMHDR_all_{pays}'] = spark.sql(f"""SELECT 
    		 Date_of_reserving,		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Policy_Line_No,
    		 Policy_Line_Seq_No,
    		 Cvr_Typ,
    		 cover,
    		 Schm,
    		 Accdnt_Dt,
    		 Acc_yr,
    		 Acc_Mnth,
    		 Rgstrtn_Dt,
    		 Rgstrtn_Yr,
    		 Rgstrtn_Mnth,
    		 Cls_Dt,
    		 STATUS,
    		 potential_clm_amt,
    		 Otstndng_Balnc,
    		 Undrwrtng_Cmpny,
    		 Max_Nmbr_Bnfts,
    		 Expry_dt,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Frst_Bnft_Pd_Yr,
    		 Frst_Bnft_Pd_Mnth,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 Incptn_Dt,
    		 Insrnc_Trm,
    		 Mnthly_Bnft,
    		 Prdct,
    		 Gndr,
    		 Dt_of_Brth,
    		 Nmbr_Mnths_Pndng,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Probablty_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type,
    		 Legal_Entity 
    		FROM CLMHDR_all_{pays} 
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_all_{pays}'].createOrReplaceTempView(f'CLMHDR_all_{pays}')

    # CREATION DE LA BSE CLMHDR POUR LE DATA LAKE
    _dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""SELECT 		
    		 country,
    		 Rsrv_Grp,
    		 Clm_Nmbr,
    		 Schm as SCHEME ,
    		 Cvr_Typ as cover,
    		 Undrwrtng_Cmpny as Entity_CD,
    		 Legal_Entity as Entity,
    		 Accdnt_Dt as Incident_date,
    		 Rgstrtn_Dt,
    		 Latst_Bnft_Pd_Yr,
    		 Latst_Bnft_Pd_Mnth,
    		 year(Incptn_Dt) as Vintage_year,
    		 Date_of_reserving,
    		 STATUS,
    		 Totl_Amnt_Pd,
    		 Totl_Bnfts_Amnt_Pd,
    		 Probablty_Otstndng as Probablty_Accptd,
    		 Nmbr_Bnfts_Pd,
    		 Nmbr_Bnfts_Otstndng,
    		 Rsrv_Typ,
    		 Rsrv_Amt,
    		 informer_type
    		 
    		FROM {ouput}.CLMHDR_all_{pays} 
    		
    		GROUP BY Clm_Nmbr """)
    _dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

    import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Reassurance.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_02, out="Parametres_Reas", onglet="Parametres_Reas")
Parametres_Reas = spark.table('Parametres_Reas')
Parametres_Reas = (Parametres_Reas
    .withColumn('y_char', F.expr("""trim(cast(Original_underwritter as string))"""))
)
Parametres_Reas = Parametres_Reas.drop('Original_underwritter')
Parametres_Reas = Parametres_Reas.withColumnRenamed('y_char', 'Original_underwritter')
Parametres_Reas.createOrReplaceTempView('Parametres_Reas')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""select distinct
t1.*,
t8.QP_rei_CLAIM AS QP_rei_CLAIM 
from CLMHDR_all_{pays}_CR  t1 
left join Parametres_Reas t8 on (t1.country=t8.country AND t1.scheme=t8.scheme AND t1.cover=t8.cover AND t1.Entity_CD=t8.Original_underwritter) 
 """)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.table(f'CLMHDR_all_{pays}_CR')
_dfs[f'CLMHDR_all_{pays}_CR'] = (_dfs[f'CLMHDR_all_{pays}_CR']
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM IS NULL"""), F.lit(0)))
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM NOT IN (.)"""), F.lit(4)))
    .withColumn('Rsrv_Amt', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Otstndng', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Probablty_Accptd', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('Nmbr_Bnfts_Pd', F.when(F.expr("""Rsrv_Grp IN ('ZZ1','ZZ2')"""), F.lit(0)))
    .withColumn('QP_rei_CLAIM', F.when(F.expr("""Type_Insurance=0"""), F.lit(1)))
)
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Totl_Bnfts_Amnt_Pd', 'Totl_Bnfts_Amnt_Pd_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'] = _dfs[f'CLMHDR_all_{pays}_CR'].withColumnRenamed('Totl_Amnt_Pd', 'Totl_Amnt_Pd_Gross')
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.table(f'CLMHDR_all_{pays}_CR')
_dfs[f'CLMHDR_all_{pays}_CR'] = (_dfs[f'CLMHDR_all_{pays}_CR']
    .withColumn('Rsrv_Amt_Net', F.expr("""Rsrv_Amt_Gross*QP_rei_CLAIM"""))
    .withColumn('Totl_Amnt_Pd_Net', F.expr("""Totl_Amnt_Pd_Gross*QP_rei_CLAIM"""))
    .withColumn('Totl_Bnfts_Amnt_Pd_Net', F.expr("""Totl_Bnfts_Amnt_Pd_Gross*QP_rei_CLAIM"""))
)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

_dfs[f'CLMHDR_all_{pays}_CR'] = spark.sql(f"""SELECT 		
		 country,
		 Rsrv_Grp,
		 Clm_Nmbr,
		 Type_Insurance,
		 SCHEME ,
		 cover,
		 Entity_CD,
		 Entity,
		 Incident_date,
		 Rgstrtn_Dt,
		 Latst_Bnft_Pd_Yr,
		 Latst_Bnft_Pd_Mnth,
		 Vintage_year,
		 Date_of_reserving,
		 STATUS,
		 Totl_Amnt_Pd_Gross,
		 Totl_Amnt_Pd_Net,
		 Totl_Bnfts_Amnt_Pd_Gross,
		 Totl_Bnfts_Amnt_Pd_Net,
		 Probablty_Accptd,
		 Nmbr_Bnfts_Pd,
		 Nmbr_Bnfts_Otstndng,
		 Rsrv_Typ,
		 Rsrv_Amt_Gross,
		 Rsrv_Amt_Net,
		 informer_type
		 		 
		FROM CLMHDR_all_{pays}_CR 
		
		GROUP BY Clm_Nmbr """)
_dfs[f'CLMHDR_all_{pays}_CR'].createOrReplaceTempView(f'CLMHDR_all_{pays}_CR')

mend()
# #########################################################################################################################################################################################
# ######################################################   CALCUL DES RESERVES PAR PAYS ET PAR COUVERTURE : TOUT LES PAYS     #############################################################
# ########################################################################################################################################################################################
# FRANCE
acr_fr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="FR", prod="Mortgage", guideline="12", type_prod="M")
acr_fr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="FR", prod="Non_Mortgage", guideline="12", type_prod="NM")
acr_fr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="FR", prod="Mortgage", guideline="12", type_prod="M")
acr_fr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="FR", prod="Non_Mortgage", guideline="12", type_prod="NM")
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="FR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="FR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="FR", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="FR", guideline="12")
fusion_fr(pays="FR", yr_of_calculation=yr_reserving)
# #########################################################################################################################################################################################
# ######################################################   CALCUL DES RESERVES PAR PAYS ET PAR COUVERTURE : OST     #############################################################
# ########################################################################################################################################################################################
# COLOMBIA
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="CO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="CO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="CO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="CO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="CO", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="CO", guideline="12")
fusion(pays="CO", yr_of_calculation=yr_reserving)
# MEXICO
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="MX", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="MX", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="MX", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="MX", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="MX", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="MX", guideline="12")
fusion(pays="MX", yr_of_calculation=yr_reserving)
# AUSTRIA
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="AT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="AT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="AT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="AT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="AT", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="AT", guideline="12")
fusion(pays="AT", yr_of_calculation=yr_reserving)
# BELGIUM
acr(yr_of_calculation=yr_reserving, cover_typ="IU", pays="BE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="DIS", pays="BE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="LIFE", pays="BE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="PTD", pays="BE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="CI", pays="BE", guideline="12")
acr(yr_of_calculation=yr_reserving, cover_typ="GAP", pays="BE", guideline="12")
fusion(pays="BE", yr_of_calculation=yr_reserving)