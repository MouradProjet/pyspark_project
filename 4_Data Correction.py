from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #################################################################################################################################################################################
# ##########################################################       EXTRACTION CLAIMS DU DATA LAKE   ########################################################################################
# ################################################################################################################################################################################
# ######## Name: EXTRACTION CLAIMS DU DATA LAKE
# ######## Author: ALSENY SOW
# ######## Date started :26/06/2018
# ######## Date finished:
# ######## Context: MISE EN FORE & CORRECTION DES CLAIMS QUI PERMETTENT DE CALCULER LES CASES RESERVES (ICOP & RBNP)
# #####################################################  CREATION DES LIBRARY  #########################################################################################
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
balancedate = "26/06/2026"
data_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/Claims Extracts"  # LIBNAME data
input_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Input"  # LIBNAME input
spark.sql('CREATE SCHEMA IF NOT EXISTS input')  # base Spark pour LIBNAME input
# /
# Macro Corection data - Claims Extract
# /
def data_corecction(pays):
    # %LET pays =PT ;
    import_03 = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Entity_Mappings.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_03, out="Entity_Mappings", onglet="Entity_Mappings")
_dfs[f'{pays}_CLMHDR_0'] = spark.sql(f"""SELECT  

                Country,
				CLA_CASE_NO as Clm_Nmbr,
				POLICY_LINE_NO as Policy_Line_No,
				POLICY_LINE_SEQ_NO as Policy_Line_Seq_No,
				cover as Cvr_Typ,
				SCHEME as Schm,
				INCIDENT_DATE as Accdnt_Dt,
				Year(INCIDENT_DATE) length = 3 as Acc_Yr,
				month(INCIDENT_DATE) length = 3 as Acc_Mnth,
				FIRST_OPEN_DATE as Rgstrtn_Dt,
				Year(FIRST_OPEN_DATE) length = 3 as Rgstrtn_Yr,
				month(FIRST_OPEN_DATE) length = 3 as Rgstrtn_Mnth,
				FIRST_CLOSE_DATE, 
				RECLOSE_DATE,
				STATUS,
				OUTSTANDING_LIFE_BALANCE ,
				OUTSTANDING_NONLIFE_BALANCE,
				POTENTIAL_CLM_AMT,
				uw_company as Undrwrtng_Cmpny,
				MAX_NO_OF_PAYMENTS as Max_Nmbr_Bnfts,
				case when INSURANCE_TERM = 1 then 
				cover_END_DATE + 20*365
				else cover_END_DATE end format = ddmmyy10. length = 4 as Expry_dt,
				cover_START_DATE as Incptn_Dt,
				INSURANCE_TERM as Insrnc_Trm,
				CLAIM_MONTHLY_BENEFIT,
				POLICY_MONTHLY_BENEFIT,
				IS_BULK,
				PROD_ID as Prdct,
				GENDER as Gndr,
				BIRTH_DATE as Dt_of_Brth,
				cause_code,
				informer_type,
				Legal_Entity
				
		FROM Data.{pays}_CLMHDR
		


		WHERE STATUS not in ('EC') AND  FIRST_OPEN_DATE <= input({balancedate},ddmmyy10.) AND FIRST_OPEN_DATE <> . and INCIDENT_DATE <> . """)
_dfs[f'{pays}_CLMHDR_0'].createOrReplaceTempView(f'{pays}_CLMHDR_0')

# Correct Insurance Term where Null or <1.

# KO : ligne 93
# PROC SQL;
		UPDATE &pays._CLMHDR_0 h
		SET Insrnc_Trm = 1
		WHERE Insrnc_Trm < 1 or Insrnc_Trm = .
		and IS_BULK = "Y"
		;
QUIT;
# fin KO

# #####################################################  CREATION DE LA BASE CLMTRANS FINALE  #########################################################################################
_dfs[f'{pays}_CLMTRNS_all'] = spark.sql(f"""SELECT t.country, t.CLA_CASE_NO as Clm_Nmbr,
				t.TRANS_DATE as Trns_Dt,
				case when day(TRANS_DATE)>Day(input({balancedate},ddmmyy10.)) then case when Month(TRANS_DATE)=12 
				then Year(TRANS_DATE)+1 else Year(TRANS_DATE) end else Year(TRANS_DATE) end length = 3 as Acnt_Yr,
				case when day(TRANS_DATE)>Day(input({balancedate},ddmmyy10.)) then case when Month(TRANS_DATE)=12 
				then 1 else Month(TRANS_DATE)+1 end else Month(TRANS_DATE) end length = 3 as Acnt_Mnth,
				-t.GROSS_AMT as Amt, 
				case when t.ITEM_CLASS = 2 then 'C' else m.TRANS_TYPE end as Trns_Type
		FROM DATA.{pays}_CLMTRNS t
		lEFT JOIN {pays}_TRANS_TYPE_MAP m ON (t.SPECIFICATION = m.SPECIFICATION)
		INNER JOIN {pays}_CLMHDR_0 h ON (t.CLA_CASE_NO = h.Clm_Nmbr)
		WHERE h.Clm_Nmbr <> .
		AND t.TRANS_DATE <= input({balancedate},ddmmyy10.)
		ORDER BY CLA_CASE_NO""")
_dfs[f'{pays}_CLMTRNS_all'].createOrReplaceTempView(f'{pays}_CLMTRNS_all')

# ###########################################################    CREATION DE LA BASE CLMHDR    #########################################################################################
_dfs[f'{pays}_CLMHDR_0'] = spark.table(f'{pays}_CLMHDR_0')
_dfs[f'{pays}_CLMHDR_0'] = (_dfs[f'{pays}_CLMHDR_0']
    .withColumn('Mnthly_Bnft',
        F.when(F.expr("""CLAIM_MONTHLY_BENEFIT != 0 AND CLAIM_MONTHLY_BENEFIT IS NOT NULL"""), F.col('CLAIM_MONTHLY_BENEFIT'))
         .when(F.expr("""POLICY_MONTHLY_BENEFIT != 0 AND POLICY_MONTHLY_BENEFIT IS NOT NULL"""), F.col('POLICY_MONTHLY_BENEFIT')))
    .withColumn('Cls_Dt',
        F.when(F.expr("""RECLOSE_DATE IS NOT NULL"""), F.col('RECLOSE_DATE'))
         .when(F.expr("""FIRST_CLOSE_DATE IS NOT NULL"""), F.col('FIRST_CLOSE_DATE'))
         .otherwise(F.col('NULL')))
)
# FORMAT/INFORMAT: FORMAT Cls_Dt ddmmyy10.
_dfs[f'{pays}_CLMHDR_0'] = _dfs[f'{pays}_CLMHDR_0'].drop('CLAIM_MONTHLY_BENEFIT', 'POLICY_MONTHLY_BENEFIT', 'RECLOSE_DATE', 'FIRST_CLOSE_DATE')
_dfs[f'{pays}_CLMHDR_0'].createOrReplaceTempView(f'{pays}_CLMHDR_0')

_dfs[f'{pays}_CLMHDR_1'] = spark.table(f'{pays}_CLMHDR_0').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_1'].createOrReplaceTempView(f'{pays}_CLMHDR_1')

_dfs[f'{pays}_TOTAL_AMOUNT_PAID'] = spark.sql(f"""SELECT Clm_Nmbr, sum(Amt) as Totl_Amnt_Pd, sum(case when Trns_Type = 'O' then 0 
		else Amt end) as Totl_Bnfts_Amnt_Pd
		FROM INPUT.{pays}_CLMTRNS_all
		GROUP BY Clm_Nmbr""")
_dfs[f'{pays}_TOTAL_AMOUNT_PAID'].createOrReplaceTempView(f'{pays}_TOTAL_AMOUNT_PAID')

_dfs[f'{pays}_TOTAL_AMOUNT_PAID'] = spark.table(f'{pays}_TOTAL_AMOUNT_PAID').orderBy('Clm_Nmbr')
_dfs[f'{pays}_TOTAL_AMOUNT_PAID'].createOrReplaceTempView(f'{pays}_TOTAL_AMOUNT_PAID')

_dfs[f'{pays}_CLMHDR_1'] = spark.table(f'{pays}_CLMHDR_1').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_1'].createOrReplaceTempView(f'{pays}_CLMHDR_1')

_dfs[f'{pays}_CLMHDR_2'] = spark.createDataFrame([], schema=StructType([]))
_dfs[f'{pays}_CLMHDR_2'] = (_dfs[f'{pays}_CLMHDR_2']
    .withColumn('Totl_Amnt_Pd', F.when(F.expr("""Totl_Amnt_Pd IS NULL"""), F.lit(0)))
    .withColumn('Totl_Bnfts_Amnt_Pd', F.when(F.expr("""Totl_Bnfts_Amnt_Pd IS NULL"""), F.lit(0)))
)
_dfs[f'{pays}_CLMHDR_2'].createOrReplaceTempView(f'{pays}_CLMHDR_2')

_dfs[f'{pays}_FIRSTLASTBENEFIT'] = spark.sql(f"""SELECT Clm_Nmbr,

				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(min(Trns_Dt))=12 then Year(min(Trns_Dt))+1 else Year(min(Trns_Dt)) end 
				else Year(min(Trns_Dt)) end as Frst_Bnft_Pd_Yr,

				case when day(min(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(min(Trns_Dt))=12 then 1 else Month(min(Trns_Dt))+1 end 
				else Month(min(Trns_Dt)) end as Frst_Bnft_Pd_Mnth,

				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(max(Trns_Dt))=12 then Year(max(Trns_Dt))+1 else Year(max(Trns_Dt))end 
				else Year(max(Trns_Dt)) end as latst_Bnft_Pd_Yr,

				case when day(max(Trns_Dt))>Day(input({balancedate},ddmmyy10.)) then case 
				when Month(max(Trns_Dt))=12 then 1 else Month(max(Trns_Dt))+1 end 
				else Month(max(Trns_Dt)) end as latst_Bnft_Pd_Mnth

		FROM INPUT.{pays}_CLMTRNS_all
		WHERE Amt>1 AND Trns_Type <> 'O'
		GROUP BY Clm_Nmbr
		""")
_dfs[f'{pays}_FIRSTLASTBENEFIT'].createOrReplaceTempView(f'{pays}_FIRSTLASTBENEFIT')

_dfs[f'{pays}_FIRSTLASTBENEFIT'] = spark.table(f'{pays}_FIRSTLASTBENEFIT').orderBy('Clm_Nmbr')
_dfs[f'{pays}_FIRSTLASTBENEFIT'].createOrReplaceTempView(f'{pays}_FIRSTLASTBENEFIT')

_dfs[f'{pays}_CLMHDR_2'] = spark.table(f'{pays}_CLMHDR_2').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_2'].createOrReplaceTempView(f'{pays}_CLMHDR_2')

# RETAIN variables (initial values): {'Country': '0', 'Clm_Nmbr': '0', 'Policy_Line_No': '0', 'Policy_Line_Seq_No': '0', 'Cvr_Typ': '0', 'Schm': '0', 'Accdnt_Dt': '0', 'Acc_Yr': '0', 'Acc_Mnth': '0', 'Rgstrtn_Dt': '0', 'Rgstrtn_Yr': '0', 'Rgstrtn_Mnth': '0', 'Cls_Dt': '0', 'STATUS': '0', 'OUTSTANDING_LIFE_BALANCE': '0', 'POTENTIAL_CLM_AMT': '0', 'Totl_Amnt_Rpybl': '0', 'Undrwrtng_Cmpny': '0', 'Max_Nmbr_Bnfts': '0', 'Expry_dt': '0', 'Totl_Amnt_Pd': '0', 'Totl_Bnfts_Amnt_Pd': '0', 'Frst_Bnft_Pd_Yr': '0', 'Frst_Bnft_Pd_Mnth': '0', 'Latst_Bnft_Pd_Yr': '0', 'Latst_Bnft_Pd_Mnth': '0', 'Incptn_Dt': '0', 'Insrnc_Trm': '0', 'Mnthly_Bnft': '0', 'Prdct': '0', 'Gndr': '0', 'Dt_of_Brth': '0', 'OUTSTANDING_NONLIFE_BALANCE': '0', 'cause_code': '0', 'informer_type': '0', 'Legal_Entity': '0'}
_dfs[f'{pays}_CLMHDR_3'] = spark.createDataFrame([], schema=StructType([]))
_dfs[f'{pays}_CLMHDR_3'] = (_dfs[f'{pays}_CLMHDR_3']
    .withColumn('Frst_Bnft_Pd_Yr', F.when(F.expr("""Frst_Bnft_Pd_Yr IS NULL"""), F.lit(0)))
    .withColumn('Frst_Bnft_Pd_Mnth', F.when(F.expr("""Frst_Bnft_Pd_Mnth IS NULL"""), F.lit(0)))
    .withColumn('latst_Bnft_Pd_Yr', F.when(F.expr("""latst_Bnft_Pd_Yr IS NULL"""), F.lit(0)))
    .withColumn('latst_Bnft_Pd_Mnth', F.when(F.expr("""latst_Bnft_Pd_Mnth IS NULL"""), F.lit(0)))
    .withColumn('OUTSTANDING_LIFE_BALANCE', F.when(F.expr("""OUTSTANDING_LIFE_BALANCE IS NULL"""), F.lit(0)))
    .withColumn('OUTSTANDING_NONLIFE_BALANCE', F.when(F.expr("""OUTSTANDING_NONLIFE_BALANCE IS NULL"""), F.lit(0)))
    .withColumn('POTENTIAL_CLM_AMT', F.when(F.expr("""POTENTIAL_CLM_AMT IS NULL"""), F.lit(0)))
    .withColumn('Mnthly_Bnft', F.when(F.expr("""Mnthly_Bnft IS NULL"""), F.lit(0)))
    .withColumn('Prdct', F.when(F.expr("""Prdct='' AND length(Schm) = 4"""), F.expr("""substring(Schm,1,2)""")))
    .withColumn('Prdct', F.when(F.expr("""Prdct='' AND length(Schm) > 3"""), F.expr("""substring(Schm,1,3)""")))
)
_dfs[f'{pays}_CLMHDR_3'] = _dfs[f'{pays}_CLMHDR_3'].select('Country', 'Clm_Nmbr', 'Policy_Line_No', 'Policy_Line_Seq_No', 'Cvr_Typ', 'Schm', 'Accdnt_Dt', 'Acc_Yr', 'Acc_Mnth', 'Rgstrtn_Dt', 'Rgstrtn_Yr', 'Rgstrtn_Mnth', 'Cls_Dt', 'STATUS', 'OUTSTANDING_LIFE_BALANCE', 'Totl_Amnt_Rpybl', 'Undrwrtng_Cmpny', 'Max_Nmbr_Bnfts', 'Expry_dt', 'Totl_Amnt_Pd', 'Totl_Bnfts_Amnt_Pd', 'Frst_Bnft_Pd_Yr', 'Frst_Bnft_Pd_Mnth', 'Latst_Bnft_Pd_Yr', 'Latst_Bnft_Pd_Mnth', 'Incptn_Dt', 'Insrnc_Trm', 'Mnthly_Bnft', 'Prdct', 'Gndr', 'Dt_of_Brth', 'POTENTIAL_CLM_AMT', 'OUTSTANDING_NONLIFE_BALANCE', 'cause_code', 'informer_type', 'Legal_Entity')
_dfs[f'{pays}_CLMHDR_3'].createOrReplaceTempView(f'{pays}_CLMHDR_3')

if {pays}=DE or {pays}=IT or {pays}=FI or {pays}=PT or {pays}=NO or {pays}=DK or {pays}=ES or {pays}=SE or {pays}=TR or {pays}=NL or {pays}=NI or {pays}=IE or {pays}=GR or {pays}=PL  or {pays}=CH or {pays}=UK:
    PROC SQL;
	CREATE TABLE &pays._CLMHDR_all as 
	SELECT
			h.*,
			Rsrv_Grp
	FROM &pays._CLMHDR_3 h
	LEFT JOIN &pays._RESERVE_GROUP_SPEC s ON (h.Cvr_Typ = s.Cvr_Typ )
	
	ORDER BY Clm_Nmbr;
QUIT;

data &pays._CLMHDR_all;
retain Country Rsrv_Grp Clm_Nmbr Policy_Line_No Policy_Line_Seq_No Cvr_Typ Schm Accdnt_Dt Acc_Yr Acc_Mnth Rgstrtn_Dt Rgstrtn_Yr Rgstrtn_Mnth Cls_Dt STATUS OUTSTANDING_LIFE_BALANCE  POTENTIAL_CLM_AMT Totl_Amnt_Rpybl Undrwrtng_Cmpny Max_Nmbr_Bnfts Expry_dt Totl_Amnt_Pd Totl_Bnfts_Amnt_Pd Frst_Bnft_Pd_Yr Frst_Bnft_Pd_Mnth Latst_Bnft_Pd_Yr Latst_Bnft_Pd_Mnth Incptn_Dt Insrnc_Trm Mnthly_Bnft Prdct Gndr Dt_of_Brth OUTSTANDING_NONLIFE_BALANCE cause_code informer_type Legal_Entity ;
keep   Country Rsrv_Grp Clm_Nmbr Policy_Line_No Policy_Line_Seq_No Cvr_Typ Schm Accdnt_Dt Acc_Yr Acc_Mnth Rgstrtn_Dt Rgstrtn_Yr Rgstrtn_Mnth Cls_Dt STATUS OUTSTANDING_LIFE_BALANCE  Totl_Amnt_Rpybl Undrwrtng_Cmpny Max_Nmbr_Bnfts Expry_dt Totl_Amnt_Pd Totl_Bnfts_Amnt_Pd Frst_Bnft_Pd_Yr Frst_Bnft_Pd_Mnth Latst_Bnft_Pd_Yr Latst_Bnft_Pd_Mnth Incptn_Dt Insrnc_Trm Mnthly_Bnft Prdct Gndr Dt_of_Brth POTENTIAL_CLM_AMT  OUTSTANDING_NONLIFE_BALANCE cause_code informer_type Legal_Entity ;

	set &pays._CLMHDR_all;
	if Rsrv_Grp = "" then Rsrv_Grp = "ZZ1";
	else Rsrv_Grp = Rsrv_Grp;
run;
if {pays}=FR:
    PROC SQL;
	CREATE TABLE &pays._CLMHDR_all as 
	SELECT
			h.*,
			s.SUB_PRODUCT ,
			s.PAYMENT_BENEFIT as Clm_Pymnt_Basis,
			s.PRODUCT_TYPE
	FROM &pays._CLMHDR_3 h
	LEFT JOIN &pays._SCHEME_DATABASE s ON (h.Schm=s.Schm and  h.Cvr_Typ = s.COVER_TYPE )
	;
QUIT;

data &pays._CLMHDR_all_0 ;
set &pays._CLMHDR_all ;
if SUB_PRODUCT not in ("MORTGAGE") then Sub_Prdct ="NMORTGAGE" ;
if SUB_PRODUCT     in ("MORTGAGE") then Sub_Prdct ="MORTGAGE" ;
run;


PROC SQL;
	CREATE TABLE &pays._CLMHDR_all_1 as 
	SELECT
			h.*,
			Rsrv_Grp
	FROM &pays._CLMHDR_all_0 h
	LEFT JOIN &pays._RESERVE_GROUP_SPEC s ON (h.Cvr_Typ = s.Cvr_Typ and h.Sub_Prdct=s.Sub_Prdct and h.Clm_Pymnt_Basis=s.Clm_Pymnt_Basis )
	
	ORDER BY Clm_Nmbr;
QUIT;

data &pays._CLMHDR_all;
retain Country Rsrv_Grp Clm_Nmbr Policy_Line_No Policy_Line_Seq_No Cvr_Typ Schm Accdnt_Dt Acc_Yr Acc_Mnth Rgstrtn_Dt Rgstrtn_Yr Rgstrtn_Mnth Cls_Dt STATUS OUTSTANDING_LIFE_BALANCE  POTENTIAL_CLM_AMT Totl_Amnt_Rpybl Undrwrtng_Cmpny Max_Nmbr_Bnfts Expry_dt Totl_Amnt_Pd Totl_Bnfts_Amnt_Pd Frst_Bnft_Pd_Yr Frst_Bnft_Pd_Mnth Latst_Bnft_Pd_Yr Latst_Bnft_Pd_Mnth Incptn_Dt Insrnc_Trm Mnthly_Bnft Prdct Gndr Dt_of_Brth  OUTSTANDING_NONLIFE_BALANCE cause_code informer_type Legal_Entity ;
keep   Country Rsrv_Grp Clm_Nmbr Policy_Line_No Policy_Line_Seq_No Cvr_Typ Schm Accdnt_Dt Acc_Yr Acc_Mnth Rgstrtn_Dt Rgstrtn_Yr Rgstrtn_Mnth Cls_Dt STATUS OUTSTANDING_LIFE_BALANCE  Totl_Amnt_Rpybl Undrwrtng_Cmpny Max_Nmbr_Bnfts Expry_dt Totl_Amnt_Pd Totl_Bnfts_Amnt_Pd Frst_Bnft_Pd_Yr Frst_Bnft_Pd_Mnth Latst_Bnft_Pd_Yr Latst_Bnft_Pd_Mnth Incptn_Dt Insrnc_Trm Mnthly_Bnft Prdct Gndr Dt_of_Brth POTENTIAL_CLM_AMT  OUTSTANDING_NONLIFE_BALANCE cause_code informer_type Legal_Entity ;

	set &pays._CLMHDR_all_1;
	
	if Rsrv_Grp = "" and Cvr_Typ in ('DA','DB','DS','DC') then Rsrv_Grp = "GD1";
    if Rsrv_Grp = "" and Cvr_Typ in ('DI','DJ') then Rsrv_Grp = "GD3";
    if Rsrv_Grp = "" and Cvr_Typ in ('RR','RU') then Rsrv_Grp = "GR1";
    if Rsrv_Grp = "" and Cvr_Typ in ('GP') then Rsrv_Grp = "GP1";
    if Rsrv_Grp = "" and Cvr_Typ in ('LA','LL','LR','DY','DZ') then Rsrv_Grp = "GL1";
	if Rsrv_Grp = ""  then Rsrv_Grp = "ZZ1"; 
run;
# #####################################################  FILTRE DES SCHEMES QUI NE FONT PAS PARTI DU ON-SYSTEM  #########################################################################################
# The following codes put any claims which we want to filter out and not hold reserves for into group ZZ2.
# This replaces the previous deletion of these claims from &pays._CLMHDR_all in A2 Filter. DP 23/07/2014
# S1-S7 are Santander business that is now reserved off system using bordereau.
# H1-H6, HPA are Hispamer business that is now reserved off system using bordereau
if {pays} = ES:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "H1%"
OR Schm Like "H2%"
OR Schm Like "H3%"
OR Schm Like "H4%"
OR Schm Like "H5%"
OR Schm Like "H6%"
OR Schm Like "HPA%"
OR Schm Like "S1%"
OR Schm Like "S2%"
OR Schm Like "S3%"
OR Schm Like "S4%"
OR Schm Like "S5%"
OR Schm Like "S6%"
OR Schm Like "S7%";
Quit;
# Linea Schms are reserved by loss ratio
if {pays} =IT:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "LN1%";
Quit;
# Norway TERRA Schms are excluded
if {pays} = NO:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm IN("TA.1","TB.1","TC.1","TD.1","TE.1","TF.1","TG.1","TH.1","TI.1","TJ.1");
Quit;
# Added 08/06/2017 to deal with 501/502 uw codes.
# Germany and turkey are only country which have these codes at time of writing, Underwriting company 501/502 need not be evaluated, so remove.
if {pays} = DE OR {pays} = TR:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Undrwrtng_Cmpny IN ('501','502');
Quit;
# removing of CNP Santander TPA Schms for DK, FI, NO, SE
if {pays} = DK:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "5B%" or Schm like "5C%" or Schm like "1F%" or Schm like "1G%" ;
Quit;
if {pays} = FI:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "SN%" ;
Quit;
if {pays} = NO:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "ED.%" or Schm Like "EE.%" or Schm Like "EG.%" or Schm Like "EH.%" or Schm Like "EI.%" or Schm Like "EJ.%" or Schm Like "EK.%" or Schm Like "EL.%" or Schm Like "EM.%";
Quit;
if {pays}= SE:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "ED.%" or Schm Like "EE.%" or Schm Like "EF.%" or Schm Like "EG.%" or Schm Like "EH.%" or Schm Like "EI.%" or Schm Like "EJ.%";
Quit;
# From Q3 2012 some claims started being classified under dummy Schms 8A.1 and ZA.1 due to them being in bulk and not having an
# identifiable Schm.  These should not have reserves.
if {pays} =SE:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "ZA.%" ;
Quit;
# %DO block (non-iterative): %DO;
Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "8A.%";
Quit;
%END;
# Capital One UK contract and run-off period ended on 27th November 2013
if {pays} = UK:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "CFA%" OR Schm Like "CFN%";
Quit;
# Ceasing business with the client DLFA in Denmark from 01/04/14 -
# All claims then paid by client including those which are outstanding
if {pays} = DK:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm Like "Q%";
Quit;
# Some Greece Schms have run off more than 12 months ago and terms and conditions
# dont allow for claims 12 month after insurance period. Added by DP 23/07/2014
if {pays} = GR:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm IN ("BPI.1","BPJ.1","BPK.1","BPL.1","BPM.1","EM1.1","EM2.1","GM1.1");
Quit;
# We had some German contracts that were terminated as part of project bounce.
# We were required to pay claims up to a certain period. That period has now lapsed. Added by DP 23/07/2014
if {pays} = DE:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm IN ("P4.2","P4.3");
Quit;
# Irish Santander Schms nearly run-off. Added by DP 03/11/2014
if {pays} = IE:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Rsrv_Grp = "ZZ2"
WHERE Schm IN ("EV.3","EV.4");
Quit;
# Switzerland Cembra Schms where the max number of payments had changed to 9 payments, but which hadn't been configured
# in TIA. Information as to what Schms this applied to was taken from Taneem, and the email trail can be found saved here:
# G:/Invest/RESERVES/International/ALL/RESULTS/201610/Cembra.msg
# Added by TD 01/11/2016
if {pays} = CH:
    data &pays._CLMHDR_all;
set &pays._CLMHDR_all;
if Schm in ("GO.1", "GN.1", "G9.1", "G9.2", "G9.3", "G9.4",
"G3.1", "G3.2", "G3.3", "G3.4", "G6.1", "G6.2", "G6.3", "GL.1", "GM.1")
and year(Rgstrtn_Dt) > 2014
and Max_Nmbr_Bnfts = 12 then Max_Nmbr_Bnfts = 9;
run;
# Some France UGIP claims on TIA but full history not loaded so continue reserving off-system . Added by DP 18/09/2014
# %IF &pays. = FR %THEN %DO;
# Proc Sql;
# Update &pays._CLMHDR_all
# Set Rsrv_Grp = "ZZ2"
# WHERE Schm like "UG1%" or Schm like "UG6%" or Schm like "UG7%" or Schm like "UG8%"
# or Schm like "UG9%" or Schm like "UGC%" or Schm like "UGD%" or Schm like "UGG%"
# or Schm like "UGH%" or Schm like "UGI%" or Schm like "UGN%" or Schm like "UGX%";
# Quit;
# %END;
# Some France GMAC policies needed to be switched from GL Type 101 to 102 (therefore underwritten by FACL) as part of the
# AS60 transfer. Added by TD 09/09/2015
if {pays} = FR:
    Proc Sql;
Update &pays._CLMHDR_all
Set Undrwrtng_Cmpny = '102'
Where Schm like "1%" and Cvr_Typ like "D%";
Quit;
# France claim with wrong Schm. Added by DP 18/08/2015
if {pays} = FR:
    Proc Sql;
Update &pays._CLMHDR_all 
Set Schm = "EFD.1"
WHERE Clm_Nmbr = 1050126;
Quit;
# Portugal Fidelidade Schms loaded onto TIA but with wrong open date.  Set to notification date.
# One-off adjustment for 2015Q3. Added by DP 18/08/2015
# %IF &pays. = PT %THEN %DO;
# Proc Sql;
# Update &pays._CLMHDR_all
# Set First_Open_Date = Notification_Date
# WHERE Schm like "XAM%" or Schm like "XAN%";
# Quit;
# %END;
# ###########################################################    Data correction    #########################################################################################
# Correct the  monthly benefit it uses at product level for the average
MNTHLY_BNFT_CORR = spark.sql(f"""SELECT h.Clm_Nmbr, h.Rsrv_Grp, h.Prdct, h.Schm, h.Cvr_Typ, h.Mnthly_Bnft,
		0.00001 as Mnthly_Bnft_Corr, h.STATUS
		FROM {pays}_CLMHDR_ALL h
		INNER JOIN {pays}_MNTHLY_BNFT_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE (h.Mnthly_Bnft < m.LOWER
		or h.Mnthly_Bnft > m.UPPER
		or h.Mnthly_Bnft = .)
		AND h.STATUS in ('OP','RO')""")
MNTHLY_BNFT_CORR.createOrReplaceTempView('MNTHLY_BNFT_CORR')

MNTHLY_BNFT_AVRG_PRDCT = spark.sql(f"""SELECT h.Rsrv_Grp, h.Prdct, count(h.Clm_Nmbr) as COUNT, MEAN(h.Mnthly_Bnft) as AVG_Mnthly_Bnft,
		STD(h.Mnthly_Bnft) as STD_Mnthly_Bnft
		FROM {pays}_CLMHDR_ALL h
		INNER JOIN WORK.{pays}_MNTHLY_BNFT_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE h.Mnthly_Bnft > m.LOWER
		AND h.Mnthly_Bnft < m.UPPER
		GROUP BY h.Rsrv_Grp, h.Prdct""")
MNTHLY_BNFT_AVRG_PRDCT.createOrReplaceTempView('MNTHLY_BNFT_AVRG_PRDCT')

# Make average benefit by group.
MNTHLY_BNFT_AVRG_GRP = spark.sql(f"""SELECT h.Rsrv_Grp, count(h.Clm_Nmbr) as COUNT, MEAN(h.Mnthly_Bnft) as AVG_Mnthly_Bnft,
		STD(h.Mnthly_Bnft) as STD_Mnthly_Bnft
		FROM {pays}_CLMHDR_ALL h
		INNER JOIN WORK.{pays}_MNTHLY_BNFT_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE h.Mnthly_Bnft > m.LOWER
		AND h.Mnthly_Bnft < m.UPPER
		GROUP BY h.Rsrv_Grp""")
MNTHLY_BNFT_AVRG_GRP.createOrReplaceTempView('MNTHLY_BNFT_AVRG_GRP')

MNTHLY_BNFT_CORR = spark.sql("""SELECT UNIQUE c.Clm_Nmbr, c.Rsrv_Grp, c.Prdct, c.Schm, c.Cvr_Typ, c.Mnthly_Bnft as OLD_Mnthly_Bnft,
		case when p.COUNT>9 then p.Avg_Mnthly_Bnft
		else g.Avg_Mnthly_Bnft end as Mnthly_Bnft,
		c.STATUS
		FROM MNTHLY_BNFT_CORR c
		LEFT JOIN MNTHLY_BNFT_AVRG_PRDCT p
		ON (c.Prdct = p.Prdct and c.Rsrv_Grp = p.Rsrv_Grp)
		LEFT JOIN MNTHLY_BNFT_AVRG_GRP g
		ON (c.Rsrv_Grp = g.Rsrv_Grp)
		GROUP BY c.Clm_Nmbr""")
MNTHLY_BNFT_CORR.createOrReplaceTempView('MNTHLY_BNFT_CORR')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{pays}_CLMHDR_ALL').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table('pays').join(spark.table('_CLMHDR_ALL'), ['Clm_Nmbr'], 'left')
_dfs[f'{pays}_CLMHDR_ALL'] = _dfs[f'{pays}_CLMHDR_ALL'].drop('OLD_Mnthly_Bnft')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

# Correct the OTSTNDNG_BLNC it uses at product level for the average
CLAIMS_IN_CLMTRNS = spark.sql(f"""SELECT UNIQUE t.CLA_CASE_NO AS Clm_Nmbr , max(t.TRANS_DATE) format=ddmmyy10. as TRANS_DATE, h.FIRST_OPEN_DATE,
		h.FIRST_CLOSE_DATE, h.REOPEN_DATE, h.RECLOSE_DATE, h.STATUS, 
		SUM(-GROSS_AMT) as AMT 
		FROM DATA.{pays}_CLMTRNS t
		INNER JOIN DATA.{pays}_CLMHDR h ON (h.CLA_CASE_NO = t.CLA_CASE_NO)
		WHERE ITEM_CLASS in (2,3,4)
		GROUP BY t.CLA_CASE_NO, h.FIRST_OPEN_DATE, h.FIRST_CLOSE_DATE, h.REOPEN_DATE, h.RECLOSE_DATE,
		 h.STATUS""")
CLAIMS_IN_CLMTRNS.createOrReplaceTempView('CLAIMS_IN_CLMTRNS')


# KO : ligne 612
# PROC SQL;
		UPDATE &pays._CLMHDR_ALL
		SET OUTSTANDING_LIFE_BALANCE = OUTSTANDING_NONLIFE_BALANCE
		WHERE OUTSTANDING_LIFE_BALANCE in (.,0);
QUIT;
# fin KO

OTSTNDNG_BLNC_CORR = spark.sql(f"""SELECT UNIQUE h.Clm_Nmbr, h.Rsrv_Grp, h.Prdct, h.Schm, h.Cvr_Typ, h.OUTSTANDING_LIFE_BALANCE,
		0.00001 as Otstndng_Balnc_Corr, h.STATUS
		FROM {pays}_CLMHDR_ALL h
		INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE STATUS in ('OP','RO')
		and (h.OUTSTANDING_LIFE_BALANCE < m.LOWER
		or h.OUTSTANDING_LIFE_BALANCE > m.UPPER
		or h.OUTSTANDING_LIFE_BALANCE is null)""")
OTSTNDNG_BLNC_CORR.createOrReplaceTempView('OTSTNDNG_BLNC_CORR')

OTSTANDING_BLNC_AVRG_PRDCT = spark.sql(f"""SELECT UNIQUE h.Rsrv_Grp, h.Prdct, count(h.Clm_Nmbr) as COUNT, MEAN(t.AMT) as AVG_AMT, STD(t.AMT) as STD_AMT
		FROM Claims_in_CLMTRNS t
		INNER JOIN {pays}_CLMHDR_ALL h
		ON (t.Clm_Nmbr = h.Clm_Nmbr)
		INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE t.AMT > m.LOWER
		AND t.AMT < m.UPPER
		AND t.AMT <> .
		GROUP BY m.Rsrv_Grp, h.Prdct""")
OTSTANDING_BLNC_AVRG_PRDCT.createOrReplaceTempView('OTSTANDING_BLNC_AVRG_PRDCT')

OTSTANDING_BLNC_AVRG_GRP = spark.sql(f"""SELECT UNIQUE h.Rsrv_Grp, count(h.Clm_Nmbr) as COUNT, MEAN(t.AMT) as AVG_AMT, STD(t.AMT) as STD_AMT
		FROM Claims_in_CLMTRNS t
		INNER JOIN {pays}_CLMHDR_ALL h
		ON (t.Clm_Nmbr = h.Clm_Nmbr)
		INNER JOIN {pays}_OTSTANDING_BLNC_LIMITS m
		ON (h.Rsrv_Grp = m.Rsrv_Grp)
		WHERE t.AMT > m.LOWER
		AND t.AMT < m.UPPER
		AND t.AMT <> .
		GROUP BY m.Rsrv_Grp""")
OTSTANDING_BLNC_AVRG_GRP.createOrReplaceTempView('OTSTANDING_BLNC_AVRG_GRP')

Otstndng_blnc_corr = spark.sql("""SELECT UNIQUE c.Clm_Nmbr, c.Rsrv_Grp, c.Prdct, c.Schm, c.Cvr_Typ, c.OUTSTANDING_LIFE_BALANCE as OLD_OUTSTANDING_LIFE_BALANCE,
		case when p.COUNT>9 then p.AVG_AMT
			 else g.AVG_AMT end as OUTSTANDING_LIFE_BALANCE,
		c.STATUS
		FROM Otstndng_blnc_corr c
		LEFT JOIN OTSTANDING_BLNC_AVRG_PRDCT p
		ON (c.Prdct = p.Prdct and c.Rsrv_Grp = p.Rsrv_Grp)
		LEFT JOIN OTSTANDING_BLNC_AVRG_GRP g
		ON (c.Rsrv_Grp = g.Rsrv_Grp)
		GROUP BY c.Clm_Nmbr""")
Otstndng_blnc_corr.createOrReplaceTempView('Otstndng_blnc_corr')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{pays}_CLMHDR_ALL').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table('pays').join(spark.table('_CLMHDR_ALL'), ['Clm_Nmbr'], 'left')
_dfs[f'{pays}_CLMHDR_ALL'] = _dfs[f'{pays}_CLMHDR_ALL'].drop('OLD_OUTSTANDING_LIFE_BALANCE')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{pays}_CLMHDR_ALL')
_dfs[f'{pays}_CLMHDR_ALL'] = (_dfs[f'{pays}_CLMHDR_ALL']
    .withColumn('OUTSTANDING_LIFE_BALANCE', F.when(F.expr("""OUTSTANDING_LIFE_BALANCE IS NULL"""), F.lit(0)))
)
_dfs[f'{pays}_CLMHDR_ALL'] = _dfs[f'{pays}_CLMHDR_ALL'].drop('OUTSTANDING_NONLIFE_BALANCE')
_dfs[f'{pays}_CLMHDR_ALL'] = _dfs[f'{pays}_CLMHDR_ALL'].withColumnRenamed('OUTSTANDING_LIFE_BALANCE', 'Otstndng_Balnc')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')
# LIBNAME INPUT -> base Spark: input.{pays}_CLMHDR_ALL
_dfs[f'{pays}_CLMHDR_ALL'].write.mode('overwrite').saveAsTable(f'input.{pays}_CLMHDR_ALL')

# Make average GAP potential Amont by group.
POTENTIAL_CLM_AMT_CORR = spark.sql(f"""SELECT h.Clm_Nmbr, h.Rsrv_Grp, h.Prdct, h.Schm, h.Cvr_Typ, h.POTENTIAL_CLM_AMT,
		0.00001 as POTENTIAL_CLM_AMT_Corr, h.STATUS
		FROM INPUT.{pays}_CLMHDR_ALL h
		WHERE h.STATUS in ('OP','RO') and h.Rsrv_Grp='GP1' """)
POTENTIAL_CLM_AMT_CORR.createOrReplaceTempView('POTENTIAL_CLM_AMT_CORR')

# Make average GAP potential Amont by group.
POTENTIAL_CLM_AMT_AVRG_GRP = spark.sql("""SELECT h.Rsrv_Grp, count(h.Clm_Nmbr) as COUNT, MEAN(h.POTENTIAL_CLM_AMT) as AVG_POTENTIAL_CLM_AMT,
		STD(h.POTENTIAL_CLM_AMT) as STD_POTENTIAL_CLM_AMT
		FROM POTENTIAL_CLM_AMT_CORR h
		GROUP BY h.Rsrv_Grp""")
POTENTIAL_CLM_AMT_AVRG_GRP.createOrReplaceTempView('POTENTIAL_CLM_AMT_AVRG_GRP')

POTENTIAL_CLM_AMT_CORR = spark.sql("""SELECT UNIQUE c.Clm_Nmbr, c.Rsrv_Grp, c.Prdct, c.Schm, c.Cvr_Typ, c.POTENTIAL_CLM_AMT as OLD_POTENTIAL_CLM_AMT,
		case when c.POTENTIAL_CLM_AMT=0 then p.AVG_POTENTIAL_CLM_AMT
			 else c.POTENTIAL_CLM_AMT end as POTENTIAL_CLM_AMT,
		c.STATUS
		FROM POTENTIAL_CLM_AMT_CORR c
		LEFT JOIN POTENTIAL_CLM_AMT_AVRG_GRP p
		ON ( c.Rsrv_Grp = p.Rsrv_Grp)
		GROUP BY c.Clm_Nmbr""")
POTENTIAL_CLM_AMT_CORR.createOrReplaceTempView('POTENTIAL_CLM_AMT_CORR')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table(f'{pays}_CLMHDR_ALL').orderBy('Clm_Nmbr')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')

_dfs[f'{pays}_CLMHDR_ALL'] = spark.table('INPUT').join(spark.table('pays'), ['Clm_Nmbr'], 'left')
_dfs[f'{pays}_CLMHDR_ALL'] = _dfs[f'{pays}_CLMHDR_ALL'].drop('OLD_POTENTIAL_CLM_AMT')
_dfs[f'{pays}_CLMHDR_ALL'].createOrReplaceTempView(f'{pays}_CLMHDR_ALL')
# LIBNAME INPUT -> base Spark: input.{pays}_CLMHDR_ALL
_dfs[f'{pays}_CLMHDR_ALL'].write.mode('overwrite').saveAsTable(f'input.{pays}_CLMHDR_ALL')

mend()
data_corecction(pays="FI")
data_corecction(pays="UK")
data_corecction(pays="FR")
data_corecction(pays="SE")
data_corecction(pays="PT")
data_corecction(pays="DE")
data_corecction(pays="NO")
data_corecction(pays="ES")
data_corecction(pays="CH")
data_corecction(pays="IT")
data_corecction(pays="PL")
data_corecction(pays="DE")
data_corecction(pays="IE")
data_corecction(pays="NL")
data_corecction(pays="NI")
data_corecction(pays="GR")
data_corecction(pays="TR")
data_corecction(pays="DK")
data_corecction(pays="AT")
data_corecction(pays="BE")
data_corecction(pays="CO")
data_corecction(pays="MX")
data_corecction(pays="LT")
data_corecction(pays="LV")
data_corecction(pays="EE")
# %DATA_CORECCTION(pays=LU) ;