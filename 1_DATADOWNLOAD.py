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
# ######## Context: EXTRACTION CLAIMS DANS LE DATA LAKE POUR CONSTRUIRE LES BASES CLMHDR & CLMTRNS QUI PERMETTENT DE CALCULER LES CASES RESERVES (ICOP & RBNP)
# ####################################################  CREATION DES LIBRARY  #########################################################################################
# LIBNAME CLAIM2 ODBC REQUIRED="DSN=WPS_Shine_blcl" SCHEMA = "clp_wps" readbuff=100000;
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
data_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/Claims Extracts"  # LIBNAME data
spark.sql('CREATE SCHEMA IF NOT EXISTS data')  # base Spark pour LIBNAME data
# %wps_mac_connexion_db(
# nom_libname = CLAIM ,
# nom_db = WPS_SHINE_BLCL ,
# nom_schema = clp_wps ,
# nom_options = readbuff=10000 schema=global_claims_extracts
# );
claim_path = "odbcold  DSN=WPS_SHINE_BLCL  authdomain="DB_WPS_SHINE_BLCL"  schema=global_claims_extracts"  # LIBNAME CLAIM
spark.sql('CREATE SCHEMA IF NOT EXISTS claim')  # base Spark pour LIBNAME CLAIM
def datadownload_dtw(pays):
    # ###########################################################       CREATION DE LA BASE CLMHDR    #########################################################################################
    # %LET pays =PT ;
    # data data.&pays._claim_header ;
    # set  CLAIM2.&pays._claim_head_tiariadmin ;
    # run ;
    _dfs[f'{pays}_claim_header'] = spark.table(f'claim.{pays}_claim_head_tiariadmin')
    # table filtrer sans categroy C
    _dfs[f'{pays}_claim_header'].createOrReplaceTempView(f'{pays}_claim_header')
    # LIBNAME data -> base Spark: data.{pays}_claim_header
    _dfs[f'{pays}_claim_header'].write.mode('overwrite').saveAsTable(f'data.{pays}_claim_header')

    # data data.&pays._claim_header ;ou il y a la catergorie C
    # set  CLAIM2.&pays._claim_header;
    # run ;
    _dfs[f'{pays}_CLMHDR'] = spark.sql(f"""SELECT COUNTRY_CD as Country,
    			CLA_CASE_NO format=BEST12.,
    			POLICY_LINE_NO format=BEST12.,
    			POLICY_LINE_SEQ_NO format = BEST12., 
    			COVER format = $2. length = 2,
    			scheme format = $8. length = 8, 
    			datepart(incident_date) as incident_date  format = ddmmyy10., 
    			datepart(NOTIFICATION_DATE) as NOTIFICATION_DATE format = ddmmyy10.,
    			datepart(COVER_START_DATE) as COVER_START_DATE format = ddmmyy10., 
    			datepart(COVER_END_DATE) as COVER_END_DATE format = ddmmyy10.,
    			datepart(FIRST_OPEN_DATE) as FIRST_OPEN_DATE  format = ddmmyy10., 
    			datepart(FIRST_CLOSE_DATE) as FIRST_CLOSE_DATE format ddmmyy10.,
    			datepart(REOPEN_DATE) as REOPEN_DATE format = ddmmyy10., 
    			datepart(RECLOSE_DATE) as RECLOSE_DATE format = ddmmyy10.,
    			STATUS format = $2. length = 2,
                CLOSE_CODE format = $3. length = 3,
    			INSURANCE_TERM format = BEST12.,
    			UW_COMPANY  , 
    			CLAIM_MONTHLY_BENEFIT format = BEST12., 
    			POLICY_MONTHLY_BENEFIT format = BEST12.,
    			OUTSTANDING_LIFE_BALANCE format = BEST12., 
    			datepart(POLICY_EXPIRY_DATE) as POLICY_EXPIRY_DATE format = ddmmyy10., 
    			MAX_NO_OF_PAYMENTS format = BEST12.,
    			IS_BULK format = $1. length = 1,
    			OUTSTANDING_NONLIFE_BALANCE format = BEST12.,
    			GROUP_POL_NO format = $6. length = 6,
    			TOTAL_PAYMENTS format = BEST12., 
    			TOTAL_NON_OTHER_PAYMENTS format = BEST12., 
    			TOTAL_NON_OTHER_PAYMENTS_AMT format = BEST12., 
    			datepart(BIRTH_DATE) as BIRTH_DATE  format = ddmmyy10.,
    			GENDER format = $1. length = 1, 
    			POLICY_NO format = BEST12., 
    			EVENT_TYPE format = $3. length = 3,
    			DECLINE format = $4. length =4,
    			DECLINE_REASON_REF format = $50. length =50,
    			potential_clm_amt  AS POTENTIAL_CLM_AMT format = BEST12.,
    			datepart(DECISION_DATE) as DECISION_DATE format = ddmmyy10.,
    			datepart(last_activity_date) as last_activity_date format = ddmmyy10.,  
    			PROD_ID format = $5. length = 5,
    		    cause_code format = $20. length = 20,
    		    informer_type format = $20. length = 20
    		FROM data.{pays}_claim_header """)
    _dfs[f'{pays}_CLMHDR'].createOrReplaceTempView(f'{pays}_CLMHDR')

    # #####################################################  CREATION DE LA BASE CLMTRANS  #########################################################################################
    _dfs[f'{pays}_claim_detail'] = spark.table(f'claim.{pays}_claim_det_tiariadmin')
    _dfs[f'{pays}_claim_detail'].createOrReplaceTempView(f'{pays}_claim_detail')
    # LIBNAME data -> base Spark: data.{pays}_claim_detail
    _dfs[f'{pays}_claim_detail'].write.mode('overwrite').saveAsTable(f'data.{pays}_claim_detail')

    # /
    # data data.&pays._claim_detail ;
    # set  CLAIM2.&pays._claim_detail ;
    # run ;
    _dfs[f'{pays}_CLMTRNS'] = spark.sql(f"""SELECT COUNTRY_CD as Country, 
    			CLA_CASE_NO format = BEST12.,
    			datepart(TRANS_DATE) as TRANS_DATE format = ddmmyy10.,
    			CURRENCY_AMT format = BEST12.,
    			SPECIFICATION format = $5. length = 5,
    			ITEM_CLASS format = BEST12.,
    			GROSS_AMT format = BEST12.,
    			datepart(DUE_DATE) as DUE_DATE format = ddmmyy10.,
    			SUBITEM_TYPE format = $3. length = 3,
    			ACC_ITEM_NO format = BEST12.
    		FROM data.{pays}_claim_detail """)
    _dfs[f'{pays}_CLMTRNS'].createOrReplaceTempView(f'{pays}_CLMTRNS')


datadownload_dtw(pays="DE")
datadownload_dtw(pays="FR")
datadownload_dtw(pays="FI")
datadownload_dtw(pays="NO")
datadownload_dtw(pays="IT")
datadownload_dtw(pays="ES")
datadownload_dtw(pays="IE")
datadownload_dtw(pays="GR")
datadownload_dtw(pays="NI")
datadownload_dtw(pays="NL")
datadownload_dtw(pays="PL")
datadownload_dtw(pays="PT")
datadownload_dtw(pays="TR")
datadownload_dtw(pays="DK")
datadownload_dtw(pays="SE")
datadownload_dtw(pays="UK")
datadownload_dtw(pays="CH")
datadownload_dtw(pays="AT")
datadownload_dtw(pays="BE")
datadownload_dtw(pays="MX")
datadownload_dtw(pays="LU")
datadownload_dtw(pays="LT")
datadownload_dtw(pays="CO")
datadownload_dtw(pays="EE")
datadownload_dtw(pays="KR")
datadownload_dtw(pays="PE")
datadownload_dtw(pays="LV")
UK_CLMHDR = spark.table('data.UK_CLMHDR')
UK_CLMHDR = (UK_CLMHDR
    .withColumn('Country', F.when(F.expr("""Country='GB'"""), F.lit('UK')))
)
UK_CLMHDR.createOrReplaceTempView('UK_CLMHDR')
# LIBNAME DATA -> base Spark: data.UK_CLMHDR
UK_CLMHDR.write.mode('overwrite').saveAsTable('data.UK_CLMHDR')

UK_CLMTRNS = spark.table('data.UK_CLMTRNS')
UK_CLMTRNS = (UK_CLMTRNS
    .withColumn('Country', F.when(F.expr("""Country='GB'"""), F.lit('UK')))
)
UK_CLMTRNS.createOrReplaceTempView('UK_CLMTRNS')
# LIBNAME DATA -> base Spark: data.UK_CLMTRNS
UK_CLMTRNS.write.mode('overwrite').saveAsTable('data.UK_CLMTRNS')
