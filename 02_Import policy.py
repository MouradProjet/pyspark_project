from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# *DATALAKE;
wps_mac_connexion_db(nom_libname="POLICY", nom_db="WPS_SHINE_BLCL", nom_schema="clp_wps", nom_options="readbuff=10000 schema=global_policy_extracts")
policy_path = "odbcold  DSN=WPS_SHINE_BLCL  authdomain="DB_WPS_SHINE_BLCL"  schema=global_policy_extracts"  # LIBNAME POLICY
spark.sql('CREATE SCHEMA IF NOT EXISTS policy')  # base Spark pour LIBNAME POLICY
# téléchargement des tables dans la policy extract
arrete = "2026_06_Prov"
cutoffdate = "26JUN2026:23:59:59"dt"
fin_annee = "2026-06-26"
pol_ext_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/Policy Extracts"  # LIBNAME Pol_ext
spark.sql('CREATE SCHEMA IF NOT EXISTS pol_ext')  # base Spark pour LIBNAME Pol_ext
# #####################################################################################################
# BLK ET MF
# #####################################################################################################
def mef(country, pays):
    # BLK
    _dfs[f'{country}_BLK'] = spark.table(f'policy.{pays}_pe_bulk_header_view')
    _dfs[f'{country}_BLK'] = _dfs[f'{country}_BLK'].filter(~F.expr(f"""policy_transaction_date > {cutoffdate}"""))
    _dfs[f'{country}_BLK'].createOrReplaceTempView(f'{country}_BLK')

    _null_ = spark.createDataFrame([], schema=StructType([]))
    _null_.createOrReplaceTempView('_null_')

    _dfs[f'{country}_BLK'] = spark.sql(f"""SELECT COUNTRY_CD format=$2., POLICY_LINE_NO, PRODUCT format=$4. length=4, PRODUCT_VERSION format=$10., 
    PRODUCT_LINE_VERSION, POLICY_NUMBER, COVER_CODE format=$5. length=5,
    datepart(START_DATE) as start_date format=ddmmyy10., datepart(END_DATE) as end_date format=ddmmyy10., 
    datepart(CANCEL_DATE) as cancel_date format=ddmmyy10., GL_TYPE format=$60., INSURANCE_TERM_MONTHS,
    TAR, RENTAL, ADVANCE, OUTSTANDING_BALANCE, BALLOON, ADVISED_TOTAL_PREMIUM, ADVISED_GROSS_PREMIUM, 
    ADVISED_TOTAL_REFUND, ADVISED_GROSS_REFUND, PREMIUM, NON_PREMIUM,
    TOTAL_COMMISSION, PREMIUM_REFUND, NON_PREMIUM_REFUND, TOTAL_COMMISSION_CLAWBACK,
    PRODUCT_LINE format=$2. length=2, CANCEL_CODE, 
    datepart(LAST_REINSTATEMENT_DATE) as last_reinstatement_date format=ddmmyy10., 
    LAST_TRANSACTION_TYPE  format=$10., OTHER, LAST_NON_ZERO_PREM,
    GL_TYPE_NO, LEGACY_SCHEME_CODE format=$20. length=20, LEGACY_AGREEMENT_NUMBER format=$50. length=50, 
    LEGACY_RENEWAL_NUMBER  format=$20. length=20 
    FROM {country}_BLK """)
    _dfs[f'{country}_BLK'].createOrReplaceTempView(f'{country}_BLK')

    _null_ = spark.createDataFrame([], schema=StructType([]))
    _null_.createOrReplaceTempView('_null_')

    _dfs[f'{country}_BLK'] = spark.table(f'pol_ext.{country}_BLK')
    _dfs[f'{country}_BLK'] = (_dfs[f'{country}_BLK']
        .withColumn('y_char', F.expr("""GL_TYPE_NO*1"""))
    )
    # _2
    # _2
    _dfs[f'{country}_BLK'] = _dfs[f'{country}_BLK'].drop('GL_TYPE_NO')
    _dfs[f'{country}_BLK'] = _dfs[f'{country}_BLK'].withColumnRenamed('y_char', 'GL_TYPE_NO')
    _dfs[f'{country}_BLK'].createOrReplaceTempView(f'{country}_BLK')
    # LIBNAME Pol_ext -> base Spark: pol_ext.{country}_BLK
    _dfs[f'{country}_BLK'].write.mode('overwrite').saveAsTable(f'pol_ext.{country}_BLK')

    # MF
    _null_ = spark.createDataFrame([], schema=StructType([]))
    _null_.createOrReplaceTempView('_null_')

    _dfs[f'{country}_MF'] = spark.sql(f"""SELECT COUNTRY_CD format=$2., POLICY_LINE_NO, PRODUCT format=$4. length=4, PRODUCT_VERSION format=$10., 
    PRODUCT_LINE_VERSION, POLICY_NUMBER, COVER_CODE format=$5. length=5,
    datepart(START_DATE) as start_date format=ddmmyy10., datepart(END_DATE) as end_date format=ddmmyy10., 
    datepart(CANCEL_DATE) as cancel_date format=ddmmyy10., GL_TYPE format=$60., INSURANCE_TERM_MONTHS,
    TAR, RENTAL, ADVANCE, OUTSTANDING_BALANCE, BALLOON, ADVISED_TOTAL_PREMIUM, ADVISED_GROSS_PREMIUM, 
    ADVISED_TOTAL_REFUND, ADVISED_GROSS_REFUND, PREMIUM, NON_PREMIUM,
    TOTAL_COMMISSION, PREMIUM_REFUND, NON_PREMIUM_REFUND, TOTAL_COMMISSION_CLAWBACK,
    PRODUCT_LINE format=$2. length=2, CANCEL_CODE, 
    datepart(LAST_REINSTATEMENT_DATE) as last_reinstatement_date format=ddmmyy10., 
    LAST_TRANSACTION_TYPE  format=$10., OTHER, LAST_NON_ZERO_PREM,
    GL_TYPE_NO, LEGACY_SCHEME_CODE format=$20. length=20, LEGACY_AGREEMENT_NUMBER format=$50. length=50, 
    LEGACY_RENEWAL_NUMBER  format=$20. length=20,
    policy_transaction_date
    FROM POLICY.{pays}_pe_fixed_term_header_view""")
    _dfs[f'{country}_MF'].createOrReplaceTempView(f'{country}_MF')

    _null_ = spark.createDataFrame([], schema=StructType([]))
    _null_.createOrReplaceTempView('_null_')

    _dfs[f'{country}_MF'] = spark.table(f'pol_ext.{country}_MF')
    _dfs[f'{country}_MF'] = (_dfs[f'{country}_MF']
        .withColumn('y_char', F.expr("""GL_TYPE_NO*1"""))
    )
    _dfs[f'{country}_MF'] = _dfs[f'{country}_MF'].filter(~F.expr(f"""policy_transaction_date > {cutoffdate}"""))
    _dfs[f'{country}_MF'] = _dfs[f'{country}_MF'].drop('GL_TYPE_NO')
    _dfs[f'{country}_MF'] = _dfs[f'{country}_MF'].withColumnRenamed('y_char', 'GL_TYPE_NO')
    _dfs[f'{country}_MF'].createOrReplaceTempView(f'{country}_MF')
    # LIBNAME Pol_ext -> base Spark: pol_ext.{country}_MF
    _dfs[f'{country}_MF'].write.mode('overwrite').saveAsTable(f'pol_ext.{country}_MF')


mef(country="AUSTRIA", pays="AT")
mef(country="BELGIUM", pays="BE")
mef(country="COLOMBIA", pays="CO")
mef(country="DENMARK", pays="DK")
mef(country="ESTONIA", pays="EE")
mef(country="FINLAND", pays="FI")
mef(country="FRANCE", pays="FR")
mef(country="GERMANY", pays="DE")
mef(country="GREECE", pays="GR")
mef(country="IRELAND", pays="IE")
mef(country="ITALY", pays="IT")
mef(country="LATVIA", pays="LV")
mef(country="LITHUANIA", pays="LT")
mef(country="LUXEMBOURG", pays="LU")
mef(country="MEXICO", pays="MX")
mef(country="NETHERLANDS", pays="NL")
mef(country="NORTHERNIRELAND", pays="NI")
mef(country="NORWAY", pays="NO")
mef(country="PERU", pays="PE")
mef(country="POLAND", pays="PL")
mef(country="PORTUGAL", pays="PT")
mef(country="SPAIN", pays="ES")
mef(country="SWEDEN", pays="SE")
mef(country="SWITZERLAND", pays="CH")
mef(country="TURKEY", pays="TR")
mef(country="UK", pays="UK")
# #####################################################################################################
# #####################################################################################################
# UPF
# #####################################################################################################
def mef(country, pays):

    # KO : ligne 2
    # proc sql;
    %wps_mac_connexion_db(
        nom_connexion = CNX ,
        nom_db = WPS_SHINE_BLCL ,
        nom_schema = clp_wps ,
        nom_options = readbuff=30000 schema=global_policy_extracts
        ); 
    select distinct  policy_transaction_date into : liste_annee separated by ' ' from  connection to cnx
    (
        SELECT year(policy_transaction_date) as  policy_transaction_date     
        FROM global_policy_extracts.&pays._pe_upfront_header_view
        where to_date(policy_transaction_date) < to_date("&fin_annee.")
        order by policy_transaction_date
    );
        disconnect from cnx;
quit;
    # fin KO

    print(f">>> {liste_annee}")
    for("annee", in=f"({liste_annee})", do="%nrstr(
 
        proc sql;
        %wps_mac_connexion_db(
            nom_connexion = CNX", nom_db="WPS_SHINE_BLCL", nom_schema="clp_wps", nom_options="readbuff=30000 schema=global_policy_extracts")
    # Concaténation des tables de la work
    _dfs[f'{country}_upf'] = spark.table('policy_transaction_date_:')
    _dfs[f'{country}_upf'].createOrReplaceTempView(f'{country}_upf')

    _dfs[f'{country}_upf'] = spark.table(f'{country}_upf')
    _dfs[f'{country}_upf'].createOrReplaceTempView(f'{country}_upf')
    # LIBNAME Pol_ext -> base Spark: pol_ext.{country}_upf
    _dfs[f'{country}_upf'].write.mode('overwrite').saveAsTable(f'pol_ext.{country}_upf')

    for("annee", in=f"({liste_annee})", do=f"%nrstr(
proc datasets lib=work memtype=DATA;   delete policy_transaction_date_{annee};   run; 
)")

mef(country="AUSTRIA", pays="AT")
mef(country="BELGIUM", pays="BE")
mef(country="COLOMBIA", pays="CO")
mef(country="DENMARK", pays="DK")
mef(country="ESTONIA", pays="EE")
mef(country="FINLAND", pays="FI")
mef(country="GERMANY", pays="DE")
mef(country="GREECE", pays="GR")
mef(country="IRELAND", pays="IE")
mef(country="ITALY", pays="IT")
mef(country="LATVIA", pays="LV")
mef(country="LITHUANIA", pays="LT")
mef(country="MEXICO", pays="MX")
mef(country="NETHERLANDS", pays="NL")
mef(country="NORTHERNIRELAND", pays="NI")
mef(country="NORWAY", pays="NO")
mef(country="PERU", pays="PE")
mef(country="POLAND", pays="PL")
mef(country="PORTUGAL", pays="PT")
mef(country="SPAIN", pays="ES")
mef(country="SWEDEN", pays="SE")
mef(country="SWITZERLAND", pays="CH")
mef(country="TURKEY", pays="TR")
mef(country="UK", pays="UK")
mef(country="FRANCE", pays="FR")
mef(country="LUXEMBOURG", pays="LU")
# #####################################################################################################
# #####################################################################################################
# MR
# #####################################################################################################
def mef(country, pays):
    # %let country = ESTONIA ; %let pays = EE ;

    # KO : ligne 5
    # proc sql;
    %wps_mac_connexion_db(
        nom_connexion = CNX ,
        nom_db = WPS_SHINE_BLCL ,
        nom_schema = clp_wps ,
        nom_options = readbuff=30000 schema=global_policy_extracts
        ); 
    select distinct TRANSACTION_DATE into : liste_annee separated by ' ' from  connection to cnx
    (
        SELECT year(TRANSACTION_DATE) as  TRANSACTION_DATE     
        FROM global_policy_extracts.&pays._pe_renewable_details_view
        where to_date(TRANSACTION_DATE) < to_date("&fin_annee.")
        order by TRANSACTION_DATE
    );
        disconnect from cnx;
quit;
    # fin KO

    print(f">>> {liste_annee}")
    for("annee", in=f"({liste_annee})", do="%nrstr(
 
        proc sql;
        %wps_mac_connexion_db(
            nom_connexion = CNX", nom_db="WPS_SHINE_BLCL", nom_schema="clp_wps", nom_options="readbuff=30000 schema=global_policy_extracts")
    # Concaténation des tables de la work
    _dfs[f'{country}_mrtrans'] = spark.table('policy_transaction_date_:')
    # _2
    _dfs[f'{country}_mrtrans'].createOrReplaceTempView(f'{country}_mrtrans')
    # LIBNAME Pol_ext -> base Spark: pol_ext.{country}_mrtrans
    _dfs[f'{country}_mrtrans'].write.mode('overwrite').saveAsTable(f'pol_ext.{country}_mrtrans')

    for("annee", in=f"({liste_annee})", do=f"%nrstr(
proc datasets lib=work memtype=DATA;   delete policy_transaction_date_{annee};   run; 
)")

mef(country="AUSTRIA", pays="AT")
mef(country="BELGIUM", pays="BE")
mef(country="COLOMBIA", pays="CO")
mef(country="DENMARK", pays="DK")
mef(country="FINLAND", pays="FI")
mef(country="FRANCE", pays="FR")
mef(country="GERMANY", pays="DE")
mef(country="GREECE", pays="GR")
mef(country="IRELAND", pays="IE")
mef(country="ITALY", pays="IT")
mef(country="MEXICO", pays="MX")
mef(country="NETHERLANDS", pays="NL")
mef(country="NORWAY", pays="NO")
mef(country="POLAND", pays="PL")
mef(country="PORTUGAL", pays="PT")
mef(country="SPAIN", pays="ES")
mef(country="SWITZERLAND", pays="CH")
mef(country="TURKEY", pays="TR")
mef(country="UK", pays="UK")
mef(country="SWEDEN", pays="SE")
mef(country="ESTONIA", pays="EE")
mef(country="PERU", pays="PE")
mef(country="LATVIA", pays="LV")
mef(country="LUXEMBOURG", pays="LU")
mef(country="NORTHERNIRELAND", pays="NI")
mef(country="LITHUANIA", pays="LT")
# #####################################################################################################