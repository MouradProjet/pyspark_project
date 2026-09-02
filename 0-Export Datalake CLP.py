from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ####################################################
# ################### INVENTAIRE TIA #################
# ####################################################
# #########################################################################
# ################### 2ème Etape: Extraction des données  #################
# #########################################################################
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_04_V2"
# /
tia_path = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/TIA"  # LIBNAME TIA
spark.sql('CREATE SCHEMA IF NOT EXISTS tia')  # base Spark pour LIBNAME TIA
# ------------------------------------------------------------------
à_path = "condition d'être dans le groupe Unix gg_sas_db_clp =="  # LIBNAME à
spark.sql('CREATE SCHEMA IF NOT EXISTS à')  # base Spark pour LIBNAME à
# ----------------------------------------------------------------
# MODOP accès au datalake CLP via WPS AXA France
# ---------------------------------------------------------------
souhaité_path = "- nom_db => ne pas changer 
- nom_schema => ne pas changer 
- nom_options => ne pas changer"  # LIBNAME souhaité
spark.sql('CREATE SCHEMA IF NOT EXISTS souhaité')  # base Spark pour LIBNAME souhaité
# /
# data lake CLP
wps_mac_connexion_db(nom_libname="clp_wps", nom_db="WPS_SHINE_BLCL", nom_schema="clp_wps", nom_options="readbuff=10000 schema=clp_wps")
clp_wps_path = "odbcold  DSN=WPS_SHINE_BLCL  authdomain="DB_WPS_SHINE_BLCL"  schema=clp_wps"  # LIBNAME clp_wps
spark.sql('CREATE SCHEMA IF NOT EXISTS clp_wps')  # base Spark pour LIBNAME clp_wps
Daap_level_1_dueonly = spark.table('clp_wps.daap_level_1_dueonly')
Daap_level_1_dueonly.createOrReplaceTempView('Daap_level_1_dueonly')
# LIBNAME TIA -> base Spark: tia.Daap_level_1_dueonly
Daap_level_1_dueonly.write.mode('overwrite').saveAsTable('tia.Daap_level_1_dueonly')

idcf_fr_ugip_cl = spark.table('clp_wps.idcf_fr_ugip_cl')
idcf_fr_ugip_cl.createOrReplaceTempView('idcf_fr_ugip_cl')
# LIBNAME TIA -> base Spark: tia.idcf_fr_ugip_cl
idcf_fr_ugip_cl.write.mode('overwrite').saveAsTable('tia.idcf_fr_ugip_cl')
