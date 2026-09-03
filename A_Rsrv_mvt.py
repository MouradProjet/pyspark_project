from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# #########################################################################################################################
# ######################################### Reserve Mouvement ###############################################################
# ##########################################################################################################################
# Macro variable: ( à renseigner avant le lancement)
lreseau = "X"
arrete1 = "2026_04_V2"
arrete2 = "2026_06_Prov"
exer1 = "Q126"
exer2 = "Q226"
pays = "BE"
output1_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete1}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"  # LIBNAME Output1
spark.sql('CREATE SCHEMA IF NOT EXISTS output1')  # base Spark pour LIBNAME Output1
output2_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete2}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"  # LIBNAME Output2
spark.sql('CREATE SCHEMA IF NOT EXISTS output2')  # base Spark pour LIBNAME Output2
# Import de la SDB
import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete2}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
    _df_tmp = (spark.read.format('com.crealytics.spark.excel')
        .option('dataAddress', f"'{onglet}'!A1")
        .option('header', 'true')
        .load(file))
    _df_tmp.createOrReplaceTempView(f'{out}')


import_excel(file=import_01, out="SDB", onglet="SDB")
def rsrv_mvt(pays, exer1, exer2):
    # ############################################# Mise en Forme des données  #################################################
    # #######################################################################################################################
    # - Import et mise en forme des données :
    # * - Ajout de PMP_Sales_Name et Agent_Name
    # * - Ajout Flag (New Claim / Stock )
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = spark.sql(f"""Select Distinct a.Date_of_reserving,
    				a.Country,
    				a.Rsrv_Typ,
    				a.Cover ,
    				a.Legal_Entity,	
    				a.Cvr_Typ,
    				a.Rsrv_Grp ,
    				a.Schm,
    				a.Clm_Nmbr,
    				a.Accdnt_Dt,
    				a.Rgstrtn_Dt,
    				a.Cls_Dt,
    				a.Status,
    				a.Otstndng_Balnc,
    				a.Probablty_Otstndng,
    				a.Totl_Bnfts_Amnt_Pd,
    				a.Frst_Bnft_Pd_Yr,
    				a.Frst_Bnft_Pd_Mnth,
    				a.Latst_Bnft_Pd_Yr,
    				a.Latst_Bnft_Pd_Mnth,
    				a.Mnthly_Bnft,
    				a.Nmbr_Mnths_Pndng,
    				a.Nmbr_Bnfts_Pd,
    				a.Nmbr_Bnfts_Otstndng,
    				a.Rsrv_Amt,
    				a.Legal_Entity,
    				a.LEGACY_SCHEME_BOOK,		
    				b.Agent_Name,
    				b.PMP_Sales_Name
    From Output1.CLMHDR_ALL_{pays} as a
    left join SDB as b on (a.Country = b.Country and a.Schm = b.Scheme)
    Where a.Rsrv_Grp not in ('ZZ1','ZZ2') and LEGACY_SCHEME_BOOK ='TIA'  """)
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}_{exer1}')

    # verifier si la colonne LEGACY_SCHEME_BOOK existe dans la table input CLMHDR
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = spark.table(f'CLMHDR_ALL_{pays}_{exer1}')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Rsrv_Typ', 'Rsrv_Typ_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Status', 'Status_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Otstndng_Balnc', 'Otstndng_Balnc_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Probablty_Otstndng', 'Probablty_Otstndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Totl_Bnfts_Amnt_Pd', 'Totl_Bnfts_Amnt_Pd_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Mnthly_Bnft', 'Mnthly_Bnft_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Nmbr_Mnths_Pndng', 'Nmbr_Mnths_Pndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Nmbr_Bnfts_Pd', 'Nmbr_Bnfts_Pd_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Nmbr_Bnfts_Otstndng', 'Nmbr_Bnfts_Otstndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer1}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}_{exer1}')

    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = spark.sql(f"""Select Distinct a.Date_of_reserving,
    				a.Country,
    				a.Rsrv_Typ,
    				a.Cover ,
    				a.Cvr_Typ,
    				a.Rsrv_Grp ,
    				a.Schm,
    				a.Clm_Nmbr,
    				a.Accdnt_Dt,
    				a.Rgstrtn_Dt,
    				a.Cls_Dt,
    				a.Status,
    				a.Otstndng_Balnc,
    				a.Probablty_Otstndng,
    				a.Totl_Bnfts_Amnt_Pd,
    				a.Frst_Bnft_Pd_Yr,
    				a.Frst_Bnft_Pd_Mnth,
    				a.Latst_Bnft_Pd_Yr,
    				a.Latst_Bnft_Pd_Mnth,
    				a.Mnthly_Bnft,
    				a.Nmbr_Mnths_Pndng,
    				a.Nmbr_Bnfts_Pd,
    				a.Nmbr_Bnfts_Otstndng,
    				a.Rsrv_Amt,
    				a.Legal_Entity,			
    				b.Agent_Name,
    				b.PMP_Sales_Name
    From  Output2.CLMHDR_ALL_{pays} as a
    left join SDB as b on (a.Country = b.Country and a.Schm = b.Scheme)
    Where Rsrv_Grp not in ('ZZ1','ZZ2') and LEGACY_SCHEME_BOOK ='TIA' """)
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}_{exer2}')

    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = spark.table(f'CLMHDR_ALL_{pays}_{exer2}')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Rsrv_Typ', 'Rsrv_Typ_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Status', 'Status_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Otstndng_Balnc', 'Otstndng_Balnc_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Probablty_Otstndng', 'Probablty_Otstndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Totl_Bnfts_Amnt_Pd', 'Totl_Bnfts_Amnt_Pd_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Mnthly_Bnft', 'Mnthly_Bnft_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Nmbr_Mnths_Pndng', 'Nmbr_Mnths_Pndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Nmbr_Bnfts_Pd', 'Nmbr_Bnfts_Pd_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Nmbr_Bnfts_Otstndng', 'Nmbr_Bnfts_Otstndng_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'] = _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_')
    _dfs[f'CLMHDR_ALL_{pays}_{exer2}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}_{exer2}')

    # Ajout Flag (New Claim- Stock)
    CLMHDR_ALL_flag = spark.sql(f"""select Distinct a.*,
               case when b.Clm_Nmbr is not null then 'Stock'
                    else 'New Claim'
               end as Flag
        from CLMHDR_ALL_{pays}_{exer2} as a
        left join CLMHDR_ALL_{pays}_{exer1} as b
        on a.Clm_Nmbr = b.Clm_Nmbr """)
    CLMHDR_ALL_flag.createOrReplaceTempView('CLMHDR_ALL_flag')

    # ##########################################  Granularity level : Country x Rsrv_Typ ##############################################
    # *###########################################################################################################################
    # exer1 Vs exer2 at the granularity  Country x Rsrv_Typ
    _dfs[f'Rsrv_Amt_{exer1}'] = spark.sql(f"""select Distinct Country ,
    				Rsrv_Typ_{exer1} as Rsrv_Typ ,
    				Sum(Rsrv_Amt_{exer1}) as Rsrv_Amt_{exer1} 
    From CLMHDR_ALL_{pays}_{exer1}
    Where Rsrv_Typ_{exer1} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer1} """)
    _dfs[f'Rsrv_Amt_{exer1}'].createOrReplaceTempView(f'Rsrv_Amt_{exer1}')

    _dfs[f'Rsrv_Amt_{exer2}'] = spark.sql(f"""select Distinct Country ,
    				Rsrv_Typ_{exer2} as Rsrv_Typ,
    				Sum(Rsrv_Amt_{exer2}) as Rsrv_Amt_{exer2}
    From CLMHDR_all_{pays}_{exer2}
    Where Rsrv_Typ_{exer2} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer2} """)
    _dfs[f'Rsrv_Amt_{exer2}'].createOrReplaceTempView(f'Rsrv_Amt_{exer2}')

    _dfs[f'Rsrv_Amt_{exer1}'] = spark.table(f'Rsrv_Amt_{exer1}').orderBy('country', 'Rsrv_typ')
    _dfs[f'Rsrv_Amt_{exer1}'].createOrReplaceTempView(f'Rsrv_Amt_{exer1}')

    _dfs[f'Rsrv_Amt_{exer2}'] = spark.table(f'Rsrv_Amt_{exer2}').orderBy('country', 'Rsrv_typ')
    _dfs[f'Rsrv_Amt_{exer2}'].createOrReplaceTempView(f'Rsrv_Amt_{exer2}')

    # MERGE: FULL OUTER JOIN (if a or b / no condition)
    Rsrv_Mvt_tot = spark.table('Rsrv_Amt_').join(spark.table('Rsrv_Amt_'), ['Country', 'Rsrv_typ'], 'full')
    Rsrv_Mvt_tot = (Rsrv_Mvt_tot
        .withColumn(f'{exer2}_vs_{exer1}', F.expr(f"""coalesce(Rsrv_Amt_{exer2},0) - coalesce(Rsrv_Amt_{exer1},0)"""))
    )
    Rsrv_Mvt_tot.createOrReplaceTempView('Rsrv_Mvt_tot')

    # ##########################################  Granularity level : Country x Rsrv_Typ x flag   ##############################################
    # *###########################################################################################################################
    Rsrv_Mvt_flag = spark.sql(f"""select  Distinct Country ,
    				 Rsrv_Typ_{exer2} ,
    				 flag,
    				 sum(Rsrv_Amt_{exer2}) as Rsrv_Amt_{exer2}
    From CLMHDR_ALL_flag as  a
    Where Rsrv_Typ_{exer2} in ('ICOP','RBNP')
    group by Country ,Rsrv_Typ_{exer2},flag   """)
    Rsrv_Mvt_flag.createOrReplaceTempView('Rsrv_Mvt_flag')

    Rsrv_Mvt_cvr = spark.sql(f"""select  Distinct Country ,
    				 Rsrv_Typ_{exer2},
    				 flag,
    				 Legal_Entity,
    				 cvr_Typ,
    				 sum(Rsrv_Amt_{exer2}) as Rsrv_Amt_{exer2}
    From CLMHDR_ALL_flag as  a
    Where Rsrv_Typ_{exer2} in ('ICOP','RBNP')
    group by Country ,Rsrv_Typ_{exer2},flag , Legal_Entity , Cvr_Typ """)
    Rsrv_Mvt_cvr.createOrReplaceTempView('Rsrv_Mvt_cvr')

    # ##########################################  Granularity level : Country x Rsrv_Typ x Legal_Entity  ##############################################
    # *###########################################################################################################################
    _dfs[f'Rsrv_Mvt_{exer1}_LE'] = spark.sql(f"""Select Distinct Country,
    				Rsrv_Typ_{exer1} as Rsrv_Typ,
    				Legal_Entity,
    				sum(Rsrv_Amt_{exer1}) As Rsrv_Amt_{exer1}
    From CLMHDR_ALL_{pays}_{exer1} As a
    Where Rsrv_Typ_{exer1} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer1} ,Legal_Entity ;
    
    proc sql""")
    _dfs[f'Rsrv_Mvt_{exer1}_LE'].createOrReplaceTempView(f'Rsrv_Mvt_{exer1}_LE')
    _dfs[f'Rsrv_Mvt_{exer2}_LE'] = spark.sql(f"""Select Distinct Country,
    				Rsrv_Typ_{exer2} as Rsrv_Typ,
    				Legal_Entity,
    				sum(Rsrv_Amt_{exer2}) As Rsrv_Amt_{exer2}
    From CLMHDR_ALL_flag As a
    Where Rsrv_Typ_{exer2} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer2} ,Legal_Entity""")
    _dfs[f'Rsrv_Mvt_{exer2}_LE'].createOrReplaceTempView(f'Rsrv_Mvt_{exer2}_LE')

    _dfs[f'Rsrv_Mvt_{exer1}_LE'] = spark.table(f'Rsrv_Mvt_{exer1}_LE').orderBy('Rsrv_typ', 'Legal_Entity')
    _dfs[f'Rsrv_Mvt_{exer1}_LE'].createOrReplaceTempView(f'Rsrv_Mvt_{exer1}_LE')

    _dfs[f'Rsrv_Mvt_{exer2}_LE'] = spark.table(f'Rsrv_Mvt_{exer2}_LE').orderBy('Rsrv_typ', 'Legal_Entity')
    _dfs[f'Rsrv_Mvt_{exer2}_LE'].createOrReplaceTempView(f'Rsrv_Mvt_{exer2}_LE')

    # MERGE: FULL OUTER JOIN (if a or b / no condition)
    Rsrv_Mvt_LE = spark.table('Rsrv_Mvt_').join(spark.table('Rsrv_Mvt_'), ['Rsrv_typ', 'Legal_Entity'], 'full')
    Rsrv_Mvt_LE = (Rsrv_Mvt_LE
        .withColumn(f'{exer2}_vs_{exer1}', F.expr(f"""coalesce(Rsrv_Amt_{exer2},0) - coalesce(Rsrv_Amt_{exer1},0)"""))
    )
    Rsrv_Mvt_LE.createOrReplaceTempView('Rsrv_Mvt_LE')

    # ##########################################  Granularity level : Country x Rsrv_Typ x Legal_Entity x Cover type  ##############################################
    # *###########################################################################################################################
    _dfs[f'Rsrv_Mvt_{exer1}_Cvr'] = spark.sql(f"""Select Distinct Country,
    				Rsrv_Typ_{exer1} as Rsrv_Typ ,
    				Legal_Entity,
    				Cvr_Typ,
    				sum(Rsrv_Amt_{exer1}) As Rsrv_Amt_{exer1}
    From CLMHDR_ALL_{pays}_{exer1} As a
    Where Rsrv_Typ_{exer1} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer1} ,Legal_Entity, Cvr_Typ ;
    
    
    proc sql""")
    _dfs[f'Rsrv_Mvt_{exer1}_Cvr'].createOrReplaceTempView(f'Rsrv_Mvt_{exer1}_Cvr')
    _dfs[f'Rsrv_Mvt_{exer2}_Cvr'] = spark.sql(f"""Select Distinct Country,
    				Rsrv_Typ_{exer2} as Rsrv_Typ ,
    				Legal_Entity,
    				Cvr_Typ ,
    				sum(Rsrv_Amt_{exer2}) As Rsrv_Amt_{exer2}
    From CLMHDR_ALL_flag As a
    Where Rsrv_Typ_{exer2} in ('ICOP','RBNP') 
    Group by Country , Rsrv_Typ_{exer2} ,Legal_Entity, Cvr_Typ""")
    _dfs[f'Rsrv_Mvt_{exer2}_Cvr'].createOrReplaceTempView(f'Rsrv_Mvt_{exer2}_Cvr')

    _dfs[f'Rsrv_Mvt_{exer1}_Cvr'] = spark.table(f'Rsrv_Mvt_{exer1}_Cvr').orderBy('Rsrv_typ', 'Legal_Entity', 'Cvr_Typ')
    _dfs[f'Rsrv_Mvt_{exer1}_Cvr'].createOrReplaceTempView(f'Rsrv_Mvt_{exer1}_Cvr')

    _dfs[f'Rsrv_Mvt_{exer2}_Cvr'] = spark.table(f'Rsrv_Mvt_{exer2}_Cvr').orderBy('Rsrv_typ', 'Legal_Entity', 'Cvr_Typ')
    _dfs[f'Rsrv_Mvt_{exer2}_Cvr'].createOrReplaceTempView(f'Rsrv_Mvt_{exer2}_Cvr')

    # MERGE: FULL OUTER JOIN (if a or b / no condition)
    Rsrv_Mvt_Cvr = spark.table('Rsrv_Mvt_').join(spark.table('Rsrv_Mvt_'), ['Rsrv_typ', 'Legal_Entity', 'Cvr_Typ'], 'full')
    Rsrv_Mvt_Cvr = (Rsrv_Mvt_Cvr
        .withColumn(f'{exer2}_vs_{exer1}', F.expr(f"""coalesce(Rsrv_Amt_{exer2},0) - coalesce(Rsrv_Amt_{exer1},0)"""))
    )
    Rsrv_Mvt_Cvr.createOrReplaceTempView('Rsrv_Mvt_Cvr')

    # ###############################################   New_claims_ICOP  #################################################################
    # *###########################################################################################################################
    New_Claims_ICOP = spark.sql(f"""Select Distinct Country,
    				Date_of_reserving,
    				Flag,
    				Cvr_Typ,
    				Legal_Entity,
    				Rsrv_Typ_{exer2},
    				Agent_Name,
    				Schm,
    				Clm_Nmbr,
    				Rgstrtn_Dt,
    				Cls_Dt,
    				status_{exer2},
    				Totl_Bnfts_Amnt_Pd_{exer2},
    				Mnthly_Bnft_{exer2},
    				Nmbr_Bnfts_Pd_{exer2},
    				Nmbr_Bnfts_Otstndng_{exer2},
    				Rsrv_Amt_{exer2}	
    From CLMHDR_ALL_flag as a 
    Where Flag = 'New Claim' and Rsrv_Typ_{exer2} = 'ICOP' 
    group by Country, Date_of_reserving, Flag,Rsrv_Typ_{exer2},Legal_Entity, Cvr_Typ, Agent_Name """)
    New_Claims_ICOP.createOrReplaceTempView('New_Claims_ICOP')

    # ###############################################   New_claims_RBNP  #################################################################
    # *###########################################################################################################################
    New_Claims_RBNP = spark.sql(f"""Select Distinct Country,
    				Date_of_reserving,
    				Flag,
    				Cvr_Typ,
    				Legal_Entity,
    				Rsrv_Typ_{exer2},
    				Agent_Name,
    				Schm,
    				Clm_Nmbr,
    				Rgstrtn_Dt,
    				Cls_Dt,
    				status_{exer2},
    				Nmbr_Mnths_Pndng_{exer2},
    				Totl_Bnfts_Amnt_Pd_{exer2},
    				Mnthly_Bnft_{exer2},
    				Nmbr_Bnfts_Pd_{exer2},
    				Nmbr_Bnfts_Otstndng_{exer2},
    				Otstndng_Balnc_{exer2},
    				Probablty_Otstndng_{exer2},
    				Rsrv_Amt_{exer2}	
    From CLMHDR_ALL_flag as a 
    Where Flag = 'New Claim' and Rsrv_Typ_{exer2} = 'RBNP' 
    group by Country, Date_of_reserving, Flag,Rsrv_Typ_{exer2},Legal_Entity, Cvr_Typ, Agent_Name """)
    New_Claims_RBNP.createOrReplaceTempView('New_Claims_RBNP')

    # ############################################### Stock_ICOP####################################################
    # ###############################################################################################################
    Stock_ICOP_0 = spark.sql(f"""select Distinct Country,
    				Date_of_reserving,
    				Flag,
    				Clm_Nmbr,
    				Rsrv_Typ_{exer2},
    				Legal_Entity,
    				Cover,
    				Cvr_Typ,
    				Agent_Name,
    				Schm,
    				Clm_Nmbr,
    				status_{exer2},
    				cls_Dt,
    				Rgstrtn_Dt,
    				Totl_Bnfts_Amnt_Pd_{exer2},
    				Mnthly_Bnft_{exer2},
    				Nmbr_Mnths_Pndng_{exer2},
    				Nmbr_Bnfts_Pd_{exer2},
    				Nmbr_Bnfts_Otstndng_{exer2},
    				Rsrv_Amt_{exer2}	
    From CLMHDR_ALL_flag
    Where Flag = 'Stock' And Rsrv_Typ_{exer2} ='ICOP'
    Group by Country, Date_of_reserving, flag ,Rsrv_Typ_{exer2}, Legal_Entity, Cvr_Typ, Agent_Name, Schm """)
    Stock_ICOP_0.createOrReplaceTempView('Stock_ICOP_0')

    Stock_ICOP = spark.sql(f"""Select Distinct a.Country ,
    				a.Date_of_reserving,
    				a.Legal_Entity,
    				a.Clm_Nmbr,
    				a.cover ,
    				a.Cvr_Typ,
    				a.Agent_Name,
    				a.Schm,
    				a.Rsrv_Typ_{exer2} ,
    				b.Rsrv_Typ_{exer1} ,
    				a.Totl_Bnfts_Amnt_Pd_{exer2},
    				a.Mnthly_Bnft_{exer2},
    				b.Mnthly_Bnft_{exer1},
    				a.Nmbr_Bnfts_Otstndng_{exer2}, 
    				b.Nmbr_Bnfts_Otstndng_{exer1},
    				a.Rsrv_Amt_{exer2},
    				b.Rsrv_Amt_{exer1}
    From Stock_ICOP_0 As a 
    left join CLMHDR_ALL_{pays}_{exer1}  As b
    On (a.Country = b.Country)  and(a.Legal_Entity = b.Legal_Entity)and (a.Agent_Name = b.Agent_Name) and (a.Schm = b.Schm) and (a.Clm_Nmbr = b.Clm_Nmbr)
    group by a.Country, a.Date_of_reserving ,a.Rsrv_Typ_{exer2}, Legal_Entity,a.Cover , a.Cvr_Typ, a.Agent_Name,a.Schm """)
    Stock_ICOP.createOrReplaceTempView('Stock_ICOP')

    Mvt_ICOP = spark.table('Stock_ICOP')
    Mvt_ICOP = (Mvt_ICOP
        .withColumn('Amt',
        F.when(F.expr(f"""Rsrv_Typ_{exer1} ='ICOP'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Rsrv_Typ_{exer1} ='RBNP'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Rsrv_Typ_{exer1} ='CLOSE'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Missing (Rsrv_Typ_{exer1} )"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .otherwise(F.col('Amt')))
    )
    Mvt_ICOP.createOrReplaceTempView('Mvt_ICOP')

    Mvt_ICOP = spark.sql(f"""select a.Stock_ICOP_{exer2},
    	   sum(Amt) as Amount
    From Mvt_ICOP a
    group by Stock_ICOP_{exer2}""")
    Mvt_ICOP.createOrReplaceTempView('Mvt_ICOP')

    # ############################################### Stock_RBNP ####################################################
    # ###############################################################################################################
    Stock_RBNP_0 = spark.sql(f"""select Distinct Country,
    				Date_of_reserving,
    				Flag,
    				Clm_Nmbr,
    				Rsrv_Typ_{exer2},
    				Legal_Entity,
    				Cover,
    				Cvr_Typ,
    				Agent_Name,
    				Schm,
    				Clm_Nmbr,
    				status_{exer2},
    				cls_Dt,
    				Rgstrtn_Dt,
    				Totl_Bnfts_Amnt_Pd_{exer2},
    				Mnthly_Bnft_{exer2},
    				Nmbr_Mnths_Pndng_{exer2},
    				Nmbr_Bnfts_Pd_{exer2},
    				Nmbr_Bnfts_Otstndng_{exer2},
    				Rsrv_Amt_{exer2}	
    From CLMHDR_ALL_flag
    Where Flag = 'Stock' And Rsrv_Typ_{exer2} ='RBNP'
    Group by Country, Date_of_reserving, flag ,Rsrv_Typ_{exer2}, Legal_Entity, Cvr_Typ, Agent_Name, Schm """)
    Stock_RBNP_0.createOrReplaceTempView('Stock_RBNP_0')

    Stock_RBNP = spark.sql(f"""Select Distinct a.Country ,
    				a.Date_of_reserving,
    				a.Legal_Entity,
    				a.Clm_Nmbr,
    				a.cover ,
    				a.Cvr_Typ,
    				a.Agent_Name,
    				a.Schm,
    				a.Rsrv_Typ_{exer2},
    				b.Rsrv_Typ_{exer1},
    				a.Totl_Bnfts_Amnt_Pd_{exer2},
    				a.Mnthly_Bnft_{exer2},
    				b.Mnthly_Bnft_{exer1},
    				a.Nmbr_Bnfts_Otstndng_{exer2},
    				b.Nmbr_Bnfts_Otstndng_{exer1},
    				a.Rsrv_Amt_{exer2},
    				b.Rsrv_Amt_{exer1}
    From Stock_RBNP_0 As a 
    left join CLMHDR_ALL_{pays}_{exer1}  As b
    On (a.Country = b.Country)  and(a.Legal_Entity = b.Legal_Entity)and (a.Agent_Name = b.Agent_Name) and (a.Schm = b.Schm) and (a.Clm_Nmbr = b.Clm_Nmbr)
    group by a.Country, a.Date_of_reserving ,a.Rsrv_Typ_{exer2}, Legal_Entity,a.Cover , a.Cvr_Typ, a.Agent_Name,a.Schm """)
    Stock_RBNP.createOrReplaceTempView('Stock_RBNP')

    Mvt_RBNP = spark.table('Stock_RBNP')
    Mvt_RBNP = (Mvt_RBNP
        .withColumn('Amt',
        F.when(F.expr(f"""Rsrv_Typ_{exer1} ='ICOP'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Rsrv_Typ_{exer1} ='RBNP'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Rsrv_Typ_{exer1} ='CLOSE'"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .when(F.expr(f"""Missing (Rsrv_Typ_{exer1} )"""), F.expr(f"""aggregate(Rsrv_Amt_{exer2})"""))
         .otherwise(F.col('Amt')))
    )
    Mvt_RBNP.createOrReplaceTempView('Mvt_RBNP')

    Mvt_RBNP = spark.sql(f"""select a.Stock_RBNP_{exer2},
    	   sum(Amt) as Amount 
    From Mvt_RBNP a
    group by Stock_RBNP_{exer2}""")
    Mvt_RBNP.createOrReplaceTempView('Mvt_RBNP')

    # ################################################### Close Claims_ICOP ###############################################
    # ################################################################################################################
    Close_Claims_ICOP = spark.sql(f"""select distinct b.Country ,
    				b.Date_of_reserving,
    				b.flag,
    				a.Legal_Entity,
    				a.cover ,
    				a.Cvr_Typ,
    				a.Agent_Name,
    				a.Schm,
    				a.Rsrv_Typ_{exer1},
    				b.Rsrv_Typ_{exer2},
    				a.Totl_Bnfts_Amnt_Pd_{exer1},
    				b.Totl_Bnfts_Amnt_Pd_{exer2},
    				a.Mnthly_Bnft_{exer1},
    				b.Mnthly_Bnft_{exer2},
    				a.Nmbr_Bnfts_Otstndng_{exer1},
    				b.Nmbr_Bnfts_Otstndng_{exer2},
    				a.Rsrv_Amt_{exer1},
    				b.Rsrv_Amt_{exer2},
    				a.Clm_Nmbr
    from CLMHDR_ALL_{pays}_{exer1} a 
    left join CLMHDR_ALL_flag b
    on (a.Schm = b.Schm) and (a.Legal_Entity = b.Legal_Entity) and (a.Clm_Nmbr = b.Clm_Nmbr) and (a.Cvr_Typ= b.Cvr_Typ)
    Where  a.Rsrv_Typ_{exer1}= 'ICOP' and b.Rsrv_Typ_{exer2} = 'CLOSE'  """)
    Close_Claims_ICOP.createOrReplaceTempView('Close_Claims_ICOP')

    # ################################################### Close Claims_RBNP ###############################################
    # ################################################################################################################
    Close_Claims_RBNP = spark.sql(f"""select Distinct b.Country ,
    				b.Date_of_reserving,
    				b.flag,
    				a.Legal_Entity,
    				a.cover ,
    				a.PMP_Sales_Name,
    				a.Schm,
    				a.Rsrv_Typ_{exer1},
    				b.Rsrv_Typ_{exer2},
    				b.status_{exer2},
    				a.Totl_Bnfts_Amnt_Pd_{exer1},
    				b.Totl_Bnfts_Amnt_Pd_{exer2},
    				a.Mnthly_Bnft_{exer1},
    				b.Mnthly_Bnft_{exer2},
    				a.Nmbr_Bnfts_Otstndng_{exer1},
    				b.Nmbr_Bnfts_Otstndng_{exer2},
    				a.Otstndng_Balnc_{exer1},
    				b.Otstndng_Balnc_{exer2},
    				a.Probablty_Otstndng_{exer1},
    				b.Probablty_Otstndng_{exer2},
    				a.Rsrv_Amt_{exer1},
    				b.Rsrv_Amt_{exer2},
    				a.Clm_Nmbr
    from CLMHDR_ALL_{pays}_{exer1} a 
    left join CLMHDR_ALL_flag b
    on (a.Schm = b.Schm) and (a.Legal_Entity = b.Legal_Entity) and (a.Clm_Nmbr = b.Clm_Nmbr) and (a.Cover = b.cover)
    Where  a.Rsrv_Typ_{exer1} = 'RBNP'  and b.Rsrv_Typ_{exer2} = 'CLOSE'  and b.status_{exer2} in('CL','DC')  """)
    Close_Claims_RBNP.createOrReplaceTempView('Close_Claims_RBNP')

    # ######################################### Export ##################################################"
    chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete2}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/REPORTING/Controles/CR/Output/Clms_Ctrl_{pays}_{exer1}_{exer2}.xlsx"
    database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{database}!A1').option('header', 'true').mode('overwrite').save(chemin)


export_excel1(database="CLMHDR_ALL_flag")
export_excel1(database="Close_Claims_ICOP")
export_excel1(database="Close_Claims_RBNP")
export_excel1(database="Stock_ICOP")
export_excel1(database="Stock_RBNP")
export_excel1(database="New_Claims_ICOP")
export_excel1(database="New_Claims_RBNP")
export_excel1(database="Rsrv_Mvt_tot")
export_excel1(database="Rsrv_Mvt_flag")
export_excel1(database="Rsrv_Mvt_LE")
export_excel1(database="Rsrv_Mvt_cvr")
export_excel1(database="Mvt_ICOP")
export_excel1(database="Mvt_RBNP")
mend()
# #################################################### Lancement des programmes ###################################################
rsrv_mvt(pays="BE", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="CH", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="DE", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="DK", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="ES", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="FI", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="FR", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="GR", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="IE", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="IT", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="MX", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="NI", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="NL", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="NO", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="PT", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="SE", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="SK", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="TR", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="UK", exer1=exer1, exer2=exer2)
rsrv_mvt(pays="CO", exer1=exer1, exer2=exer2)
