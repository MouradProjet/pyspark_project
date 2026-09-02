from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# The libname input below is infact the output location
# Experience analysis is from previous quarter
lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
quarter = "2026Q2"
out_gep_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/GEP/Output/DAAP"  # LIBNAME Out_GEP
ym_sup = 202606
ym_inf = 202403
# ####################################################   REP DATA  #########################################################################################
import_01 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx"
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=import_01, out="flag_legacy", onglet="flag_legacy")
def rep(cc, grp_rsrv):
    # %let CC=FR ; %let Grp_Rsrv=GD2;
    _dfs[f'HISTO_FLUX_{cc}'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao as Flag_Macao  
    from OUT_GEP.HISTO_FLUX_{cc}  t1 
    left join FLAG_LEGACY t8 on (t1.country=t8.country AND t1.scheme=t8.scheme and t1.cover=t8.Cover) 
     """)
    _dfs[f'HISTO_FLUX_{cc}'].createOrReplaceTempView(f'HISTO_FLUX_{cc}')

    _dfs[f'HISTO_FLUX_{cc}'] = spark.table(f'HISTO_FLUX_{cc}')
    _dfs[f'HISTO_FLUX_{cc}'] = (_dfs[f'HISTO_FLUX_{cc}']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'HISTO_FLUX_{cc}'].createOrReplaceTempView(f'HISTO_FLUX_{cc}')

    _dfs[f'HISTO_FLUX_{cc}'] = spark.table(f'HISTO_FLUX_{cc}')
    # if RPP not in ("0","") then LEGACY_SCHEME_BOOK="MACAO";
    if RPP in ("0","") then LEGACY_SCHEME_BOOK="TIA";
    _dfs[f'HISTO_FLUX_{cc}'] = _dfs[f'HISTO_FLUX_{cc}'].drop('informer_type', 'Data_Validated', 'Flag_Macao')
    _dfs[f'HISTO_FLUX_{cc}'].createOrReplaceTempView(f'HISTO_FLUX_{cc}')
    # LIBNAME OUT_GEP -> base Spark: out_gep.HISTO_FLUX_{cc}
    _dfs[f'HISTO_FLUX_{cc}'].write.mode('overwrite').saveAsTable(f'out_gep.HISTO_FLUX_{cc}')

    _dfs[f'{cc}_REP_{grp_rsrv}'] = spark.table(f'out_gep.HISTO_FLUX_{cc}')
    _dfs[f'{cc}_REP_{grp_rsrv}'] = _dfs[f'{cc}_REP_{grp_rsrv}'].filter(F.col('Rsrv_Grp') == f"{grp_rsrv}'  AND Month >'{ym_inf}'  AND LEGACY_SCHEME_BOOK='TIA")
    _dfs[f'{cc}_REP_{grp_rsrv}'] = _dfs[f'{cc}_REP_{grp_rsrv}'].select('country', 'Rsrv_Grp', 'Month', 'REP')
    _dfs[f'{cc}_REP_{grp_rsrv}'].createOrReplaceTempView(f'{cc}_REP_{grp_rsrv}')

    _dfs[f'{cc}_REP_{grp_rsrv}'] = spark.sql(f"""SELECT DISTINCT country,
                        Rsrv_Grp,
                        Month,
                        sum(REP) as REP
                                                           
         FROM    {cc}_REP_{grp_rsrv}
         where Month <='{ym_sup}'
         group by country, Rsrv_Grp, Month 
          """)
    _dfs[f'{cc}_REP_{grp_rsrv}'].createOrReplaceTempView(f'{cc}_REP_{grp_rsrv}')

    # Pour stocker l'avant dernier montant REP dans le dernier REP
    _dfs[f'{cc}_Tes_{grp_rsrv}'] = spark.sql(f"""select count(REP) as count
    from {cc}_REP_{grp_rsrv}""")
    _dfs[f'{cc}_Tes_{grp_rsrv}'].createOrReplaceTempView(f'{cc}_Tes_{grp_rsrv}')

    _NULL_ = spark.table(f'{cc}_Tes_{grp_rsrv}')
    _NULL_.createOrReplaceTempView('_NULL_')

    print(f"{x_dernier}")
    _NULL_ = spark.table(f'{cc}_REP_{grp_rsrv}')
    # IF/THEN (manual review needed):
    #   if  _N_= %EVAL({x_dernier}-1) THEN call symput('X_dernierS',REP) ;
    _NULL_.createOrReplaceTempView('_NULL_')

    print(f"{x_derniers}")
    _dfs[f'{cc}_REP_{grp_rsrv}'] = spark.table(f'{cc}_REP_{grp_rsrv}')
    _dfs[f'{cc}_REP_{grp_rsrv}'] = (_dfs[f'{cc}_REP_{grp_rsrv}']
        .withColumn('REP', F.when(F.expr(f"""_N_= {x_dernier}"""), F.expr(f"""{x_derniers}""")))
    )
    _dfs[f'{cc}_REP_{grp_rsrv}'].createOrReplaceTempView(f'{cc}_REP_{grp_rsrv}')

    chemin = f"~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/IBNR/Inputs/{quarter}/{pays}"
    database.write.format('com.crealytics.spark.excel').option('dataAddress', f'{database}!A1').option('header', 'true').mode('overwrite').save(chemin)


export_excel(database=f"{cc}_REP_{grp_rsrv}", pays=cc)
# PROC DATASETS → Spark table operations

mend()
rep(cc="FR", grp_rsrv="GD1")
rep(cc="FR", grp_rsrv="GR1")
rep(cc="FR", grp_rsrv="GL1")
rep(cc="FR", grp_rsrv="GP1")
# %REP(CC=FR,Grp_Rsrv=GD2);
# vide
# %REP(CC=FR,Grp_Rsrv=GR2);
# vide
rep(cc="FR", grp_rsrv="GR3")
rep(cc="FR", grp_rsrv="GL2")
rep(cc="FR", grp_rsrv="GD3")
rep(cc="PT", grp_rsrv="GD1")
rep(cc="PT", grp_rsrv="GR1")
rep(cc="PT", grp_rsrv="GL1")
rep(cc="DE", grp_rsrv="GD1")
rep(cc="DE", grp_rsrv="GR1")
rep(cc="DE", grp_rsrv="GL1")
rep(cc="DE", grp_rsrv="GP1")
rep(cc="DK", grp_rsrv="GD1")
rep(cc="DK", grp_rsrv="GR1")
rep(cc="DK", grp_rsrv="GL1")
rep(cc="FI", grp_rsrv="GD1")
rep(cc="FI", grp_rsrv="GR1")
rep(cc="FI", grp_rsrv="GL1")
rep(cc="FI", grp_rsrv="GC1")
rep(cc="NO", grp_rsrv="GD1")
rep(cc="NO", grp_rsrv="GR1")
rep(cc="NO", grp_rsrv="GL1")
rep(cc="NO", grp_rsrv="GC1")
# new
rep(cc="NO", grp_rsrv="GP1")
# vide
rep(cc="SE", grp_rsrv="GD1")
rep(cc="SE", grp_rsrv="GR1")
rep(cc="SE", grp_rsrv="GL1")
# new
rep(cc="SE", grp_rsrv="GC1")
rep(cc="GR", grp_rsrv="GD1")
rep(cc="GR", grp_rsrv="GR1")
rep(cc="IE", grp_rsrv="GD1")
rep(cc="IE", grp_rsrv="GR1")
rep(cc="IE", grp_rsrv="GL1")
rep(cc="IE", grp_rsrv="GC1")
rep(cc="PL", grp_rsrv="GD1")
rep(cc="PL", grp_rsrv="GR1")
rep(cc="PL", grp_rsrv="GL1")
rep(cc="PL", grp_rsrv="GC1")
rep(cc="PL", grp_rsrv="GP1")
rep(cc="NL", grp_rsrv="GD1")
rep(cc="NL", grp_rsrv="GR1")
rep(cc="TR", grp_rsrv="GD1")
rep(cc="TR", grp_rsrv="GR1")
rep(cc="TR", grp_rsrv="GL1")
rep(cc="ES", grp_rsrv="GD1")
rep(cc="ES", grp_rsrv="GR1")
rep(cc="ES", grp_rsrv="GL1")
rep(cc="ES", grp_rsrv="GC1")
rep(cc="CH", grp_rsrv="GD1")
rep(cc="CH", grp_rsrv="GR1")
rep(cc="CH", grp_rsrv="GC1")
rep(cc="UK", grp_rsrv="GD1")
rep(cc="UK", grp_rsrv="GR1")
rep(cc="UK", grp_rsrv="GL1")
rep(cc="IT", grp_rsrv="GD1")
rep(cc="IT", grp_rsrv="GR1")
rep(cc="IT", grp_rsrv="GL1")
rep(cc="IT", grp_rsrv="GC1")
rep(cc="IT", grp_rsrv="GP1")
rep(cc="CO", grp_rsrv="GR1")
rep(cc="CO", grp_rsrv="GD1")
rep(cc="BE", grp_rsrv="GL1")
rep(cc="BE", grp_rsrv="GD1")
# vide
rep(cc="BE", grp_rsrv="GR1")
# vide
rep(cc="MX", grp_rsrv="GD1")
# vide
rep(cc="MX", grp_rsrv="GR1")
# vide
rep(cc="MX", grp_rsrv="GP1")
rep(cc="AT", grp_rsrv="GL1")
rep(cc="AT", grp_rsrv="GD1")
rep(cc="AT", grp_rsrv="GR1")
rep(cc="AT", grp_rsrv="GP1")