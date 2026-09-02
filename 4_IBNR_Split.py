from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

lreseau = "X"
# Lettre du serveur "Inventprev" attention au majuscule et minuscule
arrete = "2026_06_Prov"
input_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Input"  # LIBNAME input
cr_q226_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"  # LIBNAME CR_Q226
balancedate = "26/06/2026"
balancequarter = "Q22026"
quarter = "2026Q2"
ouput = "CR_Q226"
month = 06
day = 26
yr = 2026
def dataprep(balancedate):
    # *M3: 1 Set up Year-month grid;
    YRMNTH_GRID = spark.createDataFrame([], schema=StructType([]))
    YRMNTH_GRID = (YRMNTH_GRID
        .withColumn('YR', F.expr(f"""year(intnx('month',cast({balancedate} as double),-i))"""))
        .withColumn('MNTH', F.expr(f"""month(intnx('month',cast({balancedate} as double),-i))"""))
    )
    # DO loop: for i in range(0, 95+1, 1):
    # OUTPUT statement — rows already accumulated by PySpark
    # END DO
    YRMNTH_GRID = YRMNTH_GRID.drop('i')
    YRMNTH_GRID.createOrReplaceTempView('YRMNTH_GRID')

    DEVMONTH = spark.createDataFrame([], schema=StructType([]))
    DEVMONTH = (DEVMONTH
        .withColumn('DEVMONTH', F.col('i'))
    )
    # DO loop: for i in range(1, 60+1, 1):
    # OUTPUT statement — rows already accumulated by PySpark
    # END DO
    DEVMONTH = DEVMONTH.drop('i')
    DEVMONTH.createOrReplaceTempView('DEVMONTH')

    YRMNTHDEV = spark.sql("""SELECT DISTINCT YR, MNTH, DEVMONTH
    	FROM YRMNTH_GRID, DEVMONTH""")
    YRMNTHDEV.createOrReplaceTempView('YRMNTHDEV')


dataprep(balancedate)
def ibnr_split(c, rsrv_typ):
    # *DOWNLOAD Development factor and IBNR data FROM EXCEL;
    # %let C=FR;
    # %let Rsrv_Typ=GD1;
    chemin = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/IBNR/Outputs/{quarter}/{c}/IBNR - {c} {rsrv_typ}.xlsx"
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=chemin, out="IBNR_DVLPMNT_FCTRS", onglet="RUNOFF")
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


import_excel(file=chemin, out="IBNR_RSRV_BY_YRMNTH", onglet="IBNRRES")
DEFINE_ROLLING_YR_0 = spark.sql(f"""SELECT UNIQUE Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ
		FROM {ouput}.CLMHDR_ALL_{c} 
		WHERE Rsrv_Grp='{rsrv_typ}'  and Undrwrtng_Cmpny is not null and LEGACY_SCHEME_BOOK='TIA'
		GROUP BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ
		""")
DEFINE_ROLLING_YR_0.createOrReplaceTempView('DEFINE_ROLLING_YR_0')

DEFINE_ROLLING_YR = spark.sql(f"""SELECT DISTINCT Rsrv_Grp, Yr as Acc_Yr, Mnth as Acc_Mnth, Schm, Undrwrtng_Cmpny, Cvr_Typ,
	year(input({balancedate},ddmmyy10.))*12+month(input({balancedate},ddmmyy10.))-Yr*12-Mnth+1 as Dev_Mnth,
	case when Mnth<=Month(input({balancedate},ddmmyy10.)) then Yr else Yr+1 end as Roll_Yr
	FROM YRMNTH_GRID, DEFINE_ROLLING_YR_0""")
DEFINE_ROLLING_YR.createOrReplaceTempView('DEFINE_ROLLING_YR')

_dfs[f'CLMHDR_ALL_{c}_{rsrv_typ}'] = spark.table(f'{ouput}.CLMHDR_ALL_{c}')
_dfs[f'CLMHDR_ALL_{c}_{rsrv_typ}'] = _dfs[f'CLMHDR_ALL_{c}_{rsrv_typ}'].filter(F.col('Rsrv_Grp') == f"{rsrv_typ}' AND LEGACY_SCHEME_BOOK='TIA")
_dfs[f'CLMHDR_ALL_{c}_{rsrv_typ}'].createOrReplaceTempView(f'CLMHDR_ALL_{c}_{rsrv_typ}')

CLMNS_INCRD_BY_SUB_GRP = spark.sql(f"""SELECT DISTINCT d.Rsrv_Grp, d.Acc_Yr, d.Acc_Mnth, d.Dev_Mnth, d.Roll_Yr, d.Schm, d.Undrwrtng_Cmpny, d.Cvr_Typ,
	sum(case when Totl_Amnt_Pd+Rsrv_Amt Is Null then 0 else Totl_Amnt_Pd+Rsrv_Amt end) as Clms_Incrd
	FROM CLMHDR_ALL_{c}_{rsrv_typ} h
	RIGHT JOIN DEFINE_ROLLING_YR d ON (h.Rsrv_Grp = d.Rsrv_Grp
	and h.Acc_Yr = d.Acc_Yr
	and h.Acc_Mnth = d.Acc_Mnth
	and h.Schm = d.Schm
	and h.Undrwrtng_Cmpny = d.Undrwrtng_Cmpny
	and h.Cvr_Typ = d.Cvr_Typ)
	GROUP BY d.Rsrv_Grp, d.Acc_Yr, d.Acc_Mnth, d.Dev_Mnth, d.Roll_Yr, d.Schm, d.Undrwrtng_Cmpny, d.Cvr_Typ""")
CLMNS_INCRD_BY_SUB_GRP.createOrReplaceTempView('CLMNS_INCRD_BY_SUB_GRP')

CHN_LDDR_IBNR_BY_SUB_GRP = spark.sql("""SELECT DISTINCT c.Rsrv_Grp, c.Acc_Yr, c.Acc_Mnth, c.Schm, c.Undrwrtng_Cmpny, c.Cvr_Typ, c.Clms_Incrd, c.Dev_Mnth,
	c.Roll_Yr, i.Cum_Per_Rgstrd,
	case when i.Cum_Per_Rgstrd = .  then 0 else case when i.Cum_Per_Rgstrd<0.33 then 0 
	else (c.Clms_Incrd/i.Cum_Per_Rgstrd)*(1-i.Cum_Per_Rgstrd) end end as CLIbnr_SubGrp
	FROM CLMNS_INCRD_BY_SUB_GRP c
	LEFT JOIN IBNR_DVLPMNT_FCTRS i ON (c.Dev_Mnth = i.Dev_Mnth)""")
CHN_LDDR_IBNR_BY_SUB_GRP.createOrReplaceTempView('CHN_LDDR_IBNR_BY_SUB_GRP')

Chn_lddr_ibnr_tot = spark.sql(f"""SELECT Rsrv_Grp, sum(CLIbnr_SubGrp) as CLIbnr
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.))
	GROUP BY Rsrv_Grp""")
Chn_lddr_ibnr_tot.createOrReplaceTempView('Chn_lddr_ibnr_tot')

Chn_lddr_ibnr_tot_nry = spark.sql(f"""SELECT Rsrv_Grp, sum(CLIbnr_SubGrp) as CLIbnr_nry
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.)) or Roll_Yr = (Year(input({balancedate},ddmmyy10.))-1)
	GROUP BY Rsrv_Grp""")
Chn_lddr_ibnr_tot_nry.createOrReplaceTempView('Chn_lddr_ibnr_tot_nry')

Chn_lddr_ibnr_sub_grp_tot = spark.sql(f"""SELECT Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ, sum(CLIbnr_SubGrp) as CLIbnr_SubGrp_Tot
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.))
	GROUP BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ""")
Chn_lddr_ibnr_sub_grp_tot.createOrReplaceTempView('Chn_lddr_ibnr_sub_grp_tot')

Chn_lddr_ibnr_sub_grp_tot_nry = spark.sql(f"""SELECT Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ, sum(CLIbnr_SubGrp) as CLIbnr_SubGrp_Tot_nry
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.)) or Roll_Yr = (Year(input({balancedate},ddmmyy10.))-1)
	GROUP BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ""")
Chn_lddr_ibnr_sub_grp_tot_nry.createOrReplaceTempView('Chn_lddr_ibnr_sub_grp_tot_nry')

Chn_lddr_ibnr_yrmnth = spark.sql("""SELECT Rsrv_Grp, Acc_Yr, Acc_Mnth, sum(CLIbnr_SubGrp) as CLIbnr_YrMnth
	FROM Chn_lddr_ibnr_by_sub_grp
	GROUP BY Rsrv_Grp, Acc_Yr, Acc_Mnth""")
Chn_lddr_ibnr_yrmnth.createOrReplaceTempView('Chn_lddr_ibnr_yrmnth')

CLMHDRwithPolYr = spark.sql(f"""SELECT *, Year(Incptn_Dt) as PolYr
		FROM {ouput}.CLMHDR_ALL_{c}
		where Rsrv_Grp='{rsrv_typ}' and LEGACY_SCHEME_BOOK='TIA'
		""")
CLMHDRwithPolYr.createOrReplaceTempView('CLMHDRwithPolYr')

PolYrs = spark.sql(f"""SELECT DISTINCT Year(Incptn_Dt) as PolYr
		FROM {ouput}.CLMHDR_ALL_{c}
		where Rsrv_Grp='{rsrv_typ}' and LEGACY_SCHEME_BOOK='TIA'
		ORDER BY PolYr
		""")
PolYrs.createOrReplaceTempView('PolYrs')

DEFINE_ROLLING_YR_0 = spark.sql("""SELECT DISTINCT Country,Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ
		FROM CLMHDRwithPolYr
		ORDER BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ
		""")
DEFINE_ROLLING_YR_0.createOrReplaceTempView('DEFINE_ROLLING_YR_0')

DEFINE_ROLLING_YR = spark.sql(f"""SELECT DISTINCT Country,Rsrv_Grp, Yr as Acc_Yr, Mnth as Acc_Mnth, PolYr,
	Schm, Undrwrtng_Cmpny,Cvr_Typ,
	year(input({balancedate},ddmmyy10.))*12+month(input({balancedate},ddmmyy10.))-Yr*12-Mnth+1 as Dev_Mnth,
	case when Mnth<=Month(input({balancedate},ddmmyy10.)) then Yr else Yr+1 end as Roll_Yr
	FROM YRMNTH_GRID, DEFINE_ROLLING_YR_0, PolYrs
	""")
DEFINE_ROLLING_YR.createOrReplaceTempView('DEFINE_ROLLING_YR')

CLMNS_INCRD_BY_SUB_GRP = spark.sql("""SELECT DISTINCT d.Country,d.Rsrv_Grp, d.Acc_Yr, d.Acc_Mnth, d.Dev_Mnth, d.Roll_Yr, d.Schm, d.Undrwrtng_Cmpny, d.Cvr_Typ, d.PolYr,
	sum(case when Totl_Amnt_Pd+Rsrv_Amt Is Null then 0 else Totl_Amnt_Pd+Rsrv_Amt end) as Clms_Incrd
	FROM ClmhdrwithPolYr h
	RIGHT JOIN DEFINE_ROLLING_YR d ON (h.Rsrv_Grp = d.Rsrv_Grp
	and h.Acc_Yr = d.Acc_Yr
	and h.Acc_Mnth = d.Acc_Mnth
	and h.Schm = d.Schm
	and h.Undrwrtng_Cmpny = d.Undrwrtng_Cmpny
	and h.Cvr_Typ = d.Cvr_Typ
	and h.PolYr = d.PolYr)
	GROUP BY d.Rsrv_Grp, d.Acc_Yr, d.Acc_Mnth, d.Dev_Mnth, d.Roll_Yr, d.Schm, d.Undrwrtng_Cmpny, d.Cvr_Typ, d.PolYr""")
CLMNS_INCRD_BY_SUB_GRP.createOrReplaceTempView('CLMNS_INCRD_BY_SUB_GRP')

CHN_LDDR_IBNR_BY_SUB_GRP = spark.sql("""SELECT DISTINCT c.Country,c.Rsrv_Grp, c.Acc_Yr, c.Acc_Mnth, c.Schm, c.Undrwrtng_Cmpny, c.Cvr_Typ, c.Clms_Incrd, c.Dev_Mnth,
	c.Roll_Yr, c.PolYr ,i.Cum_Per_Rgstrd,
	case when i.Cum_Per_Rgstrd = .  then 0 else case when i.Cum_Per_Rgstrd<0.33 then 0 
	else (c.Clms_Incrd/i.Cum_Per_Rgstrd)*(1-i.Cum_Per_Rgstrd) end end as CLIbnr_SubGrp
	FROM CLMNS_INCRD_BY_SUB_GRP c
	LEFT JOIN IBNR_DVLPMNT_FCTRS i ON (c.Dev_Mnth = i.Dev_Mnth)""")
CHN_LDDR_IBNR_BY_SUB_GRP.createOrReplaceTempView('CHN_LDDR_IBNR_BY_SUB_GRP')

Chn_lddr_ibnr_sub_grp_tot = spark.sql(f"""SELECT Country,Rsrv_Grp, Schm, PolYr, Undrwrtng_Cmpny, Cvr_Typ, sum(CLIbnr_SubGrp) as CLIbnr_SubGrp_Tot
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.))
	GROUP BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ, PolYr""")
Chn_lddr_ibnr_sub_grp_tot.createOrReplaceTempView('Chn_lddr_ibnr_sub_grp_tot')

Chn_lddr_ibnr_sub_grp_tot_nry = spark.sql(f"""SELECT Country,Rsrv_Grp, Schm, PolYr, Undrwrtng_Cmpny, Cvr_Typ, sum(CLIbnr_SubGrp) as CLIbnr_SubGrp_Tot_nry
	FROM Chn_lddr_ibnr_by_sub_grp
	WHERE Roll_Yr = Year(input({balancedate},ddmmyy10.)) or Roll_Yr = (Year(input({balancedate},ddmmyy10.))-1)
	GROUP BY Rsrv_Grp, Schm, Undrwrtng_Cmpny, Cvr_Typ, PolYr""")
Chn_lddr_ibnr_sub_grp_tot_nry.createOrReplaceTempView('Chn_lddr_ibnr_sub_grp_tot_nry')

_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1'] = spark.table('CHN_LDDR_IBNR_BY_SUB_GRP')
_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1'] = (_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1']
    .withColumn('rc', F.expr("""chek.instr()"""))
    .withColumn('rc2', F.expr("""test.instr()"""))
    .withColumn('rc3', F.expr("""test1.instr()"""))
    .withColumn('rc4', F.expr("""test2.instr()"""))
    .withColumn('rc5', F.expr("""test3.instr()"""))
    .withColumn('rc6', F.expr("""test4.instr()"""))
)
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Ibnr_rsrv_by_yrmnth(keep = Rsrv_Grp Acc_Yr Acc_Mnth Ibnr_YrMnth ) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH chek (dataset: 'Ibnr_rsrv_by_yrmnth(keep = Rsrv_Grp Acc_Yr Acc_Mnth Ibnr_YrMnth)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       chek.definekey ('Rsrv_Grp', 'Acc_Yr', 'Acc_Mnth' ) ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       chek.definedata ('Ibnr_YrMnth') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       chek.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Chn_lddr_ibnr_tot(keep = Rsrv_Grp CLIbnr) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH test (dataset: 'Chn_lddr_ibnr_tot(keep = Rsrv_Grp CLIbnr)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       test.definekey ('Rsrv_Grp') ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       test.definedata ('CLIbnr') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       test.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Chn_lddr_ibnr_sub_grp_tot(keep = Rsrv_Grp Schm Undrwrtng_Cmpny Cvr_Typ PolYr CLIbnr_SubGrp_Tot) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH test1 (dataset: 'Chn_lddr_ibnr_sub_grp_tot(keep = Rsrv_Grp Schm Undrwrtng_Cmpny Cvr_Typ PolYr CLIbnr_SubGrp_Tot)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       test1.definekey ('Rsrv_Grp', 'Schm', 'Undrwrtng_Cmpny', 'Cvr_Typ', 'PolYr') ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       test1.definedata ('CLIbnr_SubGrp_Tot') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       test1.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Chn_lddr_ibnr_yrmnth(keep = Rsrv_Grp Acc_Yr Acc_Mnth CLIbnr_YrMnth) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH test2 (dataset: 'Chn_lddr_ibnr_yrmnth(keep = Rsrv_Grp Acc_Yr Acc_Mnth CLIbnr_YrMnth)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       test2.definekey ('Rsrv_Grp', 'Acc_Yr', 'Acc_Mnth') ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       test2.definedata ('CLIbnr_YrMnth') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       test2.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Chn_lddr_ibnr_tot_nry(keep = Rsrv_Grp CLIbnr_nry) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH test3 (dataset: 'Chn_lddr_ibnr_tot_nry(keep = Rsrv_Grp CLIbnr_nry)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       test3.definekey ('Rsrv_Grp') ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       test3.definedata ('CLIbnr_nry') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       test3.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# IF/THEN (manual review needed):
#   If _n_ = 1 then do ;
#       IF 0 then set Chn_lddr_ibnr_sub_grp_tot_nry(keep = Rsrv_Grp Schm Undrwrtng_Cmpny Cvr_Typ PolYr CLIbnr_SubGrp_Tot_nry) ;
#       __COMMENT__:obligatorie ;
#       DCL HASH test4 (dataset: 'Chn_lddr_ibnr_sub_grp_tot_nry(keep = Rsrv_Grp Schm Undrwrtng_Cmpny Cvr_Typ PolYr CLIbnr_SubGrp_Tot_nry)') ;
#       __COMMENT__:Obligatoire aussi : j'ai mis "keep" ici pour que tu n'oublis pas de le mettre toi aussi ;
#       test4.definekey ('Rsrv_Grp' ,'Schm', 'Undrwrtng_Cmpny', 'Cvr_Typ', 'PolYr') ;
#       __COMMENT__:On demande à sas de monter en RAM la table qui servira à faire le rechercheV, ne pas oublier le même KEEP ;
#       test4.definedata ('CLIbnr_SubGrp_Tot_nry') ;
#       __COMMENT__:les clés de réunion, entre quote, séparées par des virgules ;
#       test4.definedone() ;
#       __COMMENT__:Les données à retourner. On peut en mettre plusieurs !! Entre quote et séparées par des virgules ;
#   End ;
# obligatoire
# Table de travail
# On demande d'aller rechercher les infos dans notre table de Hashing
# On demande d'aller rechercher les infos dans notre table de Hashing
# On demande d'aller rechercher les infos dans notre table de Hashing
# On demande d'aller rechercher les infos dans notre table de Hashing
# On demande d'aller rechercher les infos dans notre table de Hashing
# On demande d'aller rechercher les infos dans notre table de Hashing
_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1'] = _dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1'].drop('rc', 'rc2', 'rc3', 'rc4', 'rc5', 'rc6')
_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1'].createOrReplaceTempView(f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1')

_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}'] = spark.sql(f"""SELECT DISTINCT Country, Rsrv_Grp, Schm, PolYr, Undrwrtng_Cmpny, Cvr_Typ, CLIbnr_SubGrp_Tot, Acc_Yr, Acc_Mnth, Dev_Mnth ,Roll_Yr,CLIbnr, Cum_Per_Rgstrd, CLIbnr_SubGrp, CLIbnr_YrMnth,CLIbnr_SubGrp_Tot_nry, CLIbnr_nry, Ibnr_YrMnth,
	case 
	when CLIbnr_YrMnth <> 0 then (CLIbnr_SubGrp/CLIbnr_YrMnth)*Ibnr_YrMnth else case 
	when CLIbnr <> 0 then (CLIbnr_SubGrp_Tot/CLIbnr)*Ibnr_YrMnth 
	else (CLIbnr_SubGrp_Tot_nry/CLIbnr_nry)*Ibnr_YrMnth 
	end end as Ibnr_SubGrp
	FROM IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}_1 
	WHERE case
	when CLIbnr_YrMnth <> 0 then (CLIbnr_SubGrp/CLIbnr_YrMnth)*Ibnr_YrMnth else  case
	when CLIbnr <> 0 then (CLIbnr_SubGrp_Tot/CLIbnr)*Ibnr_YrMnth 
	else (CLIbnr_SubGrp_Tot_nry/CLIbnr_nry)*Ibnr_YrMnth 
	end end >= 0.01""")
_dfs[f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}'].createOrReplaceTempView(f'IBNR_BY_SUB_GRP_{rsrv_typ}_PolYrs_{c}')

mend()
def ibnr_fusion_fr(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GD2_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GD3_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR2_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR3_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL2_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GP1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_1(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GC1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_2(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_3(pays):
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}').unionByName(spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), allowMissingColumns=True)
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_5(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GC1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_4(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GC1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_de(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GP1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_0(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GP1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GC1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_at(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GP1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_be(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GL1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_mx(pays):
    from functools import reduce
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), spark.table(f'IBNR_BY_SUB_GRP_GP1_PolYrs_{pays}')])
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


def ibnr_fusion_co(pays):
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = spark.table(f'IBNR_BY_SUB_GRP_GD1_PolYrs_{pays}').unionByName(spark.table(f'IBNR_BY_SUB_GRP_GR1_PolYrs_{pays}'), allowMissingColumns=True)
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = (_dfs[f'{pays}_IBNR_PolYrsplit_all']
        .withColumn('Date_of_reserving', F.lit(f'{balancequarter}'))
        .withColumn('Rsrv_Typ', F.lit('IBNR'))
        .withColumn('Type_Insurance', F.lit(0))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (1,2,3)"""), F.expr("""concat(Acc_Yr,"Q1")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (4,5,6)"""), F.expr("""concat(Acc_Yr,"Q2")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (7,8,9)"""), F.expr("""concat(Acc_Yr,"Q3")""")))
        .withColumn('Incident_Quarter', F.when(F.expr("""Acc_Mnth IN (10,11,12)"""), F.expr("""concat(Acc_Yr,"Q4")""")))
    )
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Cvr_Typ', 'Cover')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('PolYr', 'Vintage_year')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Schm', 'Scheme')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Ibnr_SubGrp', 'Rsrv_Amt')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'] = _dfs[f'{pays}_IBNR_PolYrsplit_all'].withColumnRenamed('Undrwrtng_Cmpny', 'Entity_CD')
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].createOrReplaceTempView(f'{pays}_IBNR_PolYrsplit_all')
    # LIBNAME {ouput} -> base Spark: {ouput}.{pays}_IBNR_PolYrsplit_all
    _dfs[f'{pays}_IBNR_PolYrsplit_all'].write.mode('overwrite').saveAsTable(f'{ouput}.{pays}_IBNR_PolYrsplit_all')

    _dfs[f'IBNR_{pays}'] = spark.sql(f"""SELECT country,
                        Rsrv_Grp,
                        Scheme,
                        Type_Insurance,
                        cover,
                        Entity_CD,
                        Incident_Quarter,
                        Vintage_year,
                        Date_of_reserving,
                        Rsrv_Typ,
                        Rsrv_Amt as Rsrv_Amt 
                                                             
         FROM    {ouput}.{pays}_IBNR_PolYrsplit_all
         
          """)
    _dfs[f'IBNR_{pays}'].createOrReplaceTempView(f'IBNR_{pays}')


# POLAND
ibnr_split(c="PL", rsrv_typ="GD1")
ibnr_split(c="PL", rsrv_typ="GR1")
ibnr_split(c="PL", rsrv_typ="GL1")
ibnr_split(c="PL", rsrv_typ="GC1")
ibnr_split(c="PL", rsrv_typ="GP1")
ibnr_fusion_0(pays="PL")
# ITALY
ibnr_split(c="IT", rsrv_typ="GD1")
ibnr_split(c="IT", rsrv_typ="GR1")
ibnr_split(c="IT", rsrv_typ="GL1")
ibnr_split(c="IT", rsrv_typ="GC1")
ibnr_split(c="IT", rsrv_typ="GP1")
ibnr_fusion_0(pays="IT")
# DENMARK
ibnr_split(c="DK", rsrv_typ="GD1")
ibnr_split(c="DK", rsrv_typ="GR1")
ibnr_split(c="DK", rsrv_typ="GL1")
ibnr_fusion_2(pays="DK")
# GERMANY
ibnr_split(c="DE", rsrv_typ="GD1")
ibnr_split(c="DE", rsrv_typ="GR1")
ibnr_split(c="DE", rsrv_typ="GL1")
ibnr_split(c="DE", rsrv_typ="GP1")
ibnr_fusion_de(pays="DE")
# IRELAND
ibnr_split(c="IE", rsrv_typ="GD1")
ibnr_split(c="IE", rsrv_typ="GR1")
ibnr_split(c="IE", rsrv_typ="GL1")
ibnr_split(c="IE", rsrv_typ="GC1")
ibnr_fusion_1(pays="IE")
# NETHERLAND
ibnr_split(c="NL", rsrv_typ="GD1")
ibnr_split(c="NL", rsrv_typ="GR1")
ibnr_fusion_3(pays="NL")
# GREECE
ibnr_split(c="GR", rsrv_typ="GD1")
ibnr_split(c="GR", rsrv_typ="GR1")
# %IBNR_SPLIT(c=GR,Rsrv_Typ=GL1) ;
ibnr_fusion_3(pays="GR")
# Traitement spécial pour la grèce
IBNR_GR = spark.table(f'{ouput}.IBNR_GR')
IBNR_GR = (IBNR_GR
    .withColumn('Rsrv_Amt', F.when(F.expr("""Country = 'GR' AND Scheme IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
)
IBNR_GR.createOrReplaceTempView('IBNR_GR')
# LIBNAME {ouput} -> base Spark: {ouput}.IBNR_GR
IBNR_GR.write.mode('overwrite').saveAsTable(f'{ouput}.IBNR_GR')

GR_IBNR_POLYRSPLIT_ALL = spark.table(f'{ouput}.GR_IBNR_POLYRSPLIT_ALL')
GR_IBNR_POLYRSPLIT_ALL = (GR_IBNR_POLYRSPLIT_ALL
    .withColumn('Rsrv_Amt', F.when(F.expr("""Country = 'GR' AND Scheme IN ('BP3.3', 'BP3.4', 'BP5.3', 'BP5.4', 'BP7.3', 'BP7.4')"""), F.lit(0)))
)
GR_IBNR_POLYRSPLIT_ALL.createOrReplaceTempView('GR_IBNR_POLYRSPLIT_ALL')
# LIBNAME {ouput} -> base Spark: {ouput}.GR_IBNR_POLYRSPLIT_ALL
GR_IBNR_POLYRSPLIT_ALL.write.mode('overwrite').saveAsTable(f'{ouput}.GR_IBNR_POLYRSPLIT_ALL')

# Fin traitement spécial pour la grèce
# PORTUGAL
ibnr_split(c="PT", rsrv_typ="GD1")
ibnr_split(c="PT", rsrv_typ="GR1")
ibnr_split(c="PT", rsrv_typ="GL1")
ibnr_fusion_2(pays="PT")
# SWEDEN
ibnr_split(c="SE", rsrv_typ="GD1")
ibnr_split(c="SE", rsrv_typ="GR1")
ibnr_split(c="SE", rsrv_typ="GL1")
ibnr_split(c="SE", rsrv_typ="GC1")
ibnr_fusion_1(pays="SE")
# FINLAND
ibnr_split(c="FI", rsrv_typ="GC1")
ibnr_split(c="FI", rsrv_typ="GD1")
ibnr_split(c="FI", rsrv_typ="GR1")
ibnr_split(c="FI", rsrv_typ="GL1")
ibnr_fusion_1(pays="FI")
# FRANCE
ibnr_split(c="FR", rsrv_typ="GD1")
ibnr_split(c="FR", rsrv_typ="GD2")
ibnr_split(c="FR", rsrv_typ="GD3")
ibnr_split(c="FR", rsrv_typ="GR1")
ibnr_split(c="FR", rsrv_typ="GR2")
ibnr_split(c="FR", rsrv_typ="GR3")
ibnr_split(c="FR", rsrv_typ="GL1")
ibnr_split(c="FR", rsrv_typ="GL2")
ibnr_split(c="FR", rsrv_typ="GP1")
ibnr_fusion_fr(pays="FR")
# NORWAY
ibnr_split(c="NO", rsrv_typ="GD1")
ibnr_split(c="NO", rsrv_typ="GR1")
ibnr_split(c="NO", rsrv_typ="GL1")
ibnr_split(c="NO", rsrv_typ="GC1")
ibnr_fusion_1(pays="NO")
# TURKEY
ibnr_split(c="TR", rsrv_typ="GD1")
ibnr_split(c="TR", rsrv_typ="GR1")
ibnr_split(c="TR", rsrv_typ="GL1")
ibnr_fusion_2(pays="TR")
# SPAIN
ibnr_split(c="ES", rsrv_typ="GC1")
ibnr_split(c="ES", rsrv_typ="GD1")
ibnr_split(c="ES", rsrv_typ="GR1")
ibnr_split(c="ES", rsrv_typ="GL1")
ibnr_fusion_1(pays="ES")
# SWITERLAND
ibnr_split(c="CH", rsrv_typ="GD1")
ibnr_split(c="CH", rsrv_typ="GR1")
ibnr_split(c="CH", rsrv_typ="GC1")
ibnr_fusion_4(pays="CH")
# UK
ibnr_split(c="UK", rsrv_typ="GD1")
ibnr_split(c="UK", rsrv_typ="GR1")
ibnr_split(c="UK", rsrv_typ="GL1")
ibnr_fusion_2(pays="UK")
# COLOMBIA
ibnr_split(c="CO", rsrv_typ="GD1")
ibnr_split(c="CO", rsrv_typ="GR1")
ibnr_fusion_co(pays="CO")
# AUSTRIA
ibnr_split(c="AT", rsrv_typ="GD1")
ibnr_split(c="AT", rsrv_typ="GL1")
ibnr_split(c="AT", rsrv_typ="GR1")
ibnr_split(c="AT", rsrv_typ="GP1")
ibnr_fusion_at(pays="AT")
# MEXICO
ibnr_split(c="MX", rsrv_typ="GD1")
ibnr_split(c="MX", rsrv_typ="GR1")
ibnr_split(c="MX", rsrv_typ="GP1")
ibnr_fusion_mx(pays="MX")
# BELGIUM
ibnr_split(c="BE", rsrv_typ="GD1")
ibnr_split(c="BE", rsrv_typ="GR1")
ibnr_split(c="BE", rsrv_typ="GL1")
ibnr_fusion_be(pays="BE")
# NORTHEN IRELAND
# %IBNR_SPLIT(c=NI,Rsrv_Typ=GD1) ;
# %IBNR_SPLIT(c=NI,Rsrv_Typ=GR1) ;
# %IBNR_SPLIT(c=NI,Rsrv_Typ=GL1) ;
# %IBNR_FUSION_2(pays=NI) ;
from functools import reduce
wps_daap_IBNR_POLYRSPLIT_ALL = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{ouput}.PT_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.DE_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.DK_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.FR_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.GR_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.IE_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.NL_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.NO_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.PL_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.SE_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.TR_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.CH_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.ES_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.FI_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.IT_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.UK_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.CO_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.AT_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.MX_IBNR_POLYRSPLIT_ALL'), spark.table(f'{ouput}.BE_IBNR_POLYRSPLIT_ALL')])
wps_daap_IBNR_POLYRSPLIT_ALL.createOrReplaceTempView('wps_daap_IBNR_POLYRSPLIT_ALL')
# LIBNAME {ouput} -> base Spark: {ouput}.wps_daap_IBNR_POLYRSPLIT_ALL
wps_daap_IBNR_POLYRSPLIT_ALL.write.mode('overwrite').saveAsTable(f'{ouput}.wps_daap_IBNR_POLYRSPLIT_ALL')

from functools import reduce
_dfs[f'wps_daap_ibnr_{yr}{month}{day}_G'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{ouput}.IBNR_PT'), spark.table(f'{ouput}.IBNR_DE'), spark.table(f'{ouput}.IBNR_DK'), spark.table(f'{ouput}.IBNR_FR'), spark.table(f'{ouput}.IBNR_GR'), spark.table(f'{ouput}.IBNR_IE'), spark.table(f'{ouput}.IBNR_NL'), spark.table(f'{ouput}.IBNR_NO'), spark.table(f'{ouput}.IBNR_PL'), spark.table(f'{ouput}.IBNR_SE'), spark.table(f'{ouput}.IBNR_TR'), spark.table(f'{ouput}.IBNR_CH'), spark.table(f'{ouput}.IBNR_ES'), spark.table(f'{ouput}.IBNR_FI'), spark.table(f'{ouput}.IBNR_IT'), spark.table(f'{ouput}.IBNR_UK'), spark.table(f'{ouput}.IBNR_CO'), spark.table(f'{ouput}.IBNR_AT'), spark.table(f'{ouput}.IBNR_MX'), spark.table(f'{ouput}.IBNR_BE')])
_dfs[f'wps_daap_ibnr_{yr}{month}{day}_G'].createOrReplaceTempView(f'wps_daap_ibnr_{yr}{month}{day}_G')
# LIBNAME {ouput} -> base Spark: {ouput}.wps_daap_ibnr_{yr}{month}{day}_G
_dfs[f'wps_daap_ibnr_{yr}{month}{day}_G'].write.mode('overwrite').saveAsTable(f'{ouput}.wps_daap_ibnr_{yr}{month}{day}_G')

# #################################################################################################################################################################################
# ######################################################   CALCUL DU NET INSURANCE : CASE RESERVES & IBNR    #####################################################
# ################################################################################################################################################################################
import_02 = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/Reassurance.xlsx"
def import_excel(file, out, onglet):
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

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}_G')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Undrwrtng_Cmpny', F.expr("""Entity_CD*1"""))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.sql(f"""select 
t1.*,
t8.QP_rei_CLAIM AS QP_rei_CLAIM 
from WPS_DAAP_IBNR_{yr}{month}{day}  t1 
left join Parametres_Reas t8 on (t1.country=t8.country AND t1.scheme=t8.scheme AND t1.cover=t8.cover AND t1.Entity_CD=t8.Original_underwritter) 
 """)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM IS NULL"""), F.lit(0)))
    .withColumn('Type_Insurance', F.when(F.expr("""QP_rei_CLAIM NOT IN (.)"""), F.lit(4)))
    .withColumn('QP_rei_CLAIM', F.when(F.expr("""Type_Insurance=0"""), F.lit(1)))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].withColumnRenamed('Rsrv_Amt', 'Rsrv_Amt_Gross')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Rsrv_Amt_Net', F.expr("""Rsrv_Amt_Gross*QP_rei_CLAIM"""))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].select('country', 'Rsrv_Grp', 'Scheme', 'Type_Insurance', 'Cover', 'Entity_CD', 'Entity', 'Incident_Quarter', 'Vintage_year', 'Date_of_reserving', 'Rsrv_Typ', 'Rsrv_Amt_Gross', 'Rsrv_Amt_Net')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.sql(f"""select DISTINCT
t1.*,
t8.Entity AS Entity 
from WPS_DAAP_IBNR_{yr}{month}{day}  t1 
left join {ouput}.WPS_DAAP_CASE_RESERVES_{yr}{month}{day} t8 on (t1.country=t8.country AND t1.scheme=t8.scheme AND t1.cover=t8.cover AND t1.Entity_CD=t8.Entity_CD) 
 """)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

# RETAIN variables (initial values): {'country': '0', 'Rsrv_Grp': '0', 'Scheme': '0', 'Type_Insurance': '0', 'Cover': '0', 'Entity_CD': '0', 'Entity': '0', 'Incident_Quarter': '0', 'Vintage_year': '0', 'Date_of_reserving': '0', 'Rsrv_Typ': '0', 'Rsrv_Amt_Gross': '0', 'Rsrv_Amt_Net': '0'}
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = _dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].select('country', 'Rsrv_Grp', 'Scheme', 'Type_Insurance', 'Cover', 'Entity_CD', 'Entity', 'Incident_Quarter', 'Vintage_year', 'Date_of_reserving', 'Rsrv_Typ', 'Rsrv_Amt_Gross', 'Rsrv_Amt_Net')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.sql(f"""SELECT distinct country,
                    Rsrv_Grp,
                    Scheme,
                    Type_Insurance,
                    cover,
                    Entity_CD,
                    Entity,
                    Incident_Quarter,
                    Vintage_year,
                    Date_of_reserving,
                    Rsrv_Typ,
                    sum(Rsrv_Amt_Gross) as Rsrv_Amt_Gross,
                    sum(Rsrv_Amt_Net) as Rsrv_Amt_Net                                       
     FROM    WPS_DAAP_IBNR_{yr}{month}{day}
     group by country, Rsrv_Grp,Scheme,Type_Insurance, cover,Entity_CD,Entity,Incident_Quarter,Vintage_year,Date_of_reserving, Rsrv_Typ
      """)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')

_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = spark.table(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'] = (_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}']
    .withColumn('Entity', F.when(F.expr("""Entity='UNKNOWN' AND Cover IN ('LL','LR')"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""Entity='UNKNOWN' AND Cover IN ('RU','RR')"""), F.lit('FICL')))
    .withColumn('Entity_CD', F.when(F.expr("""Entity_CD='' AND Cover IN ('LL','LR')"""), F.lit('102')))
    .withColumn('Entity_CD', F.when(F.expr("""Entity_CD='' AND Cover IN ('RU','RR')"""), F.lit('101')))
    .withColumn('Entity', F.when(F.expr("""Entity='UNKNOWN' AND Entity_CD='792'"""), F.lit('FACL')))
    .withColumn('Entity', F.when(F.expr("""Entity='UNKNOWN' AND Entity_CD='801'"""), F.lit('FICL')))
)
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].createOrReplaceTempView(f'WPS_DAAP_IBNR_{yr}{month}{day}')
# LIBNAME {ouput} -> base Spark: {ouput}.WPS_DAAP_IBNR_{yr}{month}{day}
_dfs[f'WPS_DAAP_IBNR_{yr}{month}{day}'].write.mode('overwrite').saveAsTable(f'{ouput}.WPS_DAAP_IBNR_{yr}{month}{day}')
