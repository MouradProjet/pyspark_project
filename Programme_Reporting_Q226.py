from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
spark = SparkSession.builder.getOrCreate()
_dfs = {}  # container for DataFrames with dynamic names (macro variables)

# ######## Name: Reporting TIA claims
# ######## Author: GNANISSO SARE
# ######## Date started :17/03/2025
# ######## Date finished:------------
# ######## Context: Construction des inputs de le reporting des claims
# ####################################################################################################################
# ########################################Les 4 last quarters ################################################
# ########################################################################################################################
lreseau = "~/NAS/X"
n1 = 2025
n2 = 2026
nb1 = 1
nb2 = 2
nb3 = 3
nb4 = 4
vision1 = "Q325"
vision2 = "Q425"
vision3 = "Q126"
vision4 = "Q226"
quarter1 = "2025_09_Q4"
quarter2 = "2025_12_Prov"
quarter3 = "2026_04_V2"
quarter4 = "2026_06_Prov"
q1 = "2025Q3"
q2 = "2025Q4"
q3 = "2026Q1"
q4 = "2026Q2"
output1 = "CR_Q325"
output2 = "CR_Q425"
output3 = "CR_Q126"
output4 = "CR_Q226"
month1 = 08
day1 = 29
yr1 = 2025
month2 = 12
day2 = 26
yr2 = 2025
month3 = 03
day3 = 27
yr3 = 2026
month4 = 06
day4 = 26
yr4 = 2026
ym_sup1 = 202508
ym_inf1 = 202408
ym_sup2 = 202512
ym_inf2 = 202412
ym_sup3 = 202603
ym_inf3 = 202503
ym_sup4 = 202606
ym_inf4 = 202506
def import_excel(file, out, onglet):
        _df_tmp = (spark.read.format('com.crealytics.spark.excel')
            .option('dataAddress', f'{onglet}!A1')
            .option('header', 'true')
            .load(file))
        _df_tmp.createOrReplaceTempView(out)


def biblio(output, quarter, nb):
    import_excel(file=f"{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2026_04_V2/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Model Properties/SDB.xlsx", out=f"flag_legacy_{nb}", onglet="flag_legacy")

biblio(output=output1, quarter=quarter1, nb=nb1)
biblio(output=output2, quarter=quarter2, nb=nb2)
biblio(output=output3, quarter=quarter3, nb=nb3)
biblio(output=output4, quarter=quarter4, nb=nb4)
def cover_import(nb):
    _dfs[f'Ref_Cover_{nb}'] = spark.sql(f"""select distinct cover as covmd_cover_code, cover_name
    from Tia_{nb}.carto_tia""")
    _dfs[f'Ref_Cover_{nb}'].createOrReplaceTempView(f'Ref_Cover_{nb}')

    _dfs[f'Ref_Cover_{nb}'] = spark.table(f'Ref_Cover_{nb}')
    _dfs[f'Ref_Cover_{nb}'] = (_dfs[f'Ref_Cover_{nb}']
        .withColumn('cover_name', F.when(F.expr("""covmd_cover_code ='DT'"""), F.lit('Disability')))
    )
    _dfs[f'Ref_Cover_{nb}'].createOrReplaceTempView(f'Ref_Cover_{nb}')


cover_import(nb=nb1)
cover_import(nb=nb2)
cover_import(nb=nb3)
cover_import(nb=nb4)
import_00 = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/01_Mappings/Mapping cover TIA.xlsx"
import_01 = "~/NAS/X/08.Progammes/INTERNATIONAL/01_Equipe CLP-ALHIS/01_General/Taux de change/2026/Fx - Reel Mai 2026.xlsx"
import_02 = "~/NAS/X/08.Progammes/INTERNATIONAL/10_TABLE_ID/Table_ID.xlsx"
import_excel(file=import_00, out="Mappings", onglet="Feuil1")
import_excel(file=import_01, out="exchange", onglet="DALI Reel Exercice")
import_excel(file=import_02, out="TABLEID", onglet="Table ID")
codes_devises = spark.createDataFrame([], schema=StructType([]))
codes_devises.createOrReplaceTempView('codes_devises')

tauxdechange = spark.sql("""select a.COUNTRY, a.devise, b.YTD_VALUE
from codes_devises a
left join EXCHANGE b
on a.DEVISE = b.CURRENCY2""")
tauxdechange.createOrReplaceTempView('tauxdechange')

# ####################################################################################################################
# ########################################QUARTERLY RESERVES SUMMARY################################################
# ########################################################################################################################
def reserves_summary(output, quarter, yr, month, day, nb, vision):
    _dfs[f'wps_daap_case_reserves_{vision}'] = spark.table(f'{output}.wps_daap_case_reserves_{yr}{month}{day}')
    _dfs[f'wps_daap_case_reserves_{vision}'] = (_dfs[f'wps_daap_case_reserves_{vision}']
        .withColumn('Year', F.expr("""year(Incident_date)"""))
        .withColumn('quarter', F.when(F.expr("""month(Incident_date) IN (1, 2, 3)"""), F.expr("""concat(Year, 'Q1')""")))
        .withColumn('quarter', F.when(F.expr("""month(Incident_date) IN (4, 5, 6)"""), F.expr("""concat(Year, 'Q2')""")))
        .withColumn('quarter', F.when(F.expr("""month(Incident_date) IN (7, 8, 9)"""), F.expr("""concat(Year, 'Q3')""")))
        .withColumn('quarter', F.when(F.expr("""month(Incident_date) IN (10, 11, 12)"""), F.expr("""concat(Year, 'Q4')""")))
    )
    _dfs[f'wps_daap_case_reserves_{vision}'].createOrReplaceTempView(f'wps_daap_case_reserves_{vision}')

    _dfs[f'wps_daap_case_reserves_{vision}'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao AS Flag_Macao
    from wps_daap_case_reserves_{vision}  t1 
    left join FLAG_LEGACY_{nb} t8 on (t1.country=t8.country AND t1.scheme=t8.scheme and t1.Cover=t8.Cover) """)
    _dfs[f'wps_daap_case_reserves_{vision}'].createOrReplaceTempView(f'wps_daap_case_reserves_{vision}')

    _dfs[f'wps_daap_case_reserves_{vision}'] = spark.table(f'wps_daap_case_reserves_{vision}')
    _dfs[f'wps_daap_case_reserves_{vision}'] = (_dfs[f'wps_daap_case_reserves_{vision}']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'wps_daap_case_reserves_{vision}'].createOrReplaceTempView(f'wps_daap_case_reserves_{vision}')

    _dfs[f'wps_daap_case_reserves_{vision}'] = spark.table(f'wps_daap_case_reserves_{vision}')
    _dfs[f'wps_daap_case_reserves_{vision}'] = _dfs[f'wps_daap_case_reserves_{vision}'].drop('RPP', 'Flag_Macao')
    _dfs[f'wps_daap_case_reserves_{vision}'].createOrReplaceTempView(f'wps_daap_case_reserves_{vision}')

    _dfs[f'wps_daap_case_reserves_{vision}'] = spark.table(f'wps_daap_case_reserves_{vision}')
    _dfs[f'wps_daap_case_reserves_{vision}'] = _dfs[f'wps_daap_case_reserves_{vision}'].filter(F.expr("""country NOT IN ('CH') AND LEGACY_SCHEME_BOOK='TIA'"""))
    # la DAAP n'est pas responsable du calcul de la suisse, pour les autres c'est encore du off-system et nous n'avons pas le sign-off de l'IT (données non fiables)
    _dfs[f'wps_daap_case_reserves_{vision}'] = _dfs[f'wps_daap_case_reserves_{vision}'].filter((F.col('Country').isNotNull() & (F.col('Country') != '')))
    _dfs[f'wps_daap_case_reserves_{vision}'].createOrReplaceTempView(f'wps_daap_case_reserves_{vision}')

    _dfs[f'CR_IBNR_{vision}'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao AS Flag_Macao
    from {output}.WPS_DAAP_IBNR_{yr}{month}{day}  t1 
    left join FLAG_LEGACY_{nb} t8 on (t1.country=t8.country AND t1.SCHEME=t8.scheme and t1.cover=t8.Cover) """)
    _dfs[f'CR_IBNR_{vision}'].createOrReplaceTempView(f'CR_IBNR_{vision}')

    _dfs[f'CR_IBNR_{vision}'] = spark.table(f'CR_IBNR_{vision}')
    _dfs[f'CR_IBNR_{vision}'] = (_dfs[f'CR_IBNR_{vision}']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'CR_IBNR_{vision}'].createOrReplaceTempView(f'CR_IBNR_{vision}')

    _dfs[f'CR_IBNR_{vision}'] = spark.table(f'CR_IBNR_{vision}')
    _dfs[f'CR_IBNR_{vision}'] = _dfs[f'CR_IBNR_{vision}'].drop('RPP', 'Flag_Macao')
    _dfs[f'CR_IBNR_{vision}'].createOrReplaceTempView(f'CR_IBNR_{vision}')

    _dfs[f'CR_IBNR_{vision}'] = spark.table(f'CR_IBNR_{vision}')
    _dfs[f'CR_IBNR_{vision}'] = _dfs[f'CR_IBNR_{vision}'].filter(F.expr("""country NOT IN ('CH') AND LEGACY_SCHEME_BOOK='TIA'"""))
    # la DAAP n'est pas responsable du calcul de la suisse, pour les autres c'est encore du off-system et nous n'avons pas le sign-off de l'IT (données non fiables)
    _dfs[f'CR_IBNR_{vision}'] = _dfs[f'CR_IBNR_{vision}'].filter((F.col('Country').isNotNull() & (F.col('Country') != '')))
    _dfs[f'CR_IBNR_{vision}'].createOrReplaceTempView(f'CR_IBNR_{vision}')

    _dfs[f'CR_IBNR_{vision}'] = spark.table(f'CR_IBNR_{vision}')
    _dfs[f'CR_IBNR_{vision}'] = (_dfs[f'CR_IBNR_{vision}']
        .withColumn('quarter', F.col('Incident_Quarter'))
    )
    # quarter=substr(Incident_Quarter,6,1)*1;
    _dfs[f'CR_IBNR_{vision}'].createOrReplaceTempView(f'CR_IBNR_{vision}')

    # La partie Claims Paid
    _dfs[f'CR_PAID_{country}'] = spark.table(f'wps_daap_case_reserves_{vision}')
    _dfs[f'CR_PAID_{country}'] = _dfs[f'CR_PAID_{country}'].filter(F.col('Country') == f"{country}")
    _dfs[f'CR_PAID_{country}'] = (_dfs[f'CR_PAID_{country}']
        .withColumn('GL_TYPE_NO', F.expr("""Entity_CD*1"""))
    )
    _dfs[f'CR_PAID_{country}'] = _dfs[f'CR_PAID_{country}'].select('Country', 'SCHEME', 'cover', 'Year', 'quarter', 'GL_TYPE_NO', 'Entity', 'Totl_Amnt_Pd_Gross')
    _dfs[f'CR_PAID_{country}'].createOrReplaceTempView(f'CR_PAID_{country}')

    _dfs[f'CR_PAID_{country}_2'] = spark.sql(f"""select country, cover,Scheme, Year,quarter, GL_TYPE_NO,Entity, 'PAID' as POSTE ,sum(Totl_Amnt_Pd_Gross) AS MONTANT
    from CR_PAID_{country}
    group by country, cover,Scheme, Year, GL_TYPE_NO,Entity, POSTE,Year,quarter""")
    _dfs[f'CR_PAID_{country}_2'].createOrReplaceTempView(f'CR_PAID_{country}_2')

    _dfs[f'CR_PAID_{country}_3'] = spark.sql(f"""Select distinct a.*,b.partner_sales_name as Agent_Name,c.cover_name 
    From CR_PAID_{country}_2 a 
    Left Join TIA_{nb}.CARTO_TIA b On (a.country=b.Country and a.SCHEME=b.scheme)
    Left Join Ref_Cover_{nb} c On (a.cover=c.covmd_cover_code)""")
    _dfs[f'CR_PAID_{country}_3'].createOrReplaceTempView(f'CR_PAID_{country}_3')

    # La partie  ICOP , RBNP ,IBNR
    _dfs[f'CR_{country}'] = spark.table(f'wps_daap_case_reserves_{vision}')
    _dfs[f'CR_{country}'] = _dfs[f'CR_{country}'].filter(F.expr(f"""Country='{country}' AND Rsrv_Typ IN ('ICOP','RBNP')"""))
    _dfs[f'CR_{country}'] = (_dfs[f'CR_{country}']
        .withColumn('Year', F.expr("""year(Incident_date)"""))
        .withColumn('GL_TYPE_NO', F.expr("""Entity_CD*1"""))
    )
    _dfs[f'CR_{country}'] = _dfs[f'CR_{country}'].select('Country', 'SCHEME', 'cover', 'Year', 'quarter', 'Rsrv_Typ', 'GL_TYPE_NO', 'Entity', 'Rsrv_Amt_Gross')
    _dfs[f'CR_{country}'].createOrReplaceTempView(f'CR_{country}')

    # IBNR
    if {quarter}=2025_04_V2 and {country}=GR:
        PROC IMPORT 
    DATAFILE="~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2025_04_V2/02_Elements_Techniques/TIA/Arrete reel/RESERVES/REPORTING/Datalake/GR/WPS_DAAP_IBNR_20250328_GR.xlsx"
    OUT=GR_IBNR_IMPOR
    DBMS=XLSX
    REPLACE;
    RUN;
    DATA CR_IBNR_&Country.;
    SET GR_IBNR_IMPOR;
    WHERE Country="&Country.";
    Year = input(substr(Incident_Quarter,1,4), 4.);
    GL_TYPE_NO = input(Entity_CD, best.);
    KEEP Country SCHEME cover Year quarter GL_TYPE_NO Entity Rsrv_Typ Rsrv_Amt_Gross;
    RUN;
    # %DO block (non-iterative): %do; 
    data CR_IBNR_&Country.;
    KEEP Country SCHEME cover Year quarter GL_TYPE_NO Entity Rsrv_Typ Rsrv_Amt_Gross  ;
    set CR_IBNR_&vision. ;
    where Country="&Country." ;
    Year=substr(Incident_Quarter,1,4)*1; 
    GL_TYPE_NO= Entity_CD*1 ;
    run ;
    %end;
    _dfs[f'CR_RESERVES_{country}'] = spark.table(f'CR_{country}').unionByName(spark.table(f'CR_IBNR_{country}'), allowMissingColumns=True)
    _dfs[f'CR_RESERVES_{country}'].createOrReplaceTempView(f'CR_RESERVES_{country}')

    _dfs[f'CR_RESERVES_{country}_2'] = spark.sql(f"""select country, cover,Scheme, Year,quarter, GL_TYPE_NO,Entity, Rsrv_Typ as POSTE ,sum(Rsrv_Amt_Gross) AS MONTANT
    from CR_RESERVES_{country}
    group by country, cover,Scheme, Year,quarter, GL_TYPE_NO, POSTE,Entity,Year""")
    _dfs[f'CR_RESERVES_{country}_2'].createOrReplaceTempView(f'CR_RESERVES_{country}_2')

    _dfs[f'CR_RESERVES_{country}_3'] = spark.sql(f"""Select distinct a.*,b.partner_sales_name as Agent_Name,c.cover_name 
    From CR_RESERVES_{country}_2 a 
    Left Join TIA_{nb}.CARTO_TIA b On (a.country=b.Country and a.SCHEME=b.scheme)
    Left Join Ref_Cover_{nb} c On (a.cover=c.covmd_cover_code)""")
    _dfs[f'CR_RESERVES_{country}_3'].createOrReplaceTempView(f'CR_RESERVES_{country}_3')

    _dfs[f'Reserve_Flux_{country}'] = spark.table(f'CR_PAID_{country}_3').unionByName(spark.table(f'CR_RESERVES_{country}_3'), allowMissingColumns=True)
    _dfs[f'Reserve_Flux_{country}'].createOrReplaceTempView(f'Reserve_Flux_{country}')

    _dfs[f'Reserve_Flux_{country}'] = spark.table(f'Reserve_Flux_{country}').orderBy('country', 'cover', 'cover_name', 'Scheme', 'Agent_Name', 'Year', 'quarter', 'GL_TYPE_NO', 'Entity', 'POSTE')
    _dfs[f'Reserve_Flux_{country}'] = _dfs[f'Reserve_Flux_{country}'].dropDuplicates(['country', 'cover', 'cover_name', 'Scheme', 'Agent_Name', 'Year', 'quarter', 'GL_TYPE_NO', 'Entity', 'POSTE'])
    _dfs[f'Reserve_Flux_{country}'].createOrReplaceTempView(f'Reserve_Flux_{country}')

    # PROC TRANSPOSE
    # ID present → long-to-wide pivot
    _dfs[f'Reserve_Flux_{country}'] = _dfs[f'Reserve_Flux_{country}'].groupBy('country', 'cover', 'cover_name', 'Scheme', 'Agent_Name', 'Year', 'quarter', 'GL_TYPE_NO', 'Entity').pivot('POSTE').agg(F.first(F.col('MONTANT')))
    _dfs[f'Reserve_Flux_{country}'].createOrReplaceTempView(f'Reserve_Flux_{country}')

    _dfs[f'Reserve_Flux_{country}_{nb}'] = spark.table(f'Reserve_Flux_{country}')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = (_dfs[f'Reserve_Flux_{country}_{nb}']
        .withColumn('PAID', F.when(F.expr("""PAID IS NULL"""), F.lit(0)))
        .withColumn('IBNR', F.when(F.expr("""IBNR IS NULL"""), F.lit(0)))
        .withColumn('ICOP', F.when(F.expr("""ICOP IS NULL"""), F.lit(0)))
        .withColumn('RBNP', F.when(F.expr("""RBNP IS NULL"""), F.lit(0)))
        .withColumn('Charge_clot', F.expr("""PAID + IBNR + ICOP + RBNP"""))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('DU','ZH','DZ','DY')"""), F.lit('Disability')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('TR','TS')"""), F.lit('Pecuniary Loss')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('RV')"""), F.lit('Unemployment')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('FF')"""), F.lit('Death')))
    )
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].drop('POSTE')
    _dfs[f'Reserve_Flux_{country}_{nb}'].createOrReplaceTempView(f'Reserve_Flux_{country}_{nb}')

    _dfs[f'Reserve_Flux_{country}_{nb}'] = spark.table(f'Reserve_Flux_{country}_{nb}')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].withColumnRenamed('PAID', 'PAID_')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].withColumnRenamed('IBNR', 'IBNR_')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].withColumnRenamed('ICOP', 'ICOP_')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].withColumnRenamed('RBNP', 'RBNP_')
    _dfs[f'Reserve_Flux_{country}_{nb}'] = _dfs[f'Reserve_Flux_{country}_{nb}'].withColumnRenamed('Charge_clot', 'Charge_clot_')
    _dfs[f'Reserve_Flux_{country}_{nb}'].createOrReplaceTempView(f'Reserve_Flux_{country}_{nb}')


claims_extraction(country="GR")
claims_extraction(country="CH")
claims_extraction(country="DK")
claims_extraction(country="ES")
claims_extraction(country="IE")
claims_extraction(country="NI")
claims_extraction(country="NL")
claims_extraction(country="NO")
claims_extraction(country="PT")
claims_extraction(country="TR")
claims_extraction(country="UK")
claims_extraction(country="FI")
claims_extraction(country="BE")
claims_extraction(country="MX")
claims_extraction(country="CO")
claims_extraction(country="AT")
claims_extraction(country="IT")
claims_extraction(country="DE")
claims_extraction(country="PL")
claims_extraction(country="SE")
claims_extraction(country="FR")
from functools import reduce
_dfs[f'Reserves_{quarter}'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'Reserve_Flux_AT_{nb}'), spark.table(f'Reserve_Flux_SE_{nb}'), spark.table(f'Reserve_Flux_IT_{nb}'), spark.table(f'Reserve_Flux_PL_{nb}'), spark.table(f'Reserve_Flux_FR_{nb}'), spark.table(f'Reserve_Flux_DE_{nb}'), spark.table(f'Reserve_Flux_CH_{nb}'), spark.table(f'Reserve_Flux_DK_{nb}'), spark.table(f'Reserve_Flux_FI_{nb}'), spark.table(f'Reserve_Flux_ES_{nb}'), spark.table(f'Reserve_Flux_GR_{nb}'), spark.table(f'Reserve_Flux_IE_{nb}'), spark.table(f'Reserve_Flux_NI_{nb}'), spark.table(f'Reserve_Flux_NL_{nb}'), spark.table(f'Reserve_Flux_NO_{nb}'), spark.table(f'Reserve_Flux_PT_{nb}'), spark.table(f'Reserve_Flux_TR_{nb}'), spark.table(f'Reserve_Flux_UK_{nb}'), spark.table(f'Reserve_Flux_BE_{nb}'), spark.table(f'Reserve_Flux_CO_{nb}'), spark.table(f'Reserve_Flux_MX_{nb}')])
_dfs[f'Reserves_{quarter}'].createOrReplaceTempView(f'Reserves_{quarter}')

mend()
reserves_summary(output=output1, quarter=quarter1, yr=yr1, month=month1, day=day1, nb=nb1, vision=vision1)
reserves_summary(output=output2, quarter=quarter2, yr=yr2, month=month2, day=day2, nb=nb2, vision=vision2)
reserves_summary(output=output3, quarter=quarter3, yr=yr3, month=month3, day=day3, nb=nb3, vision=vision3)
reserves_summary(output=output4, quarter=quarter4, yr=yr4, month=month4, day=day4, nb=nb4, vision=vision4)
FLUX_ALL_QUARTERS = spark.sql(f"""select 
        coalesce(a.Country, b.Country, c.Country, d.Country) as Country ,
        coalesce(a.cover, b.cover, c.cover, d.cover) as cover,
        coalesce(a.cover_name, b.cover_name, c.cover_name, d.cover_name) as cover_name,
        coalesce(a.SCHEME, b.SCHEME, c.SCHEME, d.SCHEME) as SCHEME,
        coalesce(a.Agent_Name, b.Agent_Name, c.Agent_Name, d.Agent_Name) as Agent_Name,
        coalesce(a.Year, b.Year, c.Year, d.Year) as Year,
        coalesce(a.quarter, b.quarter, c.quarter, d.quarter) as quarter,
        coalesce(a.GL_TYPE_NO, b.GL_TYPE_NO, c.GL_TYPE_NO, d.GL_TYPE_NO) as GL_TYPE_NO,
        coalesce(a.Entity, b.Entity, c.Entity, d.Entity) as Entity,
        a.PAID_{vision1}, b.PAID_{vision2}, c.PAID_{vision3}, d.PAID_{vision4},
        a.ICOP_{vision1}, b.ICOP_{vision2}, c.ICOP_{vision3}, d.ICOP_{vision4},
        a.RBNP_{vision1}, b.RBNP_{vision2}, c.RBNP_{vision3}, d.RBNP_{vision4},
        a.IBNR_{vision1}, b.IBNR_{vision2}, c.IBNR_{vision3}, d.IBNR_{vision4},
        a.Charge_clot_{vision1}, b.Charge_clot_{vision2}, c.Charge_clot_{vision3}, d.Charge_clot_{vision4}
    from 
        Reserves_{quarter1} as a
    full join 
        Reserves_{quarter2} as b on a.Country = b.Country and a.cover = b.cover and a.cover_name = b.cover_name and a.SCHEME = b.SCHEME 
        and a.Agent_Name = b.Agent_Name and a.Year = b.Year and a.quarter = b.quarter and a.GL_TYPE_NO = b.GL_TYPE_NO
        and a.Entity=b.Entity
    full join 
        Reserves_{quarter3} as c on a.Country = c.Country and a.cover = c.cover and a.cover_name = c.cover_name and a.SCHEME = c.SCHEME 
        and a.Agent_Name = c.Agent_Name and a.Year = c.Year and a.quarter = c.quarter and a.GL_TYPE_NO = c.GL_TYPE_NO
        and a.Entity=c.Entity
    full join 
        Reserves_{quarter4} as d on a.Country = d.Country and a.cover = d.cover and a.cover_name = d.cover_name and a.SCHEME = d.SCHEME 
        and a.Agent_Name = d.Agent_Name and a.Year = d.Year and a.quarter = d.quarter and a.GL_TYPE_NO = d.GL_TYPE_NO
        and a.Entity=d.Entity""")
FLUX_ALL_QUARTERS.createOrReplaceTempView('FLUX_ALL_QUARTERS')

FLUX_ALL_QUARTERS = spark.table('FLUX_ALL_QUARTERS')
# IF/THEN (manual review needed):
#   if PAID_{vision1} = . then PAID_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if PAID_{vision2} = . then PAID_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if PAID_{vision3} = . then PAID_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if PAID_{vision4} = . then PAID_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if ICOP_{vision1} = . then ICOP_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if ICOP_{vision2} = . then ICOP_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if ICOP_{vision3} = . then ICOP_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if ICOP_{vision4} = . then ICOP_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if RBNP_{vision1} = . then RBNP_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if RBNP_{vision2} = . then RBNP_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if RBNP_{vision3} = . then RBNP_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if RBNP_{vision4} = . then RBNP_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if IBNR_{vision1} = . then IBNR_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if IBNR_{vision2} = . then IBNR_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if IBNR_{vision3} = . then IBNR_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if IBNR_{vision4} = . then IBNR_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if Charge_clot_{vision1} = . then Charge_clot_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if Charge_clot_{vision2} = . then Charge_clot_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if Charge_clot_{vision3} = . then Charge_clot_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if Charge_clot_{vision4} = . then Charge_clot_{vision4} = 0 ;
FLUX_ALL_QUARTERS.createOrReplaceTempView('FLUX_ALL_QUARTERS')

FLUX_ALL_QUARTERS = spark.sql("""select a.*, b.YTD_VALUE
from FLUX_ALL_QUARTERS a
left join tauxdechange b
on a.COUNTRY = b.COUNTRY""")
FLUX_ALL_QUARTERS.createOrReplaceTempView('FLUX_ALL_QUARTERS')

FLUX_ALL_QUARTERS = spark.table('FLUX_ALL_QUARTERS')
FLUX_ALL_QUARTERS = (FLUX_ALL_QUARTERS
    .withColumn(f'PAID_{vision1}', F.expr(f"""PAID_{vision1}*YTD_VALUE"""))
    .withColumn(f'PAID_{vision2}', F.expr(f"""PAID_{vision2}*YTD_VALUE"""))
    .withColumn(f'PAID_{vision3}', F.expr(f"""PAID_{vision3}*YTD_VALUE"""))
    .withColumn(f'PAID_{vision4}', F.expr(f"""PAID_{vision4}*YTD_VALUE"""))
    .withColumn(f'ICOP_{vision1}', F.expr(f"""ICOP_{vision1}*YTD_VALUE"""))
    .withColumn(f'ICOP_{vision2}', F.expr(f"""ICOP_{vision2}*YTD_VALUE"""))
    .withColumn(f'ICOP_{vision3}', F.expr(f"""ICOP_{vision3}*YTD_VALUE"""))
    .withColumn(f'ICOP_{vision4}', F.expr(f"""ICOP_{vision4}*YTD_VALUE"""))
    .withColumn(f'RBNP_{vision1}', F.expr(f"""RBNP_{vision1}*YTD_VALUE"""))
    .withColumn(f'RBNP_{vision2}', F.expr(f"""RBNP_{vision2}*YTD_VALUE"""))
    .withColumn(f'RBNP_{vision3}', F.expr(f"""RBNP_{vision3}*YTD_VALUE"""))
    .withColumn(f'RBNP_{vision4}', F.expr(f"""RBNP_{vision4}*YTD_VALUE"""))
    .withColumn(f'IBNR_{vision1}', F.expr(f"""IBNR_{vision1}*YTD_VALUE"""))
    .withColumn(f'IBNR_{vision2}', F.expr(f"""IBNR_{vision2}*YTD_VALUE"""))
    .withColumn(f'IBNR_{vision3}', F.expr(f"""IBNR_{vision3}*YTD_VALUE"""))
    .withColumn(f'IBNR_{vision4}', F.expr(f"""IBNR_{vision4}*YTD_VALUE"""))
    .withColumn(f'Charge_clot_{vision1}', F.expr(f"""Charge_clot_{vision1}*YTD_VALUE"""))
    .withColumn(f'Charge_clot_{vision2}', F.expr(f"""Charge_clot_{vision2}*YTD_VALUE"""))
    .withColumn(f'Charge_clot_{vision3}', F.expr(f"""Charge_clot_{vision3}*YTD_VALUE"""))
    .withColumn(f'Charge_clot_{vision4}', F.expr(f"""Charge_clot_{vision4}*YTD_VALUE"""))
)
FLUX_ALL_QUARTERS.createOrReplaceTempView('FLUX_ALL_QUARTERS')

# ####################################################################################################################
# ########################################SINISTRES PAYES ACCOUNTING################################################
# ########################################################################################################################
def accounting_payment(nb, q, quarter, vision):
    _dfs[f'{country}_clmhdr'] = spark.table(f'sin_{nb}.{country}_clmhdr')
    _dfs[f'{country}_clmhdr'] = _dfs[f'{country}_clmhdr'].select('Country', 'cla_case_no', 'cover', 'scheme', 'incident_date', 'uw_company', 'Legal_Entity')
    _dfs[f'{country}_clmhdr'].createOrReplaceTempView(f'{country}_clmhdr')

    _dfs[f'{country}_clmhdr'] = spark.table(f'{country}_clmhdr')
    _dfs[f'{country}_clmhdr'] = (_dfs[f'{country}_clmhdr']
        .withColumn('Occurence_month', F.expr("""month(incident_date)"""))
        .withColumn('Occurence_year', F.expr("""year(incident_date)"""))
        .withColumn('quarter_occurrence', F.when(F.expr("""Occurence_month IN (1,2,3)"""), F.expr("""concat(Occurence_year, 'Q1')""")))
        .withColumn('quarter_occurrence', F.when(F.expr("""Occurence_month IN (4,5,6)"""), F.expr("""concat(Occurence_year, 'Q2')""")))
        .withColumn('quarter_occurrence', F.when(F.expr("""Occurence_month IN (7,8,9)"""), F.expr("""concat(Occurence_year, 'Q3')""")))
        .withColumn('quarter_occurrence', F.when(F.expr("""Occurence_month IN (10,11,12)"""), F.expr("""concat(Occurence_year, 'Q4')""")))
    )
    _dfs[f'{country}_clmhdr'].createOrReplaceTempView(f'{country}_clmhdr')

    _dfs[f'{country}_clmtrns'] = spark.table(f'sin_{nb}.{country}_clmtrns')
    _dfs[f'{country}_clmtrns'] = _dfs[f'{country}_clmtrns'].select('Country', 'cla_case_no', 'TRANS_DATE', 'currency_amt', 'gross_amt')
    _dfs[f'{country}_clmtrns'].createOrReplaceTempView(f'{country}_clmtrns')

    _dfs[f'{country}_clmtrns'] = spark.table(f'{country}_clmtrns')
    _dfs[f'{country}_clmtrns'] = (_dfs[f'{country}_clmtrns']
        .withColumn('GL_TYPE_NO', F.col('uw_company'))
        .withColumn('Transaction_month', F.expr("""month(TRANS_DATE)"""))
        .withColumn('Transaction_year', F.expr("""year(TRANS_DATE)"""))
        .withColumn('gross_amt', F.expr("""-gross_amt"""))
        .withColumn('currency_amt', F.expr("""-currency_amt"""))
        .withColumn('quarter_transaction', F.when(F.expr("""Transaction_month IN (1,2,3)"""), F.expr("""concat(Transaction_year, 'Q1')""")))
        .withColumn('quarter_transaction', F.when(F.expr("""Transaction_month IN (4,5,6)"""), F.expr("""concat(Transaction_year, 'Q2')""")))
        .withColumn('quarter_transaction', F.when(F.expr("""Transaction_month IN (7,8,9)"""), F.expr("""concat(Transaction_year, 'Q3')""")))
        .withColumn('quarter_transaction', F.when(F.expr("""Transaction_month IN (10,11,12)"""), F.expr("""concat(Transaction_year, 'Q4')""")))
    )
    _dfs[f'{country}_clmtrns'].createOrReplaceTempView(f'{country}_clmtrns')

    _dfs[f'{country}_aggregated_clmtrns'] = spark.sql(f"""Select distinct a.Country, a.cla_case_no as claim_number, a.Transaction_year, a.quarter_transaction, sum(a.gross_amt) AS Claim_paid
        From {country}_clmtrns a
        Group by a.Country, a.cla_case_no, a.Transaction_year, a.quarter_transaction""")
    _dfs[f'{country}_aggregated_clmtrns'].createOrReplaceTempView(f'{country}_aggregated_clmtrns')

    _dfs[f'{country}_final_clmhdr'] = spark.sql(f"""SELECT a.*, b.Transaction_year, b.quarter_transaction, b.Claim_paid
    FROM {country}_clmhdr AS a
    LEFT JOIN {country}_aggregated_clmtrns AS b
    ON a.cla_case_no = b.claim_number""")
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    # IF/THEN (manual review needed):
    #   IF NOT MISSING(Claim_paid) THEN OUTPUT ;
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.sql(f"""Select distinct a.*,b.partner_sales_name as Agent_name,c.cover_name 
    From {country}_final_clmhdr a 
    Left Join TIA_{nb}.CARTO_TIA b On (a.country=b.Country and a.SCHEME=b.scheme)
    Left Join Ref_Cover_{nb} c On (a.cover=c.covmd_cover_code)""")
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    _dfs[f'{country}_final_clmhdr'] = (_dfs[f'{country}_final_clmhdr']
        .withColumn('cover_name', F.when(F.expr("""cover IN ('DU','ZH','DZ','DY')"""), F.lit('Disability')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('TR','TS')"""), F.lit('Pecuniary Loss')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('RV')"""), F.lit('Unemployment')))
        .withColumn('cover_name', F.when(F.expr("""cover IN ('FF')"""), F.lit('Death')))
    )
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    # IF/THEN (manual review needed):
    #   if quarter_transaction="{q}" ;
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao AS Flag_Macao
    from {country}_final_clmhdr  t1 
    left join FLAG_LEGACY_{nb} t8 on (t1.country=t8.country AND t1.SCHEME=t8.scheme and t1.cover=t8.Cover) """)
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    _dfs[f'{country}_final_clmhdr'] = (_dfs[f'{country}_final_clmhdr']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    _dfs[f'{country}_final_clmhdr'] = _dfs[f'{country}_final_clmhdr'].drop('RPP', 'Flag_Macao')
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    _dfs[f'{country}_final_clmhdr'] = _dfs[f'{country}_final_clmhdr'].filter(F.expr("""country NOT IN ('CH') AND LEGACY_SCHEME_BOOK='TIA'"""))
    # la DAAP n'est pas responsable du calcul de la suisse, pour les autres c'est encore du off-system et nous n'avons pas le sign-off de l'IT (données non fiables)
    _dfs[f'{country}_final_clmhdr'] = _dfs[f'{country}_final_clmhdr'].filter((F.col('Country').isNotNull() & (F.col('Country') != '')))
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr'] = spark.table(f'{country}_final_clmhdr')
    _dfs[f'{country}_final_clmhdr'] = (_dfs[f'{country}_final_clmhdr']
        .withColumn('GL_TYPE_NO', F.col('uw_company'))
        .withColumn('Entity', F.col('Legal_Entity'))
    )
    _dfs[f'{country}_final_clmhdr'].createOrReplaceTempView(f'{country}_final_clmhdr')

    _dfs[f'{country}_final_clmhdr_{nb}'] = spark.sql(f"""select country, cover, cover_name, Scheme,Agent_Name,Occurence_year,quarter_occurrence,Transaction_year ,quarter_transaction,GL_TYPE_NO,Entity,LEGACY_SCHEME_BOOK,sum(Claim_paid) AS Claim_paid
    from {country}_final_clmhdr
    group by country, cover, cover_name, Scheme,Agent_Name,Occurence_year,quarter_occurrence,Transaction_year ,quarter_transaction,GL_TYPE_NO,Entity,LEGACY_SCHEME_BOOK""")
    _dfs[f'{country}_final_clmhdr_{nb}'].createOrReplaceTempView(f'{country}_final_clmhdr_{nb}')


recuperation_claims(country="GR")
recuperation_claims(country="CH")
recuperation_claims(country="DK")
recuperation_claims(country="ES")
recuperation_claims(country="IE")
recuperation_claims(country="NI")
recuperation_claims(country="NL")
recuperation_claims(country="NO")
recuperation_claims(country="PT")
recuperation_claims(country="TR")
recuperation_claims(country="UK")
recuperation_claims(country="FI")
recuperation_claims(country="BE")
recuperation_claims(country="MX")
recuperation_claims(country="CO")
recuperation_claims(country="AT")
recuperation_claims(country="IT")
recuperation_claims(country="DE")
recuperation_claims(country="PL")
recuperation_claims(country="SE")
recuperation_claims(country="FR")
# Étape 1 : Création de la nouvelle variable Entity_New
from functools import reduce
_dfs[f'Claims_{quarter}_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'GR_final_clmhdr_{nb}'), spark.table(f'CH_final_clmhdr_{nb}'), spark.table(f'DK_final_clmhdr_{nb}'), spark.table(f'ES_final_clmhdr_{nb}'), spark.table(f'IE_final_clmhdr_{nb}'), spark.table(f'NI_final_clmhdr_{nb}'), spark.table(f'NL_final_clmhdr_{nb}'), spark.table(f'NO_final_clmhdr_{nb}'), spark.table(f'PT_final_clmhdr_{nb}'), spark.table(f'TR_final_clmhdr_{nb}'), spark.table(f'UK_final_clmhdr_{nb}'), spark.table(f'FI_final_clmhdr_{nb}'), spark.table(f'BE_final_clmhdr_{nb}'), spark.table(f'MX_final_clmhdr_{nb}'), spark.table(f'CO_final_clmhdr_{nb}'), spark.table(f'AT_final_clmhdr_{nb}'), spark.table(f'IT_final_clmhdr_{nb}'), spark.table(f'DE_final_clmhdr_{nb}'), spark.table(f'PL_final_clmhdr_{nb}'), spark.table(f'SE_final_clmhdr_{nb}'), spark.table(f'FR_final_clmhdr_{nb}')])
_dfs[f'Claims_{quarter}_all'].createOrReplaceTempView(f'Claims_{quarter}_all')

_dfs[f'Claims_{quarter}_all'] = spark.table(f'Claims_{quarter}_all')
_dfs[f'Claims_{quarter}_all'] = _dfs[f'Claims_{quarter}_all'].withColumnRenamed('Claim_paid', 'Claim_paid_')
_dfs[f'Claims_{quarter}_all'].createOrReplaceTempView(f'Claims_{quarter}_all')

mend()
accounting_payment(nb=nb1, q=q1, quarter=quarter1, vision=vision1)
accounting_payment(nb=nb2, q=q2, quarter=quarter2, vision=vision2)
accounting_payment(nb=nb3, q=q3, quarter=quarter3, vision=vision3)
accounting_payment(nb=nb4, q=q4, quarter=quarter4, vision=vision4)
CLAIMS_PAID_ALL_QUARTERS = spark.sql(f"""select 
        coalesce(a.Country, b.Country, c.Country, d.Country) as Country ,
        coalesce(a.cover, b.cover, c.cover, d.cover) as cover,
        coalesce(a.cover_name, b.cover_name, c.cover_name, d.cover_name) as cover_name,
        coalesce(a.SCHEME, b.SCHEME, c.SCHEME, d.SCHEME) as SCHEME,
        coalesce(a.Agent_Name, b.Agent_Name, c.Agent_Name, d.Agent_Name) as Agent_Name,
        coalesce(a.Occurence_year, b.Occurence_year, c.Occurence_year, d.Occurence_year) as Occurence_year,
        coalesce(a.quarter_occurrence, b.quarter_occurrence, c.quarter_occurrence, d.quarter_occurrence) as quarter_occurrence,
        coalesce(a.Transaction_year, b.Transaction_year, c.Transaction_year, d.Transaction_year) as Transaction_year,
        coalesce(a.quarter_transaction, b.quarter_transaction, c.quarter_transaction, d.quarter_transaction) as quarter_transaction,
        coalesce(a.GL_TYPE_NO, b.GL_TYPE_NO, c.GL_TYPE_NO, d.GL_TYPE_NO) as GL_TYPE_NO,
        coalesce(a.Entity, b.Entity, c.Entity, d.Entity) as Entity,
        a.Claim_paid_{vision1}, b.Claim_paid_{vision2}, c.Claim_paid_{vision3}, d.Claim_paid_{vision4}
    from 
        Claims_{quarter1}_all as a
    full join 
        Claims_{quarter2}_all as b on a.Country = b.Country and a.cover = b.cover and a.cover_name = b.cover_name and a.SCHEME = b.SCHEME 
        and a.Agent_Name = b.Agent_Name and a.Occurence_year=b.Occurence_year and a.quarter_occurrence = b.quarter_occurrence and a.Transaction_year = b.Transaction_year and a.quarter_transaction = b.quarter_transaction and a.GL_TYPE_NO = b.GL_TYPE_NO
        and a.Entity=b.Entity
    full join 
        Claims_{quarter3}_all as c on a.Country = c.Country and a.cover = c.cover and a.cover_name = c.cover_name and a.SCHEME = c.SCHEME 
        and a.Agent_Name = c.Agent_Name and a.Occurence_year=c.Occurence_year and a.quarter_occurrence = c.quarter_occurrence and a.Transaction_year = c.Transaction_year and a.quarter_transaction = c.quarter_transaction and a.GL_TYPE_NO = c.GL_TYPE_NO
        and a.Entity=c.Entity
    full join 
        Claims_{quarter4}_all as d on a.Country = d.Country and a.cover = d.cover and a.cover_name = d.cover_name and a.SCHEME = d.SCHEME 
        and a.Agent_Name = d.Agent_Name and a.Occurence_year=d.Occurence_year and a.quarter_occurrence = d.quarter_occurrence and a.Transaction_year = d.Transaction_year and a.quarter_transaction = d.quarter_transaction and a.GL_TYPE_NO = d.GL_TYPE_NO
        and a.Entity=d.Entity""")
CLAIMS_PAID_ALL_QUARTERS.createOrReplaceTempView('CLAIMS_PAID_ALL_QUARTERS')

CLAIMS_PAID_ALL_QUARTERS = spark.table('CLAIMS_PAID_ALL_QUARTERS')
# IF/THEN (manual review needed):
#   if Claim_paid_{vision1} = . then Claim_paid_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision2} = . then Claim_paid_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision3} = . then Claim_paid_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision4} = . then Claim_paid_{vision4} = 0 ;
CLAIMS_PAID_ALL_QUARTERS.createOrReplaceTempView('CLAIMS_PAID_ALL_QUARTERS')

CLAIMS_PAID_ALL_QUARTERS = spark.sql("""select a.*, b.YTD_VALUE
from CLAIMS_PAID_ALL_QUARTERS a
left join tauxdechange b
on a.COUNTRY = b.COUNTRY""")
CLAIMS_PAID_ALL_QUARTERS.createOrReplaceTempView('CLAIMS_PAID_ALL_QUARTERS')

CLAIMS_PAID_ALL_QUARTERS = spark.table('CLAIMS_PAID_ALL_QUARTERS')
CLAIMS_PAID_ALL_QUARTERS = (CLAIMS_PAID_ALL_QUARTERS
    .withColumn(f'Claim_paid_{vision1}', F.expr(f"""Claim_paid_{vision1}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision2}', F.expr(f"""Claim_paid_{vision2}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision3}', F.expr(f"""Claim_paid_{vision3}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision4}', F.expr(f"""Claim_paid_{vision4}*YTD_VALUE"""))
)
CLAIMS_PAID_ALL_QUARTERS.createOrReplaceTempView('CLAIMS_PAID_ALL_QUARTERS')

# ####################################################################################################################
# ########################################Focus on ICOP and RBNP################################################
# ########################################################################################################################
def categorisation(pays, output_f, output_i, nb):
    _dfs[f'liste_{pays}_{output_i}'] = spark.sql(f"""select distinct Clm_Nmbr
    from {output_i}.CLMHDR_ALL_{pays}""")
    _dfs[f'liste_{pays}_{output_i}'].createOrReplaceTempView(f'liste_{pays}_{output_i}')

    _dfs[f'CLMHDR_ALL_{pays}_{nb}'] = spark.sql(f"""select distinct a.*, 
    case when b.Clm_Nmbr is not null then 'Stock'
    else 'New' 
    end as claims_type
    from {output_f}.CLMHDR_ALL_{pays} as a
    left join liste_{pays}_{output_i} as b
    on a.Clm_Nmbr = b.Clm_Nmbr""")
    _dfs[f'CLMHDR_ALL_{pays}_{nb}'].createOrReplaceTempView(f'CLMHDR_ALL_{pays}_{nb}')


categorisation(pays="GR", output_f=output2, output_i=output1, nb=nb2)
categorisation(pays="CH", output_f=output2, output_i=output1, nb="2")
categorisation(pays="DK", output_f=output2, output_i=output1, nb="2")
categorisation(pays="ES", output_f=output2, output_i=output1, nb="2")
categorisation(pays="IE", output_f=output2, output_i=output1, nb="2")
categorisation(pays="NI", output_f=output2, output_i=output1, nb="2")
categorisation(pays="NL", output_f=output2, output_i=output1, nb="2")
categorisation(pays="NO", output_f=output2, output_i=output1, nb="2")
categorisation(pays="PT", output_f=output2, output_i=output1, nb="2")
categorisation(pays="TR", output_f=output2, output_i=output1, nb="2")
categorisation(pays="UK", output_f=output2, output_i=output1, nb="2")
categorisation(pays="FI", output_f=output2, output_i=output1, nb="2")
categorisation(pays="BE", output_f=output2, output_i=output1, nb="2")
categorisation(pays="MX", output_f=output2, output_i=output1, nb="2")
categorisation(pays="CO", output_f=output2, output_i=output1, nb="2")
categorisation(pays="AT", output_f=output2, output_i=output1, nb="2")
categorisation(pays="IT", output_f=output2, output_i=output1, nb="2")
categorisation(pays="DE", output_f=output2, output_i=output1, nb="2")
categorisation(pays="PL", output_f=output2, output_i=output1, nb="2")
categorisation(pays="SE", output_f=output2, output_i=output1, nb="2")
categorisation(pays="FR", output_f=output2, output_i=output1, nb="2")
categorisation(pays="GR", output_f=output3, output_i=output2, nb="3")
categorisation(pays="CH", output_f=output3, output_i=output2, nb="3")
categorisation(pays="DK", output_f=output3, output_i=output2, nb="3")
categorisation(pays="ES", output_f=output3, output_i=output2, nb="3")
categorisation(pays="IE", output_f=output3, output_i=output2, nb="3")
categorisation(pays="NI", output_f=output3, output_i=output2, nb="3")
categorisation(pays="NL", output_f=output3, output_i=output2, nb="3")
categorisation(pays="NO", output_f=output3, output_i=output2, nb="3")
categorisation(pays="PT", output_f=output3, output_i=output2, nb="3")
categorisation(pays="TR", output_f=output3, output_i=output2, nb="3")
categorisation(pays="UK", output_f=output3, output_i=output2, nb="3")
categorisation(pays="FI", output_f=output3, output_i=output2, nb="3")
categorisation(pays="BE", output_f=output3, output_i=output2, nb="3")
categorisation(pays="MX", output_f=output3, output_i=output2, nb="3")
categorisation(pays="CO", output_f=output3, output_i=output2, nb="3")
categorisation(pays="AT", output_f=output3, output_i=output2, nb="3")
categorisation(pays="IT", output_f=output3, output_i=output2, nb="3")
categorisation(pays="DE", output_f=output3, output_i=output2, nb="3")
categorisation(pays="PL", output_f=output3, output_i=output2, nb="3")
categorisation(pays="SE", output_f=output3, output_i=output2, nb="3")
categorisation(pays="FR", output_f=output3, output_i=output2, nb="3")
categorisation(pays="GR", output_f=output4, output_i=output3, nb="4")
categorisation(pays="CH", output_f=output4, output_i=output3, nb="4")
categorisation(pays="DK", output_f=output4, output_i=output3, nb="4")
categorisation(pays="ES", output_f=output4, output_i=output3, nb="4")
categorisation(pays="IE", output_f=output4, output_i=output3, nb="4")
categorisation(pays="NI", output_f=output4, output_i=output3, nb="4")
categorisation(pays="NL", output_f=output4, output_i=output3, nb="4")
categorisation(pays="NO", output_f=output4, output_i=output3, nb="4")
categorisation(pays="PT", output_f=output4, output_i=output3, nb="4")
categorisation(pays="TR", output_f=output4, output_i=output3, nb="4")
categorisation(pays="UK", output_f=output4, output_i=output3, nb="4")
categorisation(pays="FI", output_f=output4, output_i=output3, nb="4")
categorisation(pays="BE", output_f=output4, output_i=output3, nb="4")
categorisation(pays="MX", output_f=output4, output_i=output3, nb="4")
categorisation(pays="CO", output_f=output4, output_i=output3, nb="4")
categorisation(pays="AT", output_f=output4, output_i=output3, nb="4")
categorisation(pays="IT", output_f=output4, output_i=output3, nb="4")
categorisation(pays="DE", output_f=output4, output_i=output3, nb="4")
categorisation(pays="PL", output_f=output4, output_i=output3, nb="4")
categorisation(pays="SE", output_f=output4, output_i=output3, nb="4")
categorisation(pays="FR", output_f=output4, output_i=output3, nb="4")
def regroupement(quarter, nb):
    from functools import reduce
    _dfs[f'CLMHDR_{quarter}_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'CLMHDR_ALL_GR_{nb}'), spark.table(f'CLMHDR_ALL_CH_{nb}'), spark.table(f'CLMHDR_ALL_DK_{nb}'), spark.table(f'CLMHDR_ALL_ES_{nb}'), spark.table(f'CLMHDR_ALL_IE_{nb}'), spark.table(f'CLMHDR_ALL_NI_{nb}'), spark.table(f'CLMHDR_ALL_NL_{nb}'), spark.table(f'CLMHDR_ALL_NO_{nb}'), spark.table(f'CLMHDR_ALL_PT_{nb}'), spark.table(f'CLMHDR_ALL_TR_{nb}'), spark.table(f'CLMHDR_ALL_UK_{nb}'), spark.table(f'CLMHDR_ALL_FI_{nb}'), spark.table(f'CLMHDR_ALL_BE_{nb}'), spark.table(f'CLMHDR_ALL_MX_{nb}'), spark.table(f'CLMHDR_ALL_CO_{nb}'), spark.table(f'CLMHDR_ALL_AT_{nb}'), spark.table(f'CLMHDR_ALL_IT_{nb}'), spark.table(f'CLMHDR_ALL_DE_{nb}'), spark.table(f'CLMHDR_ALL_PL_{nb}'), spark.table(f'CLMHDR_ALL_SE_{nb}'), spark.table(f'CLMHDR_ALL_FR_{nb}')])
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    # IF/THEN (manual review needed):
    #   IF Rsrv_Typ in ("ICOP","RBNP") ;
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.sql(f"""select distinct
    t1.*,
    t8.RPP AS RPP,
    t8.Flag_Macao AS Flag_Macao
    from CLMHDR_{quarter}_all  t1 
    left join FLAG_LEGACY_{nb} t8 on (t1.country=t8.country AND t1.Schm=t8.Scheme and t1.Cvr_Typ=t8.Cover) """)
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    _dfs[f'CLMHDR_{quarter}_all'] = (_dfs[f'CLMHDR_{quarter}_all']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    _dfs[f'CLMHDR_{quarter}_all'] = _dfs[f'CLMHDR_{quarter}_all'].drop('RPP', 'Flag_Macao')
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    # IF/THEN (manual review needed):
    #   IF LEGACY_SCHEME_BOOK="TIA" ;
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    _dfs[f'CLMHDR_{quarter}_all'] = _dfs[f'CLMHDR_{quarter}_all'].drop('SCHEME')
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')


regroupement(quarter=quarter2, nb=nb2)
regroupement(quarter=quarter3, nb=nb3)
regroupement(quarter=quarter4, nb=nb4)
from functools import reduce
_dfs[f'CLMHDR_{quarter1}_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'{output1}.CLMHDR_ALL_GR'), spark.table(f'{output1}.CLMHDR_ALL_CH'), spark.table(f'{output1}.CLMHDR_ALL_DK'), spark.table(f'{output1}.CLMHDR_ALL_ES'), spark.table(f'{output1}.CLMHDR_ALL_IE'), spark.table(f'{output1}.CLMHDR_ALL_NI'), spark.table(f'{output1}.CLMHDR_ALL_NL'), spark.table(f'{output1}.CLMHDR_ALL_NO'), spark.table(f'{output1}.CLMHDR_ALL_PT'), spark.table(f'{output1}.CLMHDR_ALL_TR'), spark.table(f'{output1}.CLMHDR_ALL_UK'), spark.table(f'{output1}.CLMHDR_ALL_FI'), spark.table(f'{output1}.CLMHDR_ALL_BE'), spark.table(f'{output1}.CLMHDR_ALL_MX'), spark.table(f'{output1}.CLMHDR_ALL_CO'), spark.table(f'{output1}.CLMHDR_ALL_AT'), spark.table(f'{output1}.CLMHDR_ALL_IT'), spark.table(f'{output1}.CLMHDR_ALL_DE'), spark.table(f'{output1}.CLMHDR_ALL_PL'), spark.table(f'{output1}.CLMHDR_ALL_SE'), spark.table(f'{output1}.CLMHDR_ALL_FR')])
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.table(f'CLMHDR_{quarter1}_all')
# IF/THEN (manual review needed):
#   IF Rsrv_Typ in ("ICOP","RBNP") ;
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.sql(f"""select distinct
t1.*,
t8.RPP AS RPP,
t8.Flag_Macao AS Flag_Macao
from CLMHDR_{quarter1}_all  t1 
left join FLAG_LEGACY_{nb1} t8 on (t1.country=t8.country AND t1.Schm=t8.scheme and t1.Cvr_Typ=t8.Cover) """)
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.table(f'CLMHDR_{quarter1}_all')
_dfs[f'CLMHDR_{quarter1}_all'] = (_dfs[f'CLMHDR_{quarter1}_all']
    .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
    .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
)
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.table(f'CLMHDR_{quarter1}_all')
_dfs[f'CLMHDR_{quarter1}_all'] = _dfs[f'CLMHDR_{quarter1}_all'].drop('RPP', 'Flag_Macao')
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.table(f'CLMHDR_{quarter1}_all')
# IF/THEN (manual review needed):
#   IF LEGACY_SCHEME_BOOK="TIA" ;
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

_dfs[f'CLMHDR_{quarter1}_all'] = spark.table(f'CLMHDR_{quarter1}_all')
_dfs[f'CLMHDR_{quarter1}_all'] = _dfs[f'CLMHDR_{quarter1}_all'].drop('SCHEME')
_dfs[f'CLMHDR_{quarter1}_all'].createOrReplaceTempView(f'CLMHDR_{quarter1}_all')

def partner_added(quarter, nb):
    _dfs[f'CLMHDR_{quarter}_all'] = spark.sql(f"""Select distinct a.*,b.partner_sales_name as Agent_Name,c.cover_name 
    From CLMHDR_{quarter}_all a 
    Left Join TIA_{nb}.CARTO_TIA b On (a.country=b.Country and a.Schm=b.scheme)
    Left Join Ref_Cover_{nb} c On (a.Cvr_Typ=c.covmd_cover_code)""")
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    _dfs[f'CLMHDR_{quarter}_all'] = (_dfs[f'CLMHDR_{quarter}_all']
        .withColumn('cover_name', F.when(F.expr("""Cvr_Typ IN ('DU','ZH','DZ','DY')"""), F.lit('Disability')))
        .withColumn('cover_name', F.when(F.expr("""Cvr_Typ IN ('TR','TS')"""), F.lit('Pecuniary Loss')))
        .withColumn('cover_name', F.when(F.expr("""Cvr_Typ IN ('RV')"""), F.lit('Unemployment')))
        .withColumn('cover_name', F.when(F.expr("""Cvr_Typ IN ('FF')"""), F.lit('Death')))
    )
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.sql(f"""select a.*, b.YTD_VALUE
    from CLMHDR_{quarter}_all a
    left join tauxdechange b
    on a.COUNTRY = b.COUNTRY""")
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')

    _dfs[f'CLMHDR_{quarter}_all'] = spark.table(f'CLMHDR_{quarter}_all')
    _dfs[f'CLMHDR_{quarter}_all'] = (_dfs[f'CLMHDR_{quarter}_all']
        .withColumn('Totl_Amnt_Pd', F.expr("""Totl_Amnt_Pd*YTD_VALUE"""))
        .withColumn('Totl_Bnfts_Amnt_Pd', F.expr("""Totl_Bnfts_Amnt_Pd*YTD_VALUE"""))
        .withColumn('Mnthly_Bnft', F.expr("""Mnthly_Bnft*YTD_VALUE"""))
        .withColumn('Rsrv_Amt', F.expr("""Rsrv_Amt*YTD_VALUE"""))
        .withColumn('Otstndng_Balnc', F.expr("""Otstndng_Balnc*YTD_VALUE"""))
        .withColumn('POTENTIAL_CLM_AMT', F.expr("""POTENTIAL_CLM_AMT*YTD_VALUE"""))
    )
    _dfs[f'CLMHDR_{quarter}_all'].createOrReplaceTempView(f'CLMHDR_{quarter}_all')


partner_added(quarter=quarter1, nb=nb1)
partner_added(quarter=quarter2, nb=nb2)
partner_added(quarter=quarter3, nb=nb3)
partner_added(quarter=quarter4, nb=nb4)
# ####################################################################################################################
# ########################################GEP  SIDE################################################
# ########################################################################################################################
def recuperation_gep(quarter, nb, ym_sup, ym_inf, vision):
    from functools import reduce
    _dfs[f'GEP_FLUX_{quarter}_all'] = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [spark.table(f'gep_{nb}.HISTO_FLUX_GR'), spark.table(f'gep_{nb}.HISTO_FLUX_CH'), spark.table(f'gep_{nb}.HISTO_FLUX_DK'), spark.table(f'gep_{nb}.HISTO_FLUX_ES'), spark.table(f'gep_{nb}.HISTO_FLUX_IE'), spark.table(f'gep_{nb}.HISTO_FLUX_NI'), spark.table(f'gep_{nb}.HISTO_FLUX_NL'), spark.table(f'gep_{nb}.HISTO_FLUX_NO'), spark.table(f'gep_{nb}.HISTO_FLUX_PT'), spark.table(f'gep_{nb}.HISTO_FLUX_TR'), spark.table(f'gep_{nb}.HISTO_FLUX_UK'), spark.table(f'gep_{nb}.HISTO_FLUX_FI'), spark.table(f'gep_{nb}.HISTO_FLUX_BE'), spark.table(f'gep_{nb}.HISTO_FLUX_MX'), spark.table(f'gep_{nb}.HISTO_FLUX_CO'), spark.table(f'gep_{nb}.HISTO_FLUX_AT'), spark.table(f'gep_{nb}.HISTO_FLUX_IT'), spark.table(f'gep_{nb}.HISTO_FLUX_DE'), spark.table(f'gep_{nb}.HISTO_FLUX_PL'), spark.table(f'gep_{nb}.HISTO_FLUX_SE'), spark.table(f'gep_{nb}.HISTO_FLUX_FR')])
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.sql(f"""select distinct
    t1.*,
    
    t8.Flag_Macao AS Flag_Macao
    from GEP_FLUX_{quarter}_all  t1 
    left join FLAG_LEGACY_{nb} t8 on (t1.country=t8.country AND t1.scheme=t8.scheme and t1.cover=t8.Cover) """)
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.table(f'GEP_FLUX_{quarter}_all')
    _dfs[f'GEP_FLUX_{quarter}_all'] = (_dfs[f'GEP_FLUX_{quarter}_all']
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao='MACAO'"""), F.lit('MACAO')))
        .withColumn('LEGACY_SCHEME_BOOK', F.when(F.expr("""Flag_Macao IN ('TIA','')"""), F.lit('TIA')))
    )
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.table(f'GEP_FLUX_{quarter}_all')
    _dfs[f'GEP_FLUX_{quarter}_all'] = _dfs[f'GEP_FLUX_{quarter}_all'].drop('Flag_Macao')
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.table(f'GEP_FLUX_{quarter}_all')
    _dfs[f'GEP_FLUX_{quarter}_all'] = _dfs[f'GEP_FLUX_{quarter}_all'].filter(F.expr(f"""Month >='{ym_inf}' AND Month <='{ym_sup}' AND LEGACY_SCHEME_BOOK='TIA'"""))
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    # Proc SQL; Create Table GEP_FLUX_&quarter._all As
    # Select distinct a.*,c.cover_name
    # From GEP_FLUX_&quarter._all a
    # Left Join TIA_&nb..CARTO_TIA b On (a.country=b.Country and a.scheme=b.scheme)
    # Left Join Ref_Cover_&nb. c
    # On (a.Cover=c.covmd_cover_code);
    # Quit;
    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.sql(f"""Select distinct a.*,b.partner_sales_name as Agent_Name,c.cover_name 
    From GEP_FLUX_{quarter}_all a 
    Left Join TIA_{nb}.CARTO_TIA b On (a.country=b.Country and a.scheme=b.scheme)
    Left Join Ref_Cover_{nb} c On (a.cover=c.covmd_cover_code)""")
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.table(f'GEP_FLUX_{quarter}_all')
    _dfs[f'GEP_FLUX_{quarter}_all'] = (_dfs[f'GEP_FLUX_{quarter}_all']
        .withColumn('cover_name', F.when(F.expr("""Cover IN ('DU','ZH','DZ','DY')"""), F.lit('Disability')))
        .withColumn('cover_name', F.when(F.expr("""Cover IN ('TR','TS')"""), F.lit('Pecuniary Loss')))
        .withColumn('cover_name', F.when(F.expr("""Cover IN ('RV')"""), F.lit('Unemployment')))
        .withColumn('cover_name', F.when(F.expr("""Cover IN ('FF')"""), F.lit('Death')))
    )
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')

    _dfs[f'GEP_FLUX_{quarter}_all'] = spark.table(f'GEP_FLUX_{quarter}_all')
    _dfs[f'GEP_FLUX_{quarter}_all'] = _dfs[f'GEP_FLUX_{quarter}_all'].withColumnRenamed('Claim_paid', 'Claim_paid_')
    _dfs[f'GEP_FLUX_{quarter}_all'] = _dfs[f'GEP_FLUX_{quarter}_all'].withColumnRenamed('GEP', 'GEP_')
    _dfs[f'GEP_FLUX_{quarter}_all'] = _dfs[f'GEP_FLUX_{quarter}_all'].withColumnRenamed('REP', 'REP_')
    _dfs[f'GEP_FLUX_{quarter}_all'].createOrReplaceTempView(f'GEP_FLUX_{quarter}_all')


recuperation_gep(quarter=quarter1, nb=nb1, ym_sup=ym_sup1, ym_inf=ym_inf1, vision=vision1)
recuperation_gep(quarter=quarter2, nb=nb2, ym_sup=ym_sup2, ym_inf=ym_inf2, vision=vision2)
recuperation_gep(quarter=quarter3, nb=nb3, ym_sup=ym_sup3, ym_inf=ym_inf3, vision=vision3)
recuperation_gep(quarter=quarter4, nb=nb4, ym_sup=ym_sup4, ym_inf=ym_inf4, vision=vision4)
GEP_FLUX_ALL_QUARTERS = spark.sql(f"""select 
        coalesce(a.Country, b.Country, c.Country, d.Country) as Country ,
        coalesce(a.Rsrv_Grp, b.Rsrv_Grp, c.Rsrv_Grp, d.Rsrv_Grp) as Rsrv_Grp,
        coalesce(a.SCHEME, b.SCHEME, c.SCHEME, d.SCHEME) as SCHEME,
        coalesce(a.cover, b.cover, c.cover, d.cover) as cover,
        coalesce(a.cover_name, b.cover_name, c.cover_name, d.cover_name) as cover_name,
        coalesce(a.Entity_CD, b.Entity_CD, c.Entity_CD, d.Entity_CD) as Entity_CD,
        coalesce(a.Cohort, b.Cohort, c.Cohort, d.Cohort) as Cohort,
        coalesce(a.Quarter, b.Quarter, c.Quarter, d.Quarter) as Quarter,
        coalesce(a.Month, b.Month, c.Month, d.Month) as Month,
        coalesce(a.Product, b.Product, c.Product, d.Product) as Product,
        coalesce(a.RPP, b.RPP, c.RPP, d.RPP) as RPP,
        coalesce(a.Agent, b.Agent, c.Agent, d.Agent) as Agent,
        coalesce(a.Agent_Name, b.Agent_Name, c.Agent_Name, d.Agent_Name) as Agent_Name,
        a.Claim_paid_{vision1}, b.Claim_paid_{vision2}, c.Claim_paid_{vision3}, d.Claim_paid_{vision4},
        a.GEP_{vision1}, b.GEP_{vision2}, c.GEP_{vision3}, d.GEP_{vision4},
        a.REP_{vision1}, b.REP_{vision2}, c.REP_{vision3}, d.REP_{vision4}
    from 
        GEP_FLUX_{quarter1}_all as a
    full join 
       GEP_FLUX_{quarter2}_all as b on a.Country = b.Country and a.cover = b.cover and a.cohort=b.cohort and a.Rsrv_Grp=b.Rsrv_Grp and a.cover_name = b.cover_name and a.SCHEME = b.SCHEME 
        and a.Quarter=b.Quarter and a.Month=b.Month and a.Agent=b.Agent and a.Agent_Name = b.Agent_Name and a.Entity_CD=b.Entity_CD and a.RPP=b.RPP and a.Product=b.Product
    full join 
        GEP_FLUX_{quarter3}_all as c on a.Country = c.Country and a.cover = c.cover and a.cohort=c.cohort and a.Rsrv_Grp=c.Rsrv_Grp and a.cover_name = c.cover_name and a.SCHEME = c.SCHEME 
        and a.Quarter=c.Quarter and a.Month=c.Month and a.Agent=c.Agent and a.Agent_Name = c.Agent_Name and a.Entity_CD=c.Entity_CD and a.RPP=c.RPP and a.Product=c.Product
    full join 
        GEP_FLUX_{quarter4}_all as d on a.Country = d.Country and a.cover = d.cover and a.cohort=d.cohort and a.Rsrv_Grp=d.Rsrv_Grp and a.cover_name = d.cover_name and a.SCHEME = d.SCHEME 
        and a.Quarter=d.Quarter and a.Month=d.Month and a.Agent=d.Agent and a.Agent_Name = d.Agent_Name and a.Entity_CD=d.Entity_CD and a.RPP=d.RPP and a.Product=d.Product""")
GEP_FLUX_ALL_QUARTERS.createOrReplaceTempView('GEP_FLUX_ALL_QUARTERS')

GEP_FLUX_ALL_QUARTERS = spark.table('GEP_FLUX_ALL_QUARTERS')
# IF/THEN (manual review needed):
#   if Claim_paid_{vision1} = . then Claim_paid_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision2} = . then Claim_paid_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision3} = . then Claim_paid_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if Claim_paid_{vision4} = . then Claim_paid_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if GEP_{vision1} = . then GEP_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if GEP_{vision2} = . then GEP_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if GEP_{vision3} = . then GEP_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if GEP_{vision4} = . then GEP_{vision4} = 0 ;
# IF/THEN (manual review needed):
#   if REP_{vision1} = . then REP_{vision1} = 0 ;
# IF/THEN (manual review needed):
#   if REP_{vision2} = . then REP_{vision2} = 0 ;
# IF/THEN (manual review needed):
#   if REP_{vision3} = . then REP_{vision3} = 0 ;
# IF/THEN (manual review needed):
#   if REP_{vision4} = . then REP_{vision4} = 0 ;
GEP_FLUX_ALL_QUARTERS.createOrReplaceTempView('GEP_FLUX_ALL_QUARTERS')

GEP_FLUX_ALL_QUARTERS = spark.sql("""select a.*, b.YTD_VALUE
from GEP_FLUX_ALL_QUARTERS a
left join tauxdechange b
on a.COUNTRY = b.COUNTRY""")
GEP_FLUX_ALL_QUARTERS.createOrReplaceTempView('GEP_FLUX_ALL_QUARTERS')

GEP_FLUX_ALL_QUARTERS = spark.table('GEP_FLUX_ALL_QUARTERS')
GEP_FLUX_ALL_QUARTERS = (GEP_FLUX_ALL_QUARTERS
    .withColumn(f'Claim_paid_{vision1}', F.expr(f"""Claim_paid_{vision1}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision2}', F.expr(f"""Claim_paid_{vision2}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision3}', F.expr(f"""Claim_paid_{vision3}*YTD_VALUE"""))
    .withColumn(f'Claim_paid_{vision4}', F.expr(f"""Claim_paid_{vision4}*YTD_VALUE"""))
    .withColumn(f'GEP_{vision1}', F.expr(f"""GEP_{vision1}*YTD_VALUE"""))
    .withColumn(f'GEP_{vision2}', F.expr(f"""GEP_{vision2}*YTD_VALUE"""))
    .withColumn(f'GEP_{vision3}', F.expr(f"""GEP_{vision3}*YTD_VALUE"""))
    .withColumn(f'GEP_{vision4}', F.expr(f"""GEP_{vision4}*YTD_VALUE"""))
    .withColumn(f'REP_{vision1}', F.expr(f"""REP_{vision1}*YTD_VALUE"""))
    .withColumn(f'REP_{vision2}', F.expr(f"""REP_{vision2}*YTD_VALUE"""))
    .withColumn(f'REP_{vision3}', F.expr(f"""REP_{vision3}*YTD_VALUE"""))
    .withColumn(f'REP_{vision4}', F.expr(f"""REP_{vision4}*YTD_VALUE"""))
)
GEP_FLUX_ALL_QUARTERS.createOrReplaceTempView('GEP_FLUX_ALL_QUARTERS')

def export_excel(data, outfile, sheet):
    data.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(outfile)


export = "~/NAS/X/08.Progammes/INTERNATIONAL/99_Travaux et Personnel/Gnanisso/Reserving TIA/Reportings TIA/Output/GEP_FLUX_Reports_Q226.xlsx"
export_excel(data=f"GEP_FLUX_{quarter1}_all", outfile=export, sheet=f"GEP_FLUX__{quarter1}_all")
export_excel(data=f"GEP_FLUX_{quarter2}_all", outfile=export, sheet=f"GEP_FLUX__{quarter2}_all")
export_excel(data=f"GEP_FLUX_{quarter3}_all", outfile=export, sheet=f"GEP_FLUX__{quarter3}_all")
export_excel(data=f"GEP_FLUX_{quarter4}_all", outfile=export, sheet=f"GEP_FLUX__{quarter4}_all")
export_excel(data=GEP_FLUX_ALL_QUARTERS, outfile=export, sheet="GEP_FLUX_ALL_QUARTERS")
# ####################################################################################################################
# ########################################EXPORT RESULTS################################################
# ########################################################################################################################
def export_excel(data, outfile, sheet):
    data.write.format('com.crealytics.spark.excel').option('dataAddress', f'{sheet}!A1').option('header', 'true').mode('overwrite').save(outfile)


export = "~/NAS/X/08.Progammes/INTERNATIONAL/99_Travaux et Personnel/Gnanisso/Reserving TIA/Reportings TIA/Output/Reservings_reports_Q226.xlsx"
export_excel(data=f"CLMHDR_{quarter1}_all", outfile=export, sheet=f"CLMHDR_{quarter1}_all")
export_excel(data=f"CLMHDR_{quarter2}_all", outfile=export, sheet=f"CLMHDR_{quarter2}_all")
export_excel(data=f"CLMHDR_{quarter3}_all", outfile=export, sheet=f"CLMHDR_{quarter3}_all")
export_excel(data=f"CLMHDR_{quarter4}_all", outfile=export, sheet=f"CLMHDR_{quarter4}_all")
export_excel(data=FLUX_ALL_QUARTERS, outfile=export, sheet="FLUX_ALL_QUARTERS")
export_excel(data=CLAIMS_PAID_ALL_QUARTERS, outfile=export, sheet="CLAIMS_PAID_ALL_QUARTERS")
# Test
aud_path = "~/NAS/X/08.Progammes/INTERNATIONAL/06_Inventaire CLP/2021_Q4_Prov/02_Elements_Techniques/TIA/Arrete reel/RESERVES/ON-SYSTEM/CASES RESERVES/Output"  # LIBNAME AUD
spark.sql('CREATE SCHEMA IF NOT EXISTS aud')  # base Spark pour LIBNAME AUD