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
data_path = f"~/NAS/{lreseau}/08.Progammes/INTERNATIONAL/06_Inventaire CLP/{arrete}/02_Elements_Techniques/TIA/Extraction Donnees/Claims Extracts"  # LIBNAME data
# #########################################################################################################################################################################################
# #####################################################  MACRO SPLIT ENTITY    ####################################################################
# #########################################################################################################################################################################################
def entity(pays):
    # Add Legal Entity to CLMHDR_2
    if {pays}=EE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # LT
    if {pays}=LT:
        %let pays=LT ;
    
    PROC SQL;
    CREATE TABLE Data.LT_CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.LT_CLMHDR;
    QUIT;
    # LV
    if {pays} =LV:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # ESTONIA
    if {pays} = EE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # AUTRICHE
    if {pays} = AT:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # BELGIUM
    if {pays} = BE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company = '101' then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # COLOMBIA
    if {pays} = CO:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN ('901','971') then 'FICL' else 'FACL' end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # DENMARK
    if {pays} = DK:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when ((uw_company = '101' AND cover not like "FL%" AND cover not like "FM%" AND cover not like "FN%"
    AND cover not like "FR%" AND cover not like "LL%" AND cover not like "LM%" AND cover not like "LN%" 
    AND cover not like "LR%") OR (uw_company IN ('102','912') AND (cover like "R%" OR cover like "U%"))
    OR (uw_company IN('921', '931', '941','951')) 
    OR (uw_company = '911'  and cover not like "LR%")) then "FICL" 
    else case when (uw_company = '952' OR (uw_company ='101' AND (cover like "FL%" OR cover like "FM%" OR cover like "FN%" OR cover like "FR%" 
    OR cover like "LL%" OR cover like "LM%" OR cover like "LN%" OR cover like "LR%")) 
    OR (uw_company = '911' and cover like "LR%") 
    OR (uw_company IN('102','912') AND cover not like "R%" AND cover not like "U%")) then  "FACL" 
    else "UNKNOWN"
    end end  as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    
    Data Data.&pays._CLMHDR;
    set Data.&pays._CLMHDR;
    If uw_company IN('951','952') and substr(scheme,1,2) IN ("5B","5C") then Legal_Entity = "TPA";
    run;
    # FINLAND
    if {pays} = FI:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when (uw_company = '951' OR 
    (uw_company = '101' AND scheme ne "OSB.1" AND cover not like "FL%" AND cover not like "FM%" AND cover not like "FN%" 
    AND cover not like "FR%" AND cover not like "LL%" AND cover not like "LM%" AND cover not like "LN%" 
    AND cover not like "LR%") 
    OR (uw_company = '101' AND scheme = "OSB.1" AND cover not like "DZ%" AND cover not like "FL%" AND cover not like "FM%" AND cover not like "FN%" 
    AND cover not like "FR%" AND cover not like "LL%" AND cover not like "LM%" AND cover not like "LN%" 
    AND cover not like "LR%")
    OR (uw_company = '102' AND (cover like "R%" OR cover like "U%" OR cover like "HC%")) 
    OR (uw_company = '102'  AND scheme = 'E10.1' AND cover = 'DM')) then "FICL" 
    else case when (scheme = "OSB.1" and cover = "DZ") OR  uw_company IN ('802','952') OR 
    (uw_company = '101' AND (cover  like "FL%" OR cover like "FM%" OR cover like "FN%" OR cover like "FR%"
    OR cover like "LL%" OR cover like "LM%" OR cover like "LN%" OR cover like "LR%"))   
    OR (uw_company = '102' and cover NOT LIKE "R%" and cover NOT LIKE "U%" and cover NOT LIKE "HC%")then "FACL" 
    else "UNKNOWN" end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    
    Data Data.&pays._CLMHDR;
    set Data.&pays._CLMHDR;
    If uw_company IN(951,952) and substr(scheme,1,3) IN ("SN1","SN2","SN3","SN4","SN6","SN7","SN8","SN9") then Legal_Entity = "TPA";
    run;
    # FRANCE
    if {pays} = FR:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when (uw_company = '101' AND cover not like "F%" AND cover not like 
    		"L%" AND cover not like "D%")  OR  (uw_company = '101' and cover IN ("FA","LA")) OR (uw_company = '101' and cover like "D%" and 
              scheme NOT IN ("ACA.1","CFD.1","CF8.1","CO0.1","CO2.1","CO2.2","CO2.3","CX1.1","ED4.1","ED5.1", "EF5.1","IR5.1")) OR (uw_company = '102' AND cover not like 
    		"F%" AND cover not like "L%" AND cover not like "D%" AND cover not like "I%") then "FICL" 
    
    else case when (uw_company ='102' AND (cover like "F%" OR cover like "L%" OR cover like "D%" OR cover like "I%"))
    OR (uw_company = '101' AND (cover like "F%" OR cover like "L%") AND cover NOT IN ("FA","LA")) 
    OR (uw_company = '101' and cover like "D%" and scheme IN ("ACA.1","CFD.1","CF8.1","CO0.1","CO2.1","CO2.2","CO2.3","CX1.1","ED4.1","ED5.1", "EF5.1","IR5.1")) 
    then  "FACL" 
    else "UNKNOWN"
    end end as Legal_Entity
    
    FROM Data.&pays._CLMHDR;
    
    QUIT;
    # GERMANY
    if {pays} = DE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when (uw_company = '101' AND cover not like "FL%" 
    AND cover not like "FM%" AND cover not like "FN%" AND cover not like "FR%" AND cover not like "LL%"
    AND cover not like "LM%" AND cover not like "LN%" AND cover not like "LR%") OR uw_company IN ('901','911','941','951') then "FICL" 
    else case when (uw_company = '101' AND (cover like "FL%" OR cover like "FM%" OR cover like "FN%" OR cover like "FR%" 
    OR cover like "LL%" OR cover like "LM%" OR cover like "LN%" OR cover like "LR%")) OR uw_company IN ('102','902','912','942','952') then "FACL" 
    else "UNKNOWN"
     end end as Legal_Entity
    
    FROM Data.&pays._CLMHDR;
    
    QUIT;
    # GREECE
    if {pays} = GR:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, Case when uw_company IN('101', '801', '811','821', '841', '851', '861') then "FICL" 
    else case when uw_company IN('102', '802', '812', '842','852', '862') then "FACL" 
    else "UNKNOWN"
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # IRELAND
    if {pays} = IE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN ('101','911') OR 
    (uw_company = '901'  AND cover NOT IN("FL","FM","FN","FR","LL","LM","LN","LR"))
    OR (uw_company = '902'  AND (cover like "C%" OR cover like "G%" OR cover like "H%" OR cover like "M%"
    OR cover like "P%" OR cover like "R%" OR cover like "U%"))
    OR (uw_company = '102' AND (cover like "R%" OR cover like "U%")) then "FICL" 
    else case when (uw_company = '102' AND cover not like "R%" AND cover not like "U%") 
    OR (uw_company =  '901' AND cover IN("FL","FM","FN","FR","LL","LM","LN","LR"))
    OR  (uw_company = '902' AND cover not like "C%" AND cover not like "G%" AND cover not like "H%" 
    AND cover not like "M%" AND cover not like "P%" AND cover not like "R%" AND cover not like "U%" ) OR uw_company = '912' then "FACL" 
    else case when uw_company IN ('942','952') then "TPA"
    else "UNKNOWN"
    end end end as Legal_Entity
    
    FROM Data.&pays._CLMHDR;
    
    QUIT;
    # ITALY
    if {pays} = IT:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN('791','801','811','821','831','841','851','861','881','911','921','931','941','971','991')  
    OR (uw_company = "101" AND cover NOT IN("FL","FM","FN","FR","LL","LM","LN","LR")) 
    OR (uw_company = "102" AND (cover like "R%" OR cover like "U%")) then "FICL" 
    else case when uw_company IN ('802','812','822','842','912','922','932','952','972','992') 
    OR (uw_company = "101" AND cover IN("FL","FM","FN","FR","LL","LM","LN","LR")) 
    OR (uw_company = "102" AND cover not like "R%" AND cover not like "U%")  then "FACL" 
    else case when uw_company = "982" then "TPA"
    else "UNKNOWN"
    end end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # NETHERLANDS
    if {pays} = NL:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, Case when uw_company = '101'  then "FICL" 
    else case when uw_company = '102' then "FACL" 
    else "UNKNOWN" 
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # NORTHERN IRELAND
    if {pays} = NI:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, Case when uw_company = '101'  then "FICL" 
    else case when uw_company='102' then "FACL" 
    else "UNKNOWN" 
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # NORWAY
    if {pays} = NO:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, Case when uw_company IN('101','951','911') then "FICL" 
    else case when uw_company IN('102','952','912') then "FACL" 
    else "UNKNOWN"
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    
    Data Data.&pays._CLMHDR;
    set Data.&pays._CLMHDR;
    If uw_company IN('951','952') and  Substr(scheme,1,2) IN ("ED","EE","EG","EH","EI","EJ","EK","EL","EM") then Legal_Entity = "TPA";
    run;
    # POLAND
    if {pays} = PL:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, Case when uw_company IN ('101','921','911','931') 
    OR (uw_company = '102' AND (cover like "P%" OR cover like "C%" OR cover like "M%" OR cover 
    like "H%" OR cover like "G%")) then "FICL" 
    else case when (uw_company = '102' AND cover not like "P%" AND cover not like "C%" 
    AND cover not like "M%" AND cover not like "H%" AND cover not like "G%") OR uw_company IN ('912','932') then "FACL" 
    else "UNKNOWN"
     end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # PORTUGAL
    if {pays} = PT:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN ('101','671','681','701','711','811','821','831','851','861','871','881','891','901','921','931','941','951','961','971','991','911')
    then "FICL" 
    else case when uw_company IN ('102','682','712','802','812','832','872','882','892','902','922','972','982','992','912') 
    then "FACL" 
    else case when uw_company  = '842' then "TPA"
    else "UNKNOWN" 
    end end end  as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # SPAIN
    if {pays} = ES:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN('101','121','812','821','831','841','861','871','881','921','922','911','851') OR 
    (uw_company = '901' AND cover ne "FL" AND cover ne "FM" AND cover ne "FN" AND cover ne "FR" AND cover ne "LL" AND cover ne "LM" AND cover ne "LN" AND cover ne "LR") OR (uw_company = '862' AND (cover like "R%" OR cover like "U%")) then "FICL" 
    else case when uw_company IN('122','802','832','882','902','912','852') OR (uw_company = '901' AND (cover = "FL" OR cover = "FM" OR cover = "FN" OR cover = "FR" OR cover = "LL" OR cover = "LM" OR cover = "LN" OR cover = "LR")) OR (uw_company = '862' AND cover not like "R%" AND cover not like "U%") then "FACL" 
    else "UNKNOWN"
    end end  as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # SWEDEN
    if {pays} = SE:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN ('901','911')  OR (uw_company = '951' and cover ne "LR") OR
    (uw_company = '101' AND cover NOT IN("FL","FM","FN","FR","LL","LM","LN","LR")) 
    OR (uw_company = '102' AND (cover like "R%" OR cover like "U%")) then "FICL" 
    else case when uw_company IN ('902','952','912') OR (uw_company = '951' and cover = "LR") OR
    (uw_company = '101' AND cover IN("FL","FM","FN","FR","LL","LM","LN","LR"))
    OR (uw_company = '102' AND cover not like "R%" AND cover not like "U%") then "FACL" 
    else "UNKNOWN"
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    
    Data Data.&pays._CLMHDR;
    set Data.&pays._CLMHDR;
    If uw_company IN('951','952') and Substr(scheme,1,2) IN ("ED","EE","EF","EG","EH","EI","EJ") then Legal_Entity = "TPA";
    run;
    # SWITZERLAND
    if {pays} = CH:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when scheme = 'G3.3' OR uw_company IN ('101','961','911') OR (uw_company = '962' AND cover not like "L%" AND cover not like "F%") then "FICL"  
    else case when uw_company IN ('102','912') OR (uw_company = '962' AND (cover like "L%" OR cover like "F%"))  then "FACL" 
    else "UNKNOWN"
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # TURKEY
    if {pays} = TR:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN ('701','831','861','871','881','891','921','981','991') then "FICL" 
    else case when uw_company IN ('702','982','992') then "FACL"
    else "UNKNOWN"
    end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    # UK
    if {pays} = UK:
        PROC SQL;
    CREATE TABLE Data.&pays._CLMHDR
    AS SELECT 
    *, 
    Case when uw_company IN('131','141','911') OR (uw_company = '101' AND cover NOT IN("FL","FM","FN","FR","LL","LM","LN","LR"))
     OR (uw_company = '102' AND (cover like "R%" OR cover like "U%"))then "FICL" 
    else case when  uw_company IN(132,142,912) OR (uw_company = '102' AND cover not like "R%" AND cover not like "U%") 
    OR (uw_company = '101' AND cover IN ("FL","FM","FN","FR","LL","LM","LN","LR")) then "FACL"
    else case when uw_company IN ('811','821','831','901','902','962','971','972','982','992') then "TPA"
    else "UNKNOWN"
    end end end as Legal_Entity
    FROM Data.&pays._CLMHDR;
    QUIT;
    _dfs[f'{pays}_CLMHDR'] = spark.table(f'data.{pays}_CLMHDR')
    _dfs[f'{pays}_CLMHDR'] = (_dfs[f'{pays}_CLMHDR']
        .withColumn('Legal_Entity', F.when(F.expr("""uw_company IN ('862') AND cover IN ('RU','DS')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='FI' AND uw_company IN ('802') AND Legal_Entity=''"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='ES' AND uw_company IN ('821','861','901','911','121','821','911','901','851','831','921','861','881','871','841') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='ES' AND uw_company IN ('902','122','912','902','802','852','922','862','832','882','812') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='SE' AND uw_company IN ('952','902') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='SE' AND uw_company IN ('912') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='SE' AND uw_company IN ('911','951','901') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='TR' AND uw_company IN ('701','981','881','891','871','991','921','861','831','811') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='TR' AND uw_company IN ('992','702','982') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='NO' AND uw_company IN ('951','952','912') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('TPA')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='NO' AND uw_company IN ('911') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='ES' AND uw_company IN ('912','932') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='PL' AND uw_company IN ('931','911','921') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='PL' AND uw_company IN ('912') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='PT' AND uw_company IN ('921','951','911','851','671','961','701','711','941','931','821','861','831','971','991','901','871','881','681','891','811') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='PT' AND uw_company IN ('912','882','832','992','682','922','902','872','892','972','802','712','982','882','832','812','992','922','902','682','872','892','972','802','712','982','842') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='PT' AND uw_company IN ('842') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('TPA')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='UK' AND uw_company IN ('821','831','872','992','962') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('TPA')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='UK' AND uw_company IN ('911','901','131','141','971','811') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='UK' AND uw_company IN ('132','912','902','982','972','142') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FACL')))
        .withColumn('Legal_Entity', F.when(F.expr("""Country='CH' AND uw_company IN ('911') AND Legal_Entity IN ('','UNKNOWN')"""), F.lit('FICL')))
        .withColumn('Legal_Entity', F.when(F.expr("""uw_company IN ('501','502')"""), F.lit('UNKNOWN')))
    )
    _dfs[f'{pays}_CLMHDR'].createOrReplaceTempView(f'{pays}_CLMHDR')
    # LIBNAME Data -> base Spark: data.{pays}_CLMHDR
    _dfs[f'{pays}_CLMHDR'].write.mode('overwrite').saveAsTable(f'data.{pays}_CLMHDR')

    # Add Legal Entity to CLMTRNS_2
    _dfs[f'{pays}_CLMTRNS'] = spark.sql(f"""SELECT 
    p.*, q.Legal_Entity 
    FROM DATA.{pays}_CLMTRNS p
    LEFT JOIN Data.{pays}_CLMHDR q on p.CLA_CASE_NO=q.CLA_CASE_NO """)
    _dfs[f'{pays}_CLMTRNS'].createOrReplaceTempView(f'{pays}_CLMTRNS')


entity(pays="FI")
entity(pays="UK")
entity(pays="SE")
entity(pays="FR")
entity(pays="DK")
entity(pays="DE")
entity(pays="BE")
entity(pays="CO")
entity(pays="GR")
entity(pays="IE")
entity(pays="IT")
entity(pays="NL")
entity(pays="NI")
entity(pays="NO")
entity(pays="PL")
entity(pays="PT")
entity(pays="ES")
entity(pays="CH")
entity(pays="TR")
entity(pays="AT")
entity(pays="MX")
entity(pays="LV")
entity(pays="EE")
entity(pays="LT")
# %Entity(pays=LU);
LT_CLMTRNS = spark.sql("""SELECT 
p.*, q.Legal_Entity 
FROM DATA.LT_CLMTRNS p
LEFT JOIN Data.LT_CLMHDR q on p.CLA_CASE_NO=q.CLA_CASE_NO """)
LT_CLMTRNS.createOrReplaceTempView('LT_CLMTRNS')
