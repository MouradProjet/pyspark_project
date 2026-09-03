# -*- coding: utf-8 -*-
"""
MACRO SPLIT ENTITY — attribution du Legal_Entity (FICL / FACL / TPA / UNKNOWN)
aux bases CLMHDR et CLMTRNS, selon des règles PROPRES À CHAQUE PAYS.

Le SAS d'origine avait un bloc PROC SQL par pays, sélectionné par %if &pays=XX.
Ici, chaque règle pays est une expression SQL CASE WHEN stockée dans un
dictionnaire ; la fonction entity(pays) applique la bonne règle.

Les tables sources data.{pays}_CLMHDR et data.{pays}_CLMTRNS sont supposées
déjà présentes dans Databricks (schéma 'data').
"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

TARGET_SCHEMA = "data"

# ═══════════════════════════════════════════════════════════════════════
# RÈGLES Legal_Entity PAR PAYS
# ═══════════════════════════════════════════════════════════════════════
# Chaque valeur est l'expression SQL CASE WHEN qui calcule Legal_Entity.
# 'ne' → '<>', 'like' reste 'like', guillemets doubles → simples.
# ═══════════════════════════════════════════════════════════════════════
ENTITY_RULES = {
    "EE": """CASE WHEN uw_company = '101' THEN 'FICL' ELSE 'FACL' END""",

    "LT": """CASE WHEN uw_company = '101' THEN 'FICL' ELSE 'FACL' END""",

    "LV": """CASE WHEN uw_company = '101' THEN 'FICL' ELSE 'FACL' END""",

    "AT": """CASE WHEN uw_company = '101' THEN 'FICL' ELSE 'FACL' END""",

    "BE": """CASE WHEN uw_company = '101' THEN 'FICL' ELSE 'FACL' END""",

    "CO": """CASE WHEN uw_company IN ('901','971') THEN 'FICL' ELSE 'FACL' END""",

    "DK": """
        CASE WHEN ((uw_company = '101' AND cover NOT LIKE 'FL%' AND cover NOT LIKE 'FM%'
              AND cover NOT LIKE 'FN%' AND cover NOT LIKE 'FR%' AND cover NOT LIKE 'LL%'
              AND cover NOT LIKE 'LM%' AND cover NOT LIKE 'LN%' AND cover NOT LIKE 'LR%')
              OR (uw_company IN ('102','912') AND (cover LIKE 'R%' OR cover LIKE 'U%'))
              OR (uw_company IN ('921','931','941','951'))
              OR (uw_company = '911' AND cover NOT LIKE 'LR%')) THEN 'FICL'
        ELSE CASE WHEN (uw_company = '952'
              OR (uw_company = '101' AND (cover LIKE 'FL%' OR cover LIKE 'FM%' OR cover LIKE 'FN%'
                  OR cover LIKE 'FR%' OR cover LIKE 'LL%' OR cover LIKE 'LM%' OR cover LIKE 'LN%' OR cover LIKE 'LR%'))
              OR (uw_company = '911' AND cover LIKE 'LR%')
              OR (uw_company IN ('102','912') AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%')) THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "FI": """
        CASE WHEN (uw_company = '951'
              OR (uw_company = '101' AND scheme <> 'OSB.1' AND cover NOT LIKE 'FL%' AND cover NOT LIKE 'FM%'
                  AND cover NOT LIKE 'FN%' AND cover NOT LIKE 'FR%' AND cover NOT LIKE 'LL%'
                  AND cover NOT LIKE 'LM%' AND cover NOT LIKE 'LN%' AND cover NOT LIKE 'LR%')
              OR (uw_company = '101' AND scheme = 'OSB.1' AND cover NOT LIKE 'DZ%' AND cover NOT LIKE 'FL%'
                  AND cover NOT LIKE 'FM%' AND cover NOT LIKE 'FN%' AND cover NOT LIKE 'FR%' AND cover NOT LIKE 'LL%'
                  AND cover NOT LIKE 'LM%' AND cover NOT LIKE 'LN%' AND cover NOT LIKE 'LR%')
              OR (uw_company = '102' AND (cover LIKE 'R%' OR cover LIKE 'U%' OR cover LIKE 'HC%'))
              OR (uw_company = '102' AND scheme = 'E10.1' AND cover = 'DM')) THEN 'FICL'
        ELSE CASE WHEN (scheme = 'OSB.1' AND cover = 'DZ') OR uw_company IN ('802','952')
              OR (uw_company = '101' AND (cover LIKE 'FL%' OR cover LIKE 'FM%' OR cover LIKE 'FN%' OR cover LIKE 'FR%'
                  OR cover LIKE 'LL%' OR cover LIKE 'LM%' OR cover LIKE 'LN%' OR cover LIKE 'LR%'))
              OR (uw_company = '102' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%' AND cover NOT LIKE 'HC%') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "FR": """
        CASE WHEN (uw_company = '101' AND cover NOT LIKE 'F%' AND cover NOT LIKE 'L%' AND cover NOT LIKE 'D%')
              OR (uw_company = '101' AND cover IN ('FA','LA'))
              OR (uw_company = '101' AND cover LIKE 'D%' AND scheme NOT IN
                  ('ACA.1','CFD.1','CF8.1','CO0.1','CO2.1','CO2.2','CO2.3','CX1.1','ED4.1','ED5.1','EF5.1','IR5.1'))
              OR (uw_company = '102' AND cover NOT LIKE 'F%' AND cover NOT LIKE 'L%'
                  AND cover NOT LIKE 'D%' AND cover NOT LIKE 'I%') THEN 'FICL'
        ELSE CASE WHEN (uw_company = '102' AND (cover LIKE 'F%' OR cover LIKE 'L%' OR cover LIKE 'D%' OR cover LIKE 'I%'))
              OR (uw_company = '101' AND (cover LIKE 'F%' OR cover LIKE 'L%') AND cover NOT IN ('FA','LA'))
              OR (uw_company = '101' AND cover LIKE 'D%' AND scheme IN
                  ('ACA.1','CFD.1','CF8.1','CO0.1','CO2.1','CO2.2','CO2.3','CX1.1','ED4.1','ED5.1','EF5.1','IR5.1'))
              THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "DE": """
        CASE WHEN (uw_company = '101' AND cover NOT LIKE 'FL%' AND cover NOT LIKE 'FM%'
              AND cover NOT LIKE 'FN%' AND cover NOT LIKE 'FR%' AND cover NOT LIKE 'LL%'
              AND cover NOT LIKE 'LM%' AND cover NOT LIKE 'LN%' AND cover NOT LIKE 'LR%')
              OR uw_company IN ('901','911','941','951') THEN 'FICL'
        ELSE CASE WHEN (uw_company = '101' AND (cover LIKE 'FL%' OR cover LIKE 'FM%' OR cover LIKE 'FN%' OR cover LIKE 'FR%'
              OR cover LIKE 'LL%' OR cover LIKE 'LM%' OR cover LIKE 'LN%' OR cover LIKE 'LR%'))
              OR uw_company IN ('102','902','912','942','952') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "GR": """
        CASE WHEN uw_company IN ('101','801','811','821','841','851','861') THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('102','802','812','842','852','862') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "IE": """
        CASE WHEN uw_company IN ('101','911')
              OR (uw_company = '901' AND cover NOT IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '902' AND (cover LIKE 'C%' OR cover LIKE 'G%' OR cover LIKE 'H%' OR cover LIKE 'M%'
                  OR cover LIKE 'P%' OR cover LIKE 'R%' OR cover LIKE 'U%'))
              OR (uw_company = '102' AND (cover LIKE 'R%' OR cover LIKE 'U%')) THEN 'FICL'
        ELSE CASE WHEN (uw_company = '102' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%')
              OR (uw_company = '901' AND cover IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '902' AND cover NOT LIKE 'C%' AND cover NOT LIKE 'G%' AND cover NOT LIKE 'H%'
                  AND cover NOT LIKE 'M%' AND cover NOT LIKE 'P%' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%')
              OR uw_company = '912' THEN 'FACL'
        ELSE CASE WHEN uw_company IN ('942','952') THEN 'TPA'
        ELSE 'UNKNOWN' END END END""",

    "IT": """
        CASE WHEN uw_company IN ('791','801','811','821','831','841','851','861','881','911','921','931','941','971','991')
              OR (uw_company = '101' AND cover NOT IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '102' AND (cover LIKE 'R%' OR cover LIKE 'U%')) THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('802','812','822','842','912','922','932','952','972','992')
              OR (uw_company = '101' AND cover IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '102' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%') THEN 'FACL'
        ELSE CASE WHEN uw_company = '982' THEN 'TPA'
        ELSE 'UNKNOWN' END END END""",

    "NL": """
        CASE WHEN uw_company = '101' THEN 'FICL'
        ELSE CASE WHEN uw_company = '102' THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "NI": """
        CASE WHEN uw_company = '101' THEN 'FICL'
        ELSE CASE WHEN uw_company = '102' THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "NO": """
        CASE WHEN uw_company IN ('101','951','911') THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('102','952','912') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "PL": """
        CASE WHEN uw_company IN ('101','921','911','931')
              OR (uw_company = '102' AND (cover LIKE 'P%' OR cover LIKE 'C%' OR cover LIKE 'M%'
                  OR cover LIKE 'H%' OR cover LIKE 'G%')) THEN 'FICL'
        ELSE CASE WHEN (uw_company = '102' AND cover NOT LIKE 'P%' AND cover NOT LIKE 'C%'
                  AND cover NOT LIKE 'M%' AND cover NOT LIKE 'H%' AND cover NOT LIKE 'G%')
              OR uw_company IN ('912','932') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "PT": """
        CASE WHEN uw_company IN ('101','671','681','701','711','811','821','831','851','861','871','881','891',
                  '901','921','931','941','951','961','971','991','911') THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('102','682','712','802','812','832','872','882','892','902','922','972','982','992','912') THEN 'FACL'
        ELSE CASE WHEN uw_company = '842' THEN 'TPA'
        ELSE 'UNKNOWN' END END END""",

    "ES": """
        CASE WHEN uw_company IN ('101','121','812','821','831','841','861','871','881','921','922','911','851')
              OR (uw_company = '901' AND cover <> 'FL' AND cover <> 'FM' AND cover <> 'FN' AND cover <> 'FR'
                  AND cover <> 'LL' AND cover <> 'LM' AND cover <> 'LN' AND cover <> 'LR')
              OR (uw_company = '862' AND (cover LIKE 'R%' OR cover LIKE 'U%')) THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('122','802','832','882','902','912','852')
              OR (uw_company = '901' AND (cover = 'FL' OR cover = 'FM' OR cover = 'FN' OR cover = 'FR'
                  OR cover = 'LL' OR cover = 'LM' OR cover = 'LN' OR cover = 'LR'))
              OR (uw_company = '862' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "SE": """
        CASE WHEN uw_company IN ('901','911') OR (uw_company = '951' AND cover <> 'LR')
              OR (uw_company = '101' AND cover NOT IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '102' AND (cover LIKE 'R%' OR cover LIKE 'U%')) THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('902','952','912') OR (uw_company = '951' AND cover = 'LR')
              OR (uw_company = '101' AND cover IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '102' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "CH": """
        CASE WHEN scheme = 'G3.3' OR uw_company IN ('101','961','911')
              OR (uw_company = '962' AND cover NOT LIKE 'L%' AND cover NOT LIKE 'F%') THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('102','912')
              OR (uw_company = '962' AND (cover LIKE 'L%' OR cover LIKE 'F%')) THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "TR": """
        CASE WHEN uw_company IN ('701','831','861','871','881','891','921','981','991') THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('702','982','992') THEN 'FACL'
        ELSE 'UNKNOWN' END END""",

    "UK": """
        CASE WHEN uw_company IN ('131','141','911')
              OR (uw_company = '101' AND cover NOT IN ('FL','FM','FN','FR','LL','LM','LN','LR'))
              OR (uw_company = '102' AND (cover LIKE 'R%' OR cover LIKE 'U%')) THEN 'FICL'
        ELSE CASE WHEN uw_company IN ('132','142','912')
              OR (uw_company = '102' AND cover NOT LIKE 'R%' AND cover NOT LIKE 'U%')
              OR (uw_company = '101' AND cover IN ('FL','FM','FN','FR','LL','LM','LN','LR')) THEN 'FACL'
        ELSE CASE WHEN uw_company IN ('811','821','831','901','902','962','971','972','982','992') THEN 'TPA'
        ELSE 'UNKNOWN' END END END""",
}

# Ajustements TPA post-calcul (les DATA steps SAS après certains PROC SQL).
# clé = pays, valeur = (condition SQL, valeur forcée)
TPA_OVERRIDES = {
    "DK": ("uw_company IN ('951','952') AND substr(scheme,1,2) IN ('5B','5C')", "TPA"),
    "FI": ("uw_company IN (951,952) AND substr(scheme,1,3) IN ('SN1','SN2','SN3','SN4','SN6','SN7','SN8','SN9')", "TPA"),
    "NO": ("uw_company IN ('951','952') AND substr(scheme,1,2) IN ('ED','EE','EG','EH','EI','EJ','EK','EL','EM')", "TPA"),
    "SE": ("uw_company IN ('951','952') AND substr(scheme,1,2) IN ('ED','EE','EF','EG','EH','EI','EJ')", "TPA"),
}


# ═══════════════════════════════════════════════════════════════════════
# FONCTION D'ATTRIBUTION Legal_Entity
# ═══════════════════════════════════════════════════════════════════════
def entity(pays):
    """Calcule Legal_Entity pour CLMHDR selon la règle du pays, applique les
    ajustements TPA et le lot de corrections finales, puis propage à CLMTRNS."""

    rule = ENTITY_RULES.get(pays)
    if rule is None:
        print(f"  ⚠ {pays} : aucune règle Legal_Entity définie — ignoré")
        return

    # 1. Règle principale : CASE WHEN pays → Legal_Entity
    clmhdr = spark.sql(f"""
        SELECT *, ({rule}) AS Legal_Entity
        FROM {TARGET_SCHEMA}.{pays}_CLMHDR
    """)

    # 2. Ajustement TPA post-calcul si le pays en a un
    if pays in TPA_OVERRIDES:
        cond, val = TPA_OVERRIDES[pays]
        clmhdr = clmhdr.withColumn(
            "Legal_Entity",
            F.when(F.expr(cond), F.lit(val)).otherwise(F.col("Legal_Entity")))

    clmhdr.createOrReplaceTempView(f"{pays}_CLMHDR")

    # 3. Lot de corrections finales (le gros bloc de withColumn du SAS).
    #    Chaque correction ne s'applique qu'à son pays et ne touche que les
    #    lignes vides/UNKNOWN → .otherwise(col) pour préserver le reste.
    clmhdr = self_corrections(clmhdr)

    clmhdr.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.{pays}_CLMHDR")

    # 4. Propager Legal_Entity à CLMTRNS via jointure sur CLA_CASE_NO
    clmtrns = spark.sql(f"""
        SELECT p.*, q.Legal_Entity
        FROM {TARGET_SCHEMA}.{pays}_CLMTRNS p
        LEFT JOIN {TARGET_SCHEMA}.{pays}_CLMHDR q
          ON p.CLA_CASE_NO = q.CLA_CASE_NO
    """)
    clmtrns.write.mode("overwrite").saveAsTable(f"{TARGET_SCHEMA}.{pays}_CLMTRNS")


def self_corrections(df):
    """Corrections finales communes (le bloc de withColumn du SAS d'origine).
    Chaque when garde .otherwise(col) pour ne pas écraser les valeurs déjà bonnes."""
    corrections = [
        ("uw_company IN ('862') AND cover IN ('RU','DS')", "FICL"),
        ("Country='FI' AND uw_company IN ('802') AND Legal_Entity=''", "FACL"),
        ("Country='ES' AND uw_company IN ('821','861','901','911','121','851','831','921','881','871','841') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='ES' AND uw_company IN ('902','122','912','802','852','922','862','832','882','812') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='SE' AND uw_company IN ('952','902') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='SE' AND uw_company IN ('912') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='SE' AND uw_company IN ('911','951','901') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='TR' AND uw_company IN ('701','981','881','891','871','991','921','861','831','811') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='TR' AND uw_company IN ('992','702','982') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='NO' AND uw_company IN ('951','952','912') AND Legal_Entity IN ('','UNKNOWN')", "TPA"),
        ("Country='NO' AND uw_company IN ('911') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='ES' AND uw_company IN ('912','932') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='PL' AND uw_company IN ('931','911','921') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='PL' AND uw_company IN ('912') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='PT' AND uw_company IN ('921','951','911','851','671','961','701','711','941','931','821','861','831','971','991','901','871','881','681','891','811') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='PT' AND uw_company IN ('912','882','832','992','682','922','902','872','892','972','802','712','982','812','842') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='PT' AND uw_company IN ('842') AND Legal_Entity IN ('','UNKNOWN')", "TPA"),
        ("Country='UK' AND uw_company IN ('821','831','872','992','962') AND Legal_Entity IN ('','UNKNOWN')", "TPA"),
        ("Country='UK' AND uw_company IN ('911','901','131','141','971','811') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("Country='UK' AND uw_company IN ('132','912','902','982','972','142') AND Legal_Entity IN ('','UNKNOWN')", "FACL"),
        ("Country='CH' AND uw_company IN ('911') AND Legal_Entity IN ('','UNKNOWN')", "FICL"),
        ("uw_company IN ('501','502')", "UNKNOWN"),
    ]
    for cond, val in corrections:
        df = df.withColumn(
            "Legal_Entity",
            F.when(F.expr(cond), F.lit(val)).otherwise(F.col("Legal_Entity")))
    return df


# ═══════════════════════════════════════════════════════════════════════
# EXÉCUTION
# ═══════════════════════════════════════════════════════════════════════
pays_list = [
    "FI", "UK", "SE", "FR", "DK", "DE", "BE", "CO", "GR", "IE",
    "IT", "NL", "NI", "NO", "PL", "PT", "ES", "CH", "TR", "AT",
    "MX", "LV", "EE", "LT",
    # "LU",  # commenté dans le SAS
]

for pays in pays_list:
    print(f"Legal_Entity pour {pays}...")
    try:
        entity(pays=pays)
    except Exception as e:
        print(f"  ⚠ {pays} : {e}")

print("Attribution Legal_Entity terminée.")
