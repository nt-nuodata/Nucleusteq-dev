# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_brand")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_brand", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BRAND_PRE_0


query_0 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  BRAND_NAME AS BRAND_NAME
FROM
  BRAND_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_BRAND_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_BRAND_PRE_1


query_1 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  BRAND_NAME AS BRAND_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_BRAND_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_BRAND_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BRAND1_2


query_2 = f"""SELECT
  BrandCd AS BrandCd,
  BrandName AS BrandName,
  BrandTypeCd AS BrandTypeCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  BrandClassificationCd AS BrandClassificationCd
FROM
  Brand"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_BRAND1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_BRAND_3


query_3 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  BRAND_NAME AS BRAND_NAME,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_BRAND1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_BRAND_3")

# COMMAND ----------
# DBTITLE 1, JNR_BRAND_4


query_4 = f"""SELECT
  DETAIL.BRAND_CD AS BRAND_CD,
  DETAIL.BRAND_NAME AS BRAND_NAME,
  MASTER.BRAND_CD AS BRAND_CD_pre,
  MASTER.BRAND_NAME AS BRAND_NAME_pre,
  nvl(
    MASTER.Monotonically_Increasing_Id,
    DETAIL.Monotonically_Increasing_Id
  ) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_BRAND_PRE_1 MASTER
  FULL OUTER JOIN SQ_Shortcut_to_BRAND_3 DETAIL ON MASTER.BRAND_CD = DETAIL.BRAND_CD"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_BRAND_4")

# COMMAND ----------
# DBTITLE 1, FIL_BRAND_5


query_5 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  BRAND_NAME AS BRAND_NAME,
  BRAND_CD_pre AS BRAND_CD_pre,
  BRAND_NAME_pre AS BRAND_NAME_pre,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_BRAND_4
WHERE
  NOT ISNULL(BRAND_CD)
  AND NOT ISNULL(BRAND_CD_pre)
  AND BRAND_NAME != BRAND_NAME_pre -- Existing records with changes
  OR NOT ISNULL(BRAND_CD)
  AND ISNULL(BRAND_CD_pre) --Deleted records
  OR ISNULL(BRAND_CD)
  AND NOT ISNULL(BRAND_CD_pre) --New records"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_BRAND_5")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_STRATEGY_6


query_6 = f"""SELECT
  IFF(
    NOT ISNULL(in_BRAND_CD)
    AND NOT ISNULL(in_BRAND_CD_pre)
    AND MD5(in_BRAND_NAME) != MD5(in_BRAND_NAME_pre),
    'U',
    IFF(
      NOT ISNULL(in_BRAND_CD)
      AND ISNULL(in_BRAND_CD_pre),
      'D',
      IFF(
        ISNULL(in_BRAND_CD)
        AND NOT ISNULL(in_BRAND_CD_pre),
        'I'
      )
    )
  ) AS v_LOAD_STRATEGY_FLAG,
  DECODE(
    IFF(
      NOT ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre)
      AND MD5(BRAND_NAME) != MD5(BRAND_NAME_pre),
      'U',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        IFF(
          ISNULL(BRAND_CD)
          AND NOT ISNULL(BRAND_CD_pre),
          'I'
        )
      )
    ),
    'I',
    BRAND_CD_pre,
    'U',
    BRAND_CD_pre,
    'D',
    BRAND_CD
  ) AS BRAND_CD,
  DECODE(
    IFF(
      NOT ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre)
      AND MD5(BRAND_NAME) != MD5(BRAND_NAME_pre),
      'U',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        IFF(
          ISNULL(BRAND_CD)
          AND NOT ISNULL(BRAND_CD_pre),
          'I'
        )
      )
    ),
    'I',
    BRAND_NAME_pre,
    'U',
    BRAND_NAME_pre,
    'D',
    BRAND_NAME
  ) AS BRAND_NAME,
  IFF(
    IFF(
      NOT ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre)
      AND MD5(BRAND_NAME) != MD5(BRAND_NAME_pre),
      'U',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        IFF(
          ISNULL(BRAND_CD)
          AND NOT ISNULL(BRAND_CD_pre),
          'I'
        )
      )
    ) = 'D',
    1,
    0
  ) AS DELETE_FLAG,
  now() AS UPDATE_DT,
  now() AS LOAD_DT,
  IFF(
    NOT ISNULL(BRAND_CD)
    AND NOT ISNULL(BRAND_CD_pre)
    AND MD5(BRAND_NAME) != MD5(BRAND_NAME_pre),
    'U',
    IFF(
      NOT ISNULL(BRAND_CD)
      AND ISNULL(BRAND_CD_pre),
      'D',
      IFF(
        ISNULL(BRAND_CD)
        AND NOT ISNULL(BRAND_CD_pre),
        'I'
      )
    )
  ) AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_BRAND_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_LOAD_STRATEGY_6")

# COMMAND ----------
# DBTITLE 1, UPS_BRAND_7


query_7 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  BRAND_NAME AS BRAND_NAME,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(LOAD_STRATEGY_FLAG = 'I', 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_LOAD_STRATEGY_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPS_BRAND_7")

# COMMAND ----------
# DBTITLE 1, BRAND


spark.sql("""MERGE INTO BRAND AS TARGET
USING
  UPS_BRAND_7 AS SOURCE ON TARGET.BRAND_CD = SOURCE.BRAND_CD
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.BRAND_CD = SOURCE.BRAND_CD,
  TARGET.BRAND_NAME = SOURCE.BRAND_NAME,
  TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.BRAND_NAME = SOURCE.BRAND_NAME
  AND TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.BRAND_CD,
    TARGET.BRAND_NAME,
    TARGET.DELETE_FLAG,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.BRAND_CD,
    SOURCE.BRAND_NAME,
    SOURCE.DELETE_FLAG,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_brand")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_brand", mainWorkflowId, parentName)
