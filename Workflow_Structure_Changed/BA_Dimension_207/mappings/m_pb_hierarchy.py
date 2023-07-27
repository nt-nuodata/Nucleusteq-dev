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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_hierarchy")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_hierarchy", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_HIERARCHY_0


query_0 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PB_HIERARCHY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PB_HIERARCHY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_HIERARCHY_1


query_1 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_HIERARCHY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_HIERARCHY_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_HIERARCHY_PRE_2


query_2 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID
FROM
  PB_HIERARCHY_PRE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PB_HIERARCHY_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_HIERARCHY_PRE_3


query_3 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_HIERARCHY_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_HIERARCHY_PRE_3")

# COMMAND ----------
# DBTITLE 1, JNR_PB_HIERARCHY_4


query_4 = f"""SELECT
  DETAIL.BRAND_CD AS BRAND_CD,
  DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID,
  DETAIL.BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  DETAIL.PB_MANAGER_ID AS PB_MANAGER_ID,
  DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  MASTER.BRAND_CD AS BRAND_CD_pre,
  MASTER.SAP_DEPT_ID AS SAP_DEPT_ID_pre,
  MASTER.BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID_pre,
  MASTER.PB_MANAGER_ID AS PB_MANAGER_ID_pre,
  MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID_pre,
  nvl(
    MASTER.Monotonically_Increasing_Id,
    DETAIL.Monotonically_Increasing_Id
  ) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PB_HIERARCHY_PRE_3 MASTER
  FULL OUTER JOIN SQ_Shortcut_to_PB_HIERARCHY_1 DETAIL ON MASTER.BRAND_CD = DETAIL.BRAND_CD
  AND MASTER.SAP_DEPT_ID = DETAIL.SAP_DEPT_ID
  AND MASTER.BRAND_CLASSIFICATION_ID = DETAIL.BRAND_CLASSIFICATION_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_PB_HIERARCHY_4")

# COMMAND ----------
# DBTITLE 1, EXP_PB_HIERARCHY_5


query_5 = f"""SELECT
  IFF(
    ISNULL(i_BRAND_CD)
    AND NOT ISNULL(i_BRAND_CD_pre),
    'I',
    IFF(
      NOT ISNULL(i_BRAND_CD)
      AND ISNULL(i_BRAND_CD_pre),
      'D',
      'U'
    )
  ) AS v_LOAD_STRATEGY_FLAG,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    BRAND_CD,
    BRAND_CD_pre
  ) AS BRAND_CD,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    SAP_DEPT_ID,
    SAP_DEPT_ID_pre
  ) AS SAP_DEPT_ID,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    BRAND_CLASSIFICATION_ID,
    BRAND_CLASSIFICATION_ID_pre
  ) AS BRAND_CLASSIFICATION_ID,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    PB_MANAGER_ID,
    PB_MANAGER_ID_pre
  ) AS PB_MANAGER_ID,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    PB_DIRECTOR_ID,
    PB_DIRECTOR_ID_pre
  ) AS PB_DIRECTOR_ID,
  IFF(
    IFF(
      ISNULL(BRAND_CD)
      AND NOT ISNULL(BRAND_CD_pre),
      'I',
      IFF(
        NOT ISNULL(BRAND_CD)
        AND ISNULL(BRAND_CD_pre),
        'D',
        'U'
      )
    ) = 'D',
    1,
    0
  ) AS DELETE_FLAG,
  now() AS UPDATE_DT,
  now() AS LOAD_DT,
  IFF(
    ISNULL(BRAND_CD)
    AND NOT ISNULL(BRAND_CD_pre),
    'I',
    IFF(
      NOT ISNULL(BRAND_CD)
      AND ISNULL(BRAND_CD_pre),
      'D',
      'U'
    )
  ) AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_PB_HIERARCHY_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_PB_HIERARCHY_5")

# COMMAND ----------
# DBTITLE 1, UPS_PB_HIERARCHY_6


query_6 = f"""SELECT
  BRAND_CD AS BRAND_CD,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(LOAD_STRATEGY_FLAG = 'I', 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_PB_HIERARCHY_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("UPS_PB_HIERARCHY_6")

# COMMAND ----------
# DBTITLE 1, PB_HIERARCHY


spark.sql("""MERGE INTO PB_HIERARCHY AS TARGET
USING
  UPS_PB_HIERARCHY_6 AS SOURCE ON TARGET.SAP_DEPT_ID = SOURCE.SAP_DEPT_ID
  AND TARGET.BRAND_CLASSIFICATION_ID = SOURCE.BRAND_CLASSIFICATION_ID
  AND TARGET.BRAND_CD = SOURCE.BRAND_CD
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.BRAND_CD = SOURCE.BRAND_CD,
  TARGET.SAP_DEPT_ID = SOURCE.SAP_DEPT_ID,
  TARGET.BRAND_CLASSIFICATION_ID = SOURCE.BRAND_CLASSIFICATION_ID,
  TARGET.PB_MANAGER_ID = SOURCE.PB_MANAGER_ID,
  TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID,
  TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PB_MANAGER_ID = SOURCE.PB_MANAGER_ID
  AND TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID
  AND TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.BRAND_CD,
    TARGET.SAP_DEPT_ID,
    TARGET.BRAND_CLASSIFICATION_ID,
    TARGET.PB_MANAGER_ID,
    TARGET.PB_DIRECTOR_ID,
    TARGET.DELETE_FLAG,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.BRAND_CD,
    SOURCE.SAP_DEPT_ID,
    SOURCE.BRAND_CLASSIFICATION_ID,
    SOURCE.PB_MANAGER_ID,
    SOURCE.PB_DIRECTOR_ID,
    SOURCE.DELETE_FLAG,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_hierarchy")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_hierarchy", mainWorkflowId, parentName)
