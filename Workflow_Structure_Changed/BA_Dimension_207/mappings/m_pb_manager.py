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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_manager")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_manager", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_MANAGER1_0


query_0 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PB_MANAGER"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PB_MANAGER1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_MANAGER_1


query_1 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_MANAGER1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_MANAGER_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_MANAGER_PRE_2


query_2 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID
FROM
  PB_MANAGER_PRE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PB_MANAGER_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_MANAGER_PRE_3


query_3 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_MANAGER_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_MANAGER_PRE_3")

# COMMAND ----------
# DBTITLE 1, JNR_MANAGER_4


query_4 = f"""SELECT
  MASTER.PB_MANAGER_ID AS PB_MANAGER_ID,
  MASTER.PB_MANAGER_NAME AS PB_MANAGER_NAME,
  MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DETAIL.PB_MANAGER_ID AS PB_MANAGER_ID_pre,
  DETAIL.PB_MANAGER_NAME AS PB_MANAGER_NAME_pre,
  DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID_pre,
  nvl(
    MASTER.Monotonically_Increasing_Id,
    DETAIL.Monotonically_Increasing_Id
  ) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PB_MANAGER_1 MASTER
  FULL OUTER JOIN SQ_Shortcut_to_PB_MANAGER_PRE_3 DETAIL ON MASTER.PB_MANAGER_ID = DETAIL.PB_MANAGER_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_MANAGER_4")

# COMMAND ----------
# DBTITLE 1, FIL_MANAGER_5


query_5 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_MANAGER_ID_pre AS PB_MANAGER_ID_pre,
  PB_MANAGER_NAME_pre AS PB_MANAGER_NAME_pre,
  PB_DIRECTOR_ID_pre AS PB_DIRECTOR_ID_pre,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_MANAGER_4
WHERE
  NOT ISNULL(PB_MANAGER_ID)
  AND NOT ISNULL(PB_MANAGER_ID_pre)
  AND PB_MANAGER_NAME != PB_MANAGER_NAME_pre
  OR NOT ISNULL(PB_MANAGER_ID)
  AND ISNULL(PB_MANAGER_ID_pre)
  OR ISNULL(PB_MANAGER_ID)
  AND NOT ISNULL(PB_MANAGER_ID_pre)"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_MANAGER_5")

# COMMAND ----------
# DBTITLE 1, EXP_MANAGER_6


query_6 = f"""SELECT
  IFF(
    NOT ISNULL(PB_MANAGER_ID)
    AND NOT ISNULL(PB_MANAGER_ID_pre),
    PB_MANAGER_ID_pre,
    IFF(
      NOT ISNULL(PB_MANAGER_ID)
      AND ISNULL(PB_MANAGER_ID_pre),
      PB_MANAGER_ID,
      PB_MANAGER_ID_pre
    )
  ) AS PB_MANAGER_ID,
  IFF(
    NOT ISNULL(PB_MANAGER_NAME)
    AND NOT ISNULL(PB_MANAGER_NAME_pre),
    PB_MANAGER_NAME_pre,
    IFF(
      NOT ISNULL(PB_MANAGER_NAME)
      AND ISNULL(PB_MANAGER_NAME_pre),
      PB_MANAGER_NAME,
      PB_MANAGER_NAME_pre
    )
  ) AS PB_MANAGER_NAME,
  IFF(
    NOT ISNULL(PB_DIRECTOR_ID)
    AND NOT ISNULL(PB_DIRECTOR_ID_pre),
    PB_DIRECTOR_ID_pre,
    IFF(
      NOT ISNULL(PB_DIRECTOR_ID)
      AND ISNULL(PB_DIRECTOR_ID_pre),
      PB_DIRECTOR_ID,
      PB_DIRECTOR_ID_pre
    )
  ) AS PB_DIRECTOR_ID,
  IFF(
    NOT ISNULL(PB_MANAGER_ID)
    AND NOT ISNULL(PB_MANAGER_ID_pre),
    0,
    IFF(
      NOT ISNULL(PB_MANAGER_ID)
      AND ISNULL(PB_MANAGER_ID_pre),
      1,
      0
    )
  ) AS DELETE_FLAG,
  now() AS UPDATE_DT,
  now() AS LOAD_DT,
  IFF(
    NOT ISNULL(PB_MANAGER_ID)
    AND NOT ISNULL(PB_MANAGER_ID_pre),
    'U',
    IFF(
      NOT ISNULL(PB_MANAGER_ID)
      AND ISNULL(PB_MANAGER_ID_pre),
      'D',
      'I'
    )
  ) AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_MANAGER_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_MANAGER_6")

# COMMAND ----------
# DBTITLE 1, UPS_PB_BRAND_7


query_7 = f"""SELECT
  PB_MANAGER_ID AS PB_MANAGER_ID,
  PB_MANAGER_NAME AS PB_MANAGER_NAME,
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(LOAD_STRATEGY_FLAG = 'I', 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_MANAGER_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPS_PB_BRAND_7")

# COMMAND ----------
# DBTITLE 1, PB_MANAGER


spark.sql("""MERGE INTO PB_MANAGER AS TARGET
USING
  UPS_PB_BRAND_7 AS SOURCE ON TARGET.PB_MANAGER_ID = SOURCE.PB_MANAGER_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PB_MANAGER_ID = SOURCE.PB_MANAGER_ID,
  TARGET.PB_MANAGER_NAME = SOURCE.PB_MANAGER_NAME,
  TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID,
  TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PB_MANAGER_NAME = SOURCE.PB_MANAGER_NAME
  AND TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID
  AND TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PB_MANAGER_ID,
    TARGET.PB_MANAGER_NAME,
    TARGET.PB_DIRECTOR_ID,
    TARGET.DELETE_FLAG,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.PB_MANAGER_ID,
    SOURCE.PB_MANAGER_NAME,
    SOURCE.PB_DIRECTOR_ID,
    SOURCE.DELETE_FLAG,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_manager")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_manager", mainWorkflowId, parentName)
