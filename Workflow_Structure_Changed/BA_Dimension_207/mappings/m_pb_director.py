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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_director")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_director", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_DIRECTOR1_0


query_0 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PB_DIRECTOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PB_DIRECTOR1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_DIRECTOR_1


query_1 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_DIRECTOR1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_DIRECTOR_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PB_DIRECTOR_PRE_2


query_2 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME
FROM
  PB_DIRECTOR_PRE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PB_DIRECTOR_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_DIRECTOR_PRE_3


query_3 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PB_DIRECTOR_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_DIRECTOR_PRE_3")

# COMMAND ----------
# DBTITLE 1, JNR_DIRECTOR_4


query_4 = f"""SELECT
  MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  MASTER.PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID1,
  DETAIL.PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME1,
  nvl(
    MASTER.Monotonically_Increasing_Id,
    DETAIL.Monotonically_Increasing_Id
  ) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PB_DIRECTOR_1 MASTER
  FULL OUTER JOIN SQ_Shortcut_to_PB_DIRECTOR_PRE_3 DETAIL ON MASTER.PB_DIRECTOR_ID = DETAIL.PB_DIRECTOR_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_DIRECTOR_4")

# COMMAND ----------
# DBTITLE 1, FIL_DIRECTOR_5


query_5 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  PB_DIRECTOR_ID1 AS PB_DIRECTOR_ID1,
  PB_DIRECTOR_NAME1 AS PB_DIRECTOR_NAME1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_DIRECTOR_4
WHERE
  NOT ISNULL(PB_DIRECTOR_ID)
  AND NOT ISNULL(PB_DIRECTOR_ID1)
  AND PB_DIRECTOR_NAME != PB_DIRECTOR_NAME1
  OR NOT ISNULL(PB_DIRECTOR_ID)
  AND ISNULL(PB_DIRECTOR_ID1)
  OR ISNULL(PB_DIRECTOR_ID)
  AND NOT ISNULL(PB_DIRECTOR_ID1)"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_DIRECTOR_5")

# COMMAND ----------
# DBTITLE 1, EXP_DIRECTOR_6


query_6 = f"""SELECT
  IFF(
    NOT ISNULL(PB_DIRECTOR_ID)
    AND NOT ISNULL(PB_DIRECTOR_ID1),
    PB_DIRECTOR_ID1,
    IFF(
      NOT ISNULL(PB_DIRECTOR_ID)
      AND ISNULL(PB_DIRECTOR_ID1),
      PB_DIRECTOR_ID,
      PB_DIRECTOR_ID1
    )
  ) AS PB_DIRECTOR_ID,
  IFF(
    NOT ISNULL(PB_DIRECTOR_NAME)
    AND NOT ISNULL(PB_DIRECTOR_NAME1),
    PB_DIRECTOR_NAME1,
    IFF(
      NOT ISNULL(PB_DIRECTOR_NAME)
      AND ISNULL(PB_DIRECTOR_NAME1),
      PB_DIRECTOR_NAME,
      PB_DIRECTOR_NAME1
    )
  ) AS PB_DIRECTOR_NAME,
  IFF(
    NOT ISNULL(PB_DIRECTOR_ID)
    AND NOT ISNULL(PB_DIRECTOR_ID1),
    0,
    IFF(
      NOT ISNULL(PB_DIRECTOR_ID)
      AND ISNULL(PB_DIRECTOR_ID1),
      1,
      0
    )
  ) AS DELETE_FLAG,
  now() AS UPDATE_DT,
  now() AS LOAD_DT,
  IFF(
    NOT ISNULL(PB_DIRECTOR_ID)
    AND NOT ISNULL(PB_DIRECTOR_ID1),
    'U',
    IFF(
      NOT ISNULL(PB_DIRECTOR_ID)
      AND ISNULL(PB_DIRECTOR_ID1),
      'D',
      'I'
    )
  ) AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_DIRECTOR_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_DIRECTOR_6")

# COMMAND ----------
# DBTITLE 1, UPS_PB_DIRECTOR_7


query_7 = f"""SELECT
  PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
  PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
  DELETE_FLAG AS DELETE_FLAG,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(LOAD_STRATEGY_FLAG = 'I', 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_DIRECTOR_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPS_PB_DIRECTOR_7")

# COMMAND ----------
# DBTITLE 1, PB_DIRECTOR


spark.sql("""MERGE INTO PB_DIRECTOR AS TARGET
USING
  UPS_PB_DIRECTOR_7 AS SOURCE ON TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PB_DIRECTOR_ID = SOURCE.PB_DIRECTOR_ID,
  TARGET.PB_DIRECTOR_NAME = SOURCE.PB_DIRECTOR_NAME,
  TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PB_DIRECTOR_NAME = SOURCE.PB_DIRECTOR_NAME
  AND TARGET.DELETE_FLAG = SOURCE.DELETE_FLAG
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PB_DIRECTOR_ID,
    TARGET.PB_DIRECTOR_NAME,
    TARGET.DELETE_FLAG,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.PB_DIRECTOR_ID,
    SOURCE.PB_DIRECTOR_NAME,
    SOURCE.DELETE_FLAG,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_director")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_director", mainWorkflowId, parentName)
