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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_move_reason")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_move_reason", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_MOVE_INFO_0


query_0 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MOVE_TYPE_ID AS MOVE_TYPE_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID
FROM
  MOVE_INFO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_MOVE_INFO_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_MOVE_INFO_1


query_1 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_MOVE_INFO_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_MOVE_INFO_1")

# COMMAND ----------
# DBTITLE 1, EXP_RTRIM_2


query_2 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  RTRIM(MOVE_REASON_DESC) AS OUT_MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_MOVE_INFO_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_RTRIM_2")

# COMMAND ----------
# DBTITLE 1, FIL_NULLS_3


query_3 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  OUT_MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_RTRIM_2
WHERE
  IFF (
    ISNULL(MOVEMENT_ID)
    OR IS_SPACES(TO_CHAR(MOVEMENT_ID))
    OR MOVEMENT_ID = 9070080,
    FALSE,
    TRUE
  )"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FIL_NULLS_3")

# COMMAND ----------
# DBTITLE 1, LKP_MOVE_REASON_4


query_4 = f"""SELECT
  FN3.MOVE_REASON_ID AS IN_MOVE_REASON_ID,
  MR.MOVE_REASON_ID AS MOVE_REASON_ID,
  MR.MOVE_REASON_DESC AS MOVE_REASON_DESC,
  FN3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_NULLS_3 FN3
  LEFT JOIN MOVE_REASON MR ON MR.MOVE_REASON_ID = FN3.MR.MOVE_REASON_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_MOVE_REASON_4")

# COMMAND ----------
# DBTITLE 1, EXP_DetectChanges_5


query_5 = f"""SELECT
  LMR4.MOVE_REASON_ID AS L_MOVE_REASON_ID,
  IFF(ISNULL(LMR4.FN3.MOVE_REASON_ID), TRUE, FALSE) AS NewFlagReason,
  IFF(
    ISNULL(LMR4.FN3.MOVE_REASON_ID),
    FALSE,
    DECODE (
      TRUE,
      FN3.MOVE_REASON_DESC != RTRIM(LMR4.FN3.MOVE_REASON_DESC),
      TRUE,
      FALSE
    )
  ) AS ChangedFlagReason,
  FN3.MOVE_REASON_DESC AS MOVE_REASON_DESC,
  FN3.MOVE_REASON_ID AS MOVE_REASON_ID,
  RTRIM(L_MOVE_REASON_DESC) AS trim_MOVE_REASON_DESC,
  LMR4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_MOVE_REASON_4 LMR4
  INNER JOIN FIL_NULLS_3 FN3 ON LMR4.Monotonically_Increasing_Id = FN3.Monotonically_Increasing_Id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_DetectChanges_5")

# COMMAND ----------
# DBTITLE 1, UPD_Ins_Upd_6


query_6 = f"""SELECT
  MOVE_REASON_ID AS MOVE_REASON_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  NewFlagReason AS NewFlagReason,
  ChangedFlagReason AS ChangedFlagReason,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(
    TRUE,
    NewFlagReason = TRUE,
    'DD_INSERT',
    ChangedFlagReason = TRUE,
    'DD_UPDATE',
    'DD_REJECT'
  ) AS UPDATE_STRATEGY_FLAG
FROM
  EXP_DetectChanges_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("UPD_Ins_Upd_6")

# COMMAND ----------
# DBTITLE 1, MOVE_REASON


spark.sql("""MERGE INTO MOVE_REASON AS TARGET
USING
  UPD_Ins_Upd_6 AS SOURCE ON TARGET.MOVE_REASON_ID = SOURCE.MOVE_REASON_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.MOVE_REASON_ID = SOURCE.MOVE_REASON_ID,
  TARGET.MOVE_REASON_DESC = SOURCE.MOVE_REASON_DESC
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.MOVE_REASON_DESC = SOURCE.MOVE_REASON_DESC THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (TARGET.MOVE_REASON_ID, TARGET.MOVE_REASON_DESC)
VALUES
  (SOURCE.MOVE_REASON_ID, SOURCE.MOVE_REASON_DESC)""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_move_reason")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_move_reason", mainWorkflowId, parentName)
