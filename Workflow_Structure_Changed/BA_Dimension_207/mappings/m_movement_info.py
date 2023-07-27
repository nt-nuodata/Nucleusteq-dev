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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_movement_info")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_movement_info", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_new_move_info_0


query_0 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MOVE_TYPE_ID AS MOVE_TYPE_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID
FROM
  new_move_info"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_new_move_info_0")

# COMMAND ----------
# DBTITLE 1, SQ_SHORTCUT_NEW_MOVE_INFO_1


query_1 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MOVE_TYPE_ID AS MOVE_TYPE_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_new_move_info_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_SHORTCUT_NEW_MOVE_INFO_1")

# COMMAND ----------
# DBTITLE 1, EXP_CONVERT_2


query_2 = f"""SELECT
  TO_DECIMAL(MOVEMENT_ID) AS OUT_MOVEMENT_ID,
  MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  TO_DECIMAL(MOVE_TYPE_ID) AS OUT_MOVE_TYPE_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  TO_decimal(MOVE_REASON_ID) AS OUT_MOVE_REASON_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_SHORTCUT_NEW_MOVE_INFO_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_CONVERT_2")

# COMMAND ----------
# DBTITLE 1, LKP_MOVE_REASON_3


query_3 = f"""SELECT
  EC2.OUT_MOVE_REASON_ID AS IN_MOVE_REASON_ID,
  MR.MOVE_REASON_ID AS MOVE_REASON_ID,
  MR.MOVE_REASON_DESC AS MOVE_REASON_DESC,
  EC2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_CONVERT_2 EC2
  LEFT JOIN MOVE_REASON MR ON MR.MOVE_REASON_ID = EC2.OUT_MOVE_REASON_ID"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("LKP_MOVE_REASON_3")

# COMMAND ----------
# DBTITLE 1, LKP_MOVE_TYPE_4


query_4 = f"""SELECT
  EC2.OUT_MOVE_TYPE_ID AS IN_MOVE_TYPE_ID,
  MT.MOVE_TYPE_ID AS MOVE_TYPE_ID,
  MT.MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  EC2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_CONVERT_2 EC2
  LEFT JOIN MOVE_TYPE MT ON MT.MOVE_TYPE_ID = EC2.OUT_MOVE_TYPE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_MOVE_TYPE_4")

# COMMAND ----------
# DBTITLE 1, EXP_RTRIM_5


query_5 = f"""SELECT
  EC2.OUT_MOVEMENT_ID AS MOVEMENT_ID,
  RTRIM(LMT4.MOVE_TYPE_DESC) AS OUT_MOVE_TYPE_DESC,
  LMT4.MOVE_TYPE_ID AS MOVE_TYPE_ID,
  RTRIM(LMR3.MOVE_REASON_DESC) AS OUT_MOVE_REASON_DESC,
  LMR3.MOVE_REASON_ID AS MOVE_REASON_ID,
  LMT4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_MOVE_TYPE_4 LMT4
  INNER JOIN LKP_MOVE_REASON_3 LMR3 ON LMT4.Monotonically_Increasing_Id = LMR3.Monotonically_Increasing_Id
  INNER JOIN EXP_CONVERT_2 EC2 ON LMR3.Monotonically_Increasing_Id = EC2.Monotonically_Increasing_Id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_RTRIM_5")

# COMMAND ----------
# DBTITLE 1, FIL_NULLS_6


query_6 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  OUT_MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MOVE_TYPE_ID AS MOVE_TYPE_ID,
  OUT_MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_RTRIM_5
WHERE
  IFF (
    ISNULL(MOVEMENT_ID)
    OR IS_SPACES(TO_CHAR(MOVEMENT_ID)),
    FALSE,
    TRUE
  )"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("FIL_NULLS_6")

# COMMAND ----------
# DBTITLE 1, LKP_MOVEMENT_INFO_7


query_7 = f"""SELECT
  FN6.MOVEMENT_ID AS IN_MOVEMENT_ID,
  MI.MOVEMENT_ID AS MOVEMENT_ID,
  MI.MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
  MI.MOVE_CLASS_ID AS MOVE_CLASS_ID,
  MI.MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MI.MOVE_REASON_ID AS MOVE_REASON_ID,
  MI.MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MI.MOVE_TYPE_ID AS MOVE_TYPE_ID,
  FN6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_NULLS_6 FN6
  LEFT JOIN MOVEMENT_INFO MI ON MI.MOVEMENT_ID = FN6.MI.MOVEMENT_ID"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("LKP_MOVEMENT_INFO_7")

# COMMAND ----------
# DBTITLE 1, EXP_DetectChanges_8


query_8 = f"""SELECT
  IFF(ISNULL(LMI7.MOVEMENT_ID), TRUE, FALSE) AS NewFlag,
  IFF(
    ISNULL(LMI7.MOVEMENT_ID),
    FALSE,
    DECODE (
      TRUE,
      FN6.MOVE_TYPE_DESC != RTRIM(LMI7.MOVE_TYPE_DESC),
      TRUE,
      FN6.MOVE_TYPE_ID != RTRIM(LMI7.MOVE_TYPE_ID),
      TRUE,
      FN6.MOVE_REASON_DESC != RTRIM(LMI7.MOVE_REASON_DESC),
      TRUE,
      FN6.MOVE_REASON_ID != RTRIM(LMI7.MOVE_REASON_ID),
      TRUE,
      isnull(LMI7.MOVE_TYPE_ID),
      true,
      FALSE
    )
  ) AS ChangedFlag,
  FN6.MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  FN6.MOVE_TYPE_ID AS MOVE_TYPE_ID,
  FN6.MOVE_REASON_DESC AS MOVE_REASON_DESC,
  FN6.MOVE_REASON_ID AS MOVE_REASON_ID,
  RTRIM(L_MOVE_REASON_DESC) AS trim_MOVE_REASON_DESC,
  RTRIM(L_MOVE_REASON_ID) AS trim_MOVE_REASON_ID,
  RTRIM(L_MOVE_TYPE_DESC) AS trim_MOVE_TYPE_DESC,
  RTRIM(L_MOVE_TYPE_ID) AS trim_MOVE_TYPE_ID,
  IFF(ISNULL(LMI7.MOVE_CLASS_ID), 99, LMI7.MOVE_CLASS_ID) AS MOVE_CLASS_ID,
  IFF(
    ISNULL(LMI7.MOVE_CLASS_DESC),
    'default',
    LMI7.MOVE_CLASS_DESC
  ) AS MOVE_CLASS_DESC,
  FN6.MOVEMENT_ID AS MOVEMENT_ID,
  FN6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_NULLS_6 FN6
  INNER JOIN LKP_MOVEMENT_INFO_7 LMI7 ON FN6.Monotonically_Increasing_Id = LMI7.Monotonically_Increasing_Id"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("EXP_DetectChanges_8")

# COMMAND ----------
# DBTITLE 1, UPD_ins_upd_9


query_9 = f"""SELECT
  MOVEMENT_ID AS MOVEMENT_ID,
  MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
  MOVE_TYPE_ID AS MOVE_TYPE_ID,
  MOVE_REASON_DESC AS MOVE_REASON_DESC,
  MOVE_REASON_ID AS MOVE_REASON_ID,
  MOVE_CLASS_ID AS MOVE_CLASS_ID,
  MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
  NewFlag AS NewFlag,
  ChangedFlag AS ChangedFlag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE (
    TRUE,
    NewFlag = TRUE,
    'DD_INSERT',
    ChangedFlag = TRUE,
    'DD_UPDATE',
    'DD_REJECT'
  ) AS UPDATE_STRATEGY_FLAG
FROM
  EXP_DetectChanges_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("UPD_ins_upd_9")

# COMMAND ----------
# DBTITLE 1, MOVEMENT_INFO


spark.sql("""MERGE INTO MOVEMENT_INFO AS TARGET
USING
  UPD_ins_upd_9 AS SOURCE ON TARGET.MOVEMENT_ID = SOURCE.MOVEMENT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.MOVEMENT_ID = SOURCE.MOVEMENT_ID,
  TARGET.MOVE_CLASS_DESC = SOURCE.MOVE_CLASS_DESC,
  TARGET.MOVE_CLASS_ID = SOURCE.MOVE_CLASS_ID,
  TARGET.MOVE_REASON_DESC = SOURCE.MOVE_REASON_DESC,
  TARGET.MOVE_REASON_ID = SOURCE.MOVE_REASON_ID,
  TARGET.MOVE_TYPE_DESC = SOURCE.MOVE_TYPE_DESC,
  TARGET.MOVE_TYPE_ID = SOURCE.MOVE_TYPE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.MOVE_CLASS_DESC = SOURCE.MOVE_CLASS_DESC
  AND TARGET.MOVE_CLASS_ID = SOURCE.MOVE_CLASS_ID
  AND TARGET.MOVE_REASON_DESC = SOURCE.MOVE_REASON_DESC
  AND TARGET.MOVE_REASON_ID = SOURCE.MOVE_REASON_ID
  AND TARGET.MOVE_TYPE_DESC = SOURCE.MOVE_TYPE_DESC
  AND TARGET.MOVE_TYPE_ID = SOURCE.MOVE_TYPE_ID THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.MOVEMENT_ID,
    TARGET.MOVE_CLASS_DESC,
    TARGET.MOVE_CLASS_ID,
    TARGET.MOVE_REASON_DESC,
    TARGET.MOVE_REASON_ID,
    TARGET.MOVE_TYPE_DESC,
    TARGET.MOVE_TYPE_ID
  )
VALUES
  (
    SOURCE.MOVEMENT_ID,
    SOURCE.MOVE_CLASS_DESC,
    SOURCE.MOVE_CLASS_ID,
    SOURCE.MOVE_REASON_DESC,
    SOURCE.MOVE_REASON_ID,
    SOURCE.MOVE_TYPE_DESC,
    SOURCE.MOVE_TYPE_ID
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_movement_info")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_movement_info", mainWorkflowId, parentName)
