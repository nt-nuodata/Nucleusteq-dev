# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Discount_Type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_0


query_0 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_DISCOUNT_TYPE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_1


query_1 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_DISCOUNT_TYPE_2


query_2 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_NAME AS OMS_DISCOUNT_TYPE_NAME,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_DISCOUNT_TYPE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_A_DISCOUNT_TYPE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_3


query_3 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_NAME AS OMS_DISCOUNT_TYPE_NAME,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_DISCOUNT_TYPE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_A_DISCOUNT_TYPE_4


query_4 = f"""SELECT
  DETAIL.DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DETAIL.DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  MASTER.OMS_DISCOUNT_TYPE_ID AS lkp_OMS_DISCOUNT_TYPE_ID,
  MASTER.OMS_DISCOUNT_TYPE_NAME AS lkp_OMS_DISCOUNT_TYPE_NAME,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_PRE_1 DETAIL ON MASTER.OMS_DISCOUNT_TYPE_ID = DETAIL.DISCOUNT_TYPE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_A_DISCOUNT_TYPE_4")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  lkp_OMS_DISCOUNT_TYPE_ID AS lkp_OMS_DISCOUNT_TYPE_ID,
  lkp_OMS_DISCOUNT_TYPE_NAME AS lkp_OMS_DISCOUNT_TYPE_NAME,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_A_DISCOUNT_TYPE_4
WHERE
  ISNULL(lkp_OMS_DISCOUNT_TYPE_ID)
  OR (
    NOT iSNULL(lkp_OMS_DISCOUNT_TYPE_ID)
    AND (
      IFF(
        ISNULL(LTRIM(RTRIM(DISCOUNT_TYPE_NAME))),
        ' ',
        LTRIM(RTRIM(DISCOUNT_TYPE_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_DISCOUNT_TYPE_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_DISCOUNT_TYPE_NAME))
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_FLAG_6


query_6 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  now() AS o_UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS o_LOAD_TSTMP,
  IFF(ISNULL(lkp_OMS_DISCOUNT_TYPE_ID), 1, 2) AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_UPDATE_7


query_7 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  o_UPDATE_FLAG AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_UPDATE_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_UPDATE_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INSERT_UPDATE_7")

# COMMAND ----------
# DBTITLE 1, OMS_A_DISCOUNT_TYPE


spark.sql("""MERGE INTO OMS_A_DISCOUNT_TYPE AS TARGET
USING
  UPD_INSERT_UPDATE_7 AS SOURCE ON TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID,
  TARGET.OMS_DISCOUNT_TYPE_NAME = SOURCE.DISCOUNT_TYPE_NAME,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_DISCOUNT_TYPE_NAME = SOURCE.DISCOUNT_TYPE_NAME
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_DISCOUNT_TYPE_ID,
    TARGET.OMS_DISCOUNT_TYPE_NAME,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.DISCOUNT_TYPE_ID,
    SOURCE.DISCOUNT_TYPE_NAME,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Discount_Type", mainWorkflowId, parentName)
