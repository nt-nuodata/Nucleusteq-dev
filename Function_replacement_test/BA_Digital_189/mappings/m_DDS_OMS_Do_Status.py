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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Do_Status")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Do_Status", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DO_STATUS_PRE_0


query_0 = f"""SELECT
  ORDER_STATUS AS ORDER_STATUS,
  DESCRIPTION AS DESCRIPTION,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DO_STATUS_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_DO_STATUS_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DO_STATUS_PRE_1


query_1 = f"""SELECT
  ORDER_STATUS AS ORDER_STATUS,
  DESCRIPTION AS DESCRIPTION,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DO_STATUS_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_DO_STATUS_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DO_STATUS_2


query_2 = f"""SELECT
  OMS_DO_ORDER_STATUS AS OMS_DO_ORDER_STATUS,
  OMS_DO_ORDER_STATUS_DESC AS OMS_DO_ORDER_STATUS_DESC,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DO_STATUS"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_DO_STATUS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DO_STATUS_3


query_3 = f"""SELECT
  OMS_DO_ORDER_STATUS AS OMS_DO_ORDER_STATUS,
  OMS_DO_ORDER_STATUS_DESC AS OMS_DO_ORDER_STATUS_DESC,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DO_STATUS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_DO_STATUS_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_DO_STATUS_4


query_4 = f"""SELECT
  DETAIL.ORDER_STATUS AS ORDER_STATUS,
  DETAIL.DESCRIPTION AS DESCRIPTION,
  MASTER.OMS_DO_ORDER_STATUS AS lkp_OMS_DO_ORDER_STATUS,
  MASTER.OMS_DO_ORDER_STATUS_DESC AS lkp_OMS_DO_ORDER_STATUS_DESC,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_DO_STATUS_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_DO_STATUS_PRE_1 DETAIL ON MASTER.OMS_DO_ORDER_STATUS = DETAIL.ORDER_STATUS"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_DO_STATUS_4")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  ORDER_STATUS AS ORDER_STATUS,
  DESCRIPTION AS DESCRIPTION,
  lkp_OMS_DO_ORDER_STATUS AS lkp_OMS_DO_ORDER_STATUS,
  lkp_OMS_DO_ORDER_STATUS_DESC AS lkp_OMS_DO_ORDER_STATUS_DESC,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_DO_STATUS_4
WHERE
  ISNULL(lkp_OMS_DO_ORDER_STATUS)
  OR (
    NOT ISNULL(lkp_OMS_DO_ORDER_STATUS)
    AND (
      IFF(
        ISNULL(LTRIM(RTRIM(DESCRIPTION))),
        ' ',
        LTRIM(RTRIM(DESCRIPTION))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_DO_ORDER_STATUS_DESC))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_DO_ORDER_STATUS_DESC))
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_FLAG_6


query_6 = f"""SELECT
  ORDER_STATUS AS ORDER_STATUS,
  DESCRIPTION AS DESCRIPTION,
  now() AS o_UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS o_LOAD_TSTMP,
  IFF(ISNULL(lkp_OMS_DO_ORDER_STATUS), 1, 2) AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_UPDATE_7


query_7 = f"""SELECT
  ORDER_STATUS AS ORDER_STATUS,
  DESCRIPTION AS DESCRIPTION,
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
# DBTITLE 1, OMS_DO_STATUS


spark.sql("""MERGE INTO OMS_DO_STATUS AS TARGET
USING
  UPD_INSERT_UPDATE_7 AS SOURCE ON TARGET.OMS_DO_ORDER_STATUS = SOURCE.ORDER_STATUS
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_DO_ORDER_STATUS = SOURCE.ORDER_STATUS,
  TARGET.OMS_DO_ORDER_STATUS_DESC = SOURCE.DESCRIPTION,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_DO_ORDER_STATUS_DESC = SOURCE.DESCRIPTION
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_DO_ORDER_STATUS,
    TARGET.OMS_DO_ORDER_STATUS_DESC,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ORDER_STATUS,
    SOURCE.DESCRIPTION,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Do_Status")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Do_Status", mainWorkflowId, parentName)
