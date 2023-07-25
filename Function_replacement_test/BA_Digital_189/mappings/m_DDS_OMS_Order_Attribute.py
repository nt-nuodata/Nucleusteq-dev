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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Attribute")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Attribute", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_0


query_0 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ORDER_ATTRIBUTE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_1


query_1 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_ATTRIBUTE_2


query_2 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ORDER_ATTRIBUTE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_ORDER_ATTRIBUTE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_3


query_3 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDER_ATTRIBUTE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_ORDER_ATTRIBUTE_4


query_4 = f"""SELECT
  DETAIL.ORDER_ID AS ORDER_ID,
  DETAIL.ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  DETAIL.ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  DETAIL.ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  MASTER.OMS_ORDER_ID AS lkp_OMS_ORDER_ID,
  MASTER.ATTRIBUTE_NAME AS lkp_ATTRIBUTE_NAME1,
  MASTER.ATTRIBUTE_SEQ AS lkp_ATTRIBUTE_SEQ1,
  MASTER.ATTRIBUTE_VALUE AS lkp_ATTRIBUTE_VALUE1,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_ORDER_ATTRIBUTE_PRE_1 DETAIL ON MASTER.OMS_ORDER_ID = DETAIL.ORDER_ID
  AND MASTER.ATTRIBUTE_NAME = DETAIL.ATTRIBUTE_NAME
  AND MASTER.ATTRIBUTE_SEQ = DETAIL.ATTRIBUTE_SEQ"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_ORDER_ATTRIBUTE_4")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  lkp_OMS_ORDER_ID AS lkp_OMS_ORDER_ID,
  lkp_ATTRIBUTE_NAME1 AS lkp_ATTRIBUTE_NAME1,
  lkp_ATTRIBUTE_SEQ1 AS lkp_ATTRIBUTE_SEQ1,
  lkp_ATTRIBUTE_VALUE1 AS lkp_ATTRIBUTE_VALUE1,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_ORDER_ATTRIBUTE_4
WHERE
  ISNULL(lkp_OMS_ORDER_ID)
  AND ISNULL(lkp_ATTRIBUTE_NAME1)
  AND ISNULL(lkp_ATTRIBUTE_SEQ1)
  OR (
    NOT ISNULL(lkp_OMS_ORDER_ID)
    AND NOT ISNULL(lkp_ATTRIBUTE_NAME1)
    AND ISNULL(lkp_ATTRIBUTE_SEQ1)
    AND (
      IFF(
        ISNULL(LTRIM(RTRIM(ATTRIBUTE_VALUE))),
        ' ',
        LTRIM(RTRIM(ATTRIBUTE_VALUE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_ATTRIBUTE_VALUE1))),
        ' ',
        LTRIM(RTRIM(lkp_ATTRIBUTE_VALUE1))
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_FLAG_6


query_6 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  now() AS o_UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS o_LOAD_TSTMP,
  IFF(ISNULL(lkp_OMS_ORDER_ID), 1, 2) AS o_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPDATE_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_UPDATE_7


query_7 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
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
# DBTITLE 1, OMS_ORDER_ATTRIBUTE


spark.sql("""MERGE INTO OMS_ORDER_ATTRIBUTE AS TARGET
USING
  UPD_INSERT_UPDATE_7 AS SOURCE ON TARGET.ATTRIBUTE_NAME = SOURCE.ATTRIBUTE_NAME
  AND TARGET.OMS_ORDER_ID = SOURCE.ORDER_ID
  AND TARGET.ATTRIBUTE_SEQ = SOURCE.ATTRIBUTE_SEQ
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_ORDER_ID = SOURCE.ORDER_ID,
  TARGET.ATTRIBUTE_NAME = SOURCE.ATTRIBUTE_NAME,
  TARGET.ATTRIBUTE_SEQ = SOURCE.ATTRIBUTE_SEQ,
  TARGET.ATTRIBUTE_VALUE = SOURCE.ATTRIBUTE_VALUE,
  TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.ATTRIBUTE_VALUE = SOURCE.ATTRIBUTE_VALUE
  AND TARGET.UPDATE_TSTMP = SOURCE.o_UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_ORDER_ID,
    TARGET.ATTRIBUTE_NAME,
    TARGET.ATTRIBUTE_SEQ,
    TARGET.ATTRIBUTE_VALUE,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ORDER_ID,
    SOURCE.ATTRIBUTE_NAME,
    SOURCE.ATTRIBUTE_SEQ,
    SOURCE.ATTRIBUTE_VALUE,
    SOURCE.o_UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Attribute")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Attribute", mainWorkflowId, parentName)