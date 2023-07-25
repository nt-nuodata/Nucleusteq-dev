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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Event")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Event", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_EVENT_PRE_0


query_0 = f"""SELECT
  ORDER_EVENT_ID AS ORDER_EVENT_ID,
  ORDER_ID AS ORDER_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ORDER_EVENT_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_EVENT_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDER_EVENT_PRE_1


query_1 = f"""SELECT
  ORDER_EVENT_ID AS ORDER_EVENT_ID,
  ORDER_ID AS ORDER_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDER_EVENT_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDER_EVENT_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  ORDER_EVENT_ID AS ORDER_EVENT_ID,
  ORDER_ID AS ORDER_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ORDER_EVENT_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_EVENT


spark.sql("""INSERT INTO
  OMS_ORDER_EVENT
SELECT
  ORDER_EVENT_ID AS OMS_ORDER_EVENT_ID,
  ORDER_ID AS OMS_ORDER_ID,
  LINE_ITEM_ID AS OMS_LINE_ITEM_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  CREATED_DTTM AS OMS_CREATED_TSTMP,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Event")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Event", mainWorkflowId, parentName)
