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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Event_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Event_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID
FROM
  OMS_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ORDER_EVENT_1


query_1 = f"""SELECT
  ORDER_EVENT_ID AS ORDER_EVENT_ID,
  ORDER_ID AS ORDER_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM
FROM
  ORDER_EVENT"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_ORDER_EVENT_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ORDER_EVENT_2


query_2 = f"""SELECT
  Shortcut_to_ORDER_EVENT_1.ORDER_EVENT_ID AS ORDER_EVENT_ID,
  Shortcut_to_ORDER_EVENT_1.ORDER_ID AS ORDER_ID,
  Shortcut_to_ORDER_EVENT_1.FIELD_NAME AS FIELD_NAME,
  Shortcut_to_ORDER_EVENT_1.OLD_VALUE AS OLD_VALUE,
  Shortcut_to_ORDER_EVENT_1.NEW_VALUE AS NEW_VALUE,
  Shortcut_to_ORDER_EVENT_1.LINE_ITEM_ID AS LINE_ITEM_ID,
  Shortcut_to_ORDER_EVENT_1.CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  Shortcut_to_ORDER_EVENT_1.CREATED_SOURCE AS CREATED_SOURCE,
  Shortcut_to_ORDER_EVENT_1.CREATED_DTTM AS CREATED_DTTM,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID AS ORDER_ID1,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ORDER_EVENT_1,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID = Shortcut_to_ORDER_EVENT_1.ORDER_ID
  AND UPPER(Shortcut_to_ORDER_EVENT_1.FIELD_NAME) IN ('REF_NUM1', 'LINE ITEM: STATUS')"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_ORDER_EVENT_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_3


query_3 = f"""SELECT
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
  SQ_Shortcut_to_ORDER_EVENT_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_EVENT_PRE


spark.sql("""INSERT INTO
  OMS_ORDER_EVENT_PRE
SELECT
  ORDER_EVENT_ID AS ORDER_EVENT_ID,
  ORDER_ID AS ORDER_ID,
  FIELD_NAME AS FIELD_NAME,
  OLD_VALUE AS OLD_VALUE,
  NEW_VALUE AS NEW_VALUE,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Event_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Event_Pre", mainWorkflowId, parentName)
