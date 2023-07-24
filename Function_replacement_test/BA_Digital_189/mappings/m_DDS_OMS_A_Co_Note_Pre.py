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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Co_Note_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Co_Note_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID
FROM
  OMS_PURCH_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_A_CO_NOTE_1


query_1 = f"""SELECT
  NOTE_ID AS NOTE_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  NOTE_TYPE_ID AS NOTE_TYPE_ID,
  NOTE_SEQ AS NOTE_SEQ,
  NOTE_DESCRIPTION AS NOTE_DESCRIPTION,
  IS_INTERNAL AS IS_INTERNAL,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  NOTE_CODE AS NOTE_CODE
FROM
  A_CO_NOTE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_A_CO_NOTE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_A_CO_NOTE_2


query_2 = f"""SELECT
  Shortcut_to_A_CO_NOTE_1.NOTE_ID AS NOTE_ID,
  Shortcut_to_A_CO_NOTE_1.ENTITY_ID AS ENTITY_ID,
  Shortcut_to_A_CO_NOTE_1.ENTITY_LINE_ID AS ENTITY_LINE_ID,
  Shortcut_to_A_CO_NOTE_1.NOTE_TYPE_ID AS NOTE_TYPE_ID,
  Shortcut_to_A_CO_NOTE_1.NOTE_SEQ AS NOTE_SEQ,
  Shortcut_to_A_CO_NOTE_1.NOTE_DESCRIPTION AS NOTE_DESCRIPTION,
  Shortcut_to_A_CO_NOTE_1.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  Shortcut_to_A_CO_NOTE_1.CREATED_DTTM AS CREATED_DTTM,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0,
  Shortcut_to_A_CO_NOTE_1
WHERE
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID = Shortcut_to_A_CO_NOTE_1.ENTITY_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_A_CO_NOTE_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_3


query_3 = f"""SELECT
  NOTE_ID AS NOTE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  NOTE_TYPE_ID AS NOTE_TYPE_ID,
  NOTE_SEQ AS NOTE_SEQ,
  NOTE_DESCRIPTION AS NOTE_DESCRIPTION,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  CREATED_DTTM AS CREATED_DTTM,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_A_CO_NOTE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_A_CO_NOTE_PRE


spark.sql("""INSERT INTO
  OMS_A_CO_NOTE_PRE
SELECT
  NOTE_ID AS NOTE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  NOTE_TYPE_ID AS NOTE_TYPE_ID,
  NOTE_SEQ AS NOTE_SEQ,
  NOTE_DESCRIPTION AS NOTE_DESCRIPTION,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  CREATED_DTTM AS CREATED_DTTM,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Co_Note_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Co_Note_Pre", mainWorkflowId, parentName)
