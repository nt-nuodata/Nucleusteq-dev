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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Tax_Detail_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Tax_Detail_Pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_A_TAX_DETAIL_1


query_1 = f"""SELECT
  TAX_DETAIL_ID AS TAX_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  EXTERNAL_TAX_DETAIL_ID AS EXTERNAL_TAX_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  TAX_REQUEST_TYPE_ID AS TAX_REQUEST_TYPE_ID,
  TAX_LOCATION_NAME AS TAX_LOCATION_NAME,
  TAX_NAME AS TAX_NAME,
  TAX_AMOUNT AS TAX_AMOUNT,
  TAX_RATE AS TAX_RATE,
  TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  TAX_DTTM AS TAX_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  CHARGE_ID AS CHARGE_ID,
  CHARGE_OBJECT_TYPE AS CHARGE_OBJECT_TYPE,
  TAX_TYPE AS TAX_TYPE,
  TAX_CATEGORY AS TAX_CATEGORY
FROM
  A_TAX_DETAIL"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_A_TAX_DETAIL_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_A_TAX_DETAIL_2


query_2 = f"""SELECT
  Shortcut_to_A_TAX_DETAIL_1.TAX_DETAIL_ID AS TAX_DETAIL_ID,
  Shortcut_to_A_TAX_DETAIL_1.COMPANY_ID AS COMPANY_ID,
  Shortcut_to_A_TAX_DETAIL_1.EXTERNAL_TAX_DETAIL_ID AS EXTERNAL_TAX_DETAIL_ID,
  Shortcut_to_A_TAX_DETAIL_1.ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  Shortcut_to_A_TAX_DETAIL_1.ENTITY_ID AS ENTITY_ID,
  Shortcut_to_A_TAX_DETAIL_1.ENTITY_LINE_ID AS ENTITY_LINE_ID,
  Shortcut_to_A_TAX_DETAIL_1.TAX_REQUEST_TYPE_ID AS TAX_REQUEST_TYPE_ID,
  Shortcut_to_A_TAX_DETAIL_1.TAX_LOCATION_NAME AS TAX_LOCATION_NAME,
  Shortcut_to_A_TAX_DETAIL_1.TAX_NAME AS TAX_NAME,
  Shortcut_to_A_TAX_DETAIL_1.TAX_AMOUNT AS TAX_AMOUNT,
  Shortcut_to_A_TAX_DETAIL_1.TAX_RATE AS TAX_RATE,
  Shortcut_to_A_TAX_DETAIL_1.TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  Shortcut_to_A_TAX_DETAIL_1.TAX_DTTM AS TAX_DTTM,
  Shortcut_to_A_TAX_DETAIL_1.MARK_FOR_DELETION AS MARK_FOR_DELETION,
  Shortcut_to_A_TAX_DETAIL_1.CREATED_SOURCE AS CREATED_SOURCE,
  Shortcut_to_A_TAX_DETAIL_1.CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  Shortcut_to_A_TAX_DETAIL_1.CREATED_DTTM AS CREATED_DTTM,
  Shortcut_to_A_TAX_DETAIL_1.LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  Shortcut_to_A_TAX_DETAIL_1.LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  Shortcut_to_A_TAX_DETAIL_1.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  Shortcut_to_A_TAX_DETAIL_1.CHARGE_ID AS CHARGE_ID,
  Shortcut_to_A_TAX_DETAIL_1.CHARGE_OBJECT_TYPE AS CHARGE_OBJECT_TYPE,
  Shortcut_to_A_TAX_DETAIL_1.TAX_TYPE AS TAX_TYPE,
  Shortcut_to_A_TAX_DETAIL_1.TAX_CATEGORY AS TAX_CATEGORY,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_A_TAX_DETAIL_1,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_A_TAX_DETAIL_1.ENTITY_ID = Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_A_TAX_DETAIL_2")

# COMMAND ----------
# DBTITLE 1, EXP_TSTMP_3


query_3 = f"""SELECT
  TAX_DETAIL_ID AS TAX_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  TAX_NAME AS TAX_NAME,
  TAX_AMOUNT AS TAX_AMOUNT,
  TAX_RATE AS TAX_RATE,
  TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  TAX_DTTM AS TAX_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TAX_TYPE AS TAX_TYPE,
  TAX_CATEGORY AS TAX_CATEGORY,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_A_TAX_DETAIL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_A_TAX_DETAIL_PRE


spark.sql("""INSERT INTO
  OMS_A_TAX_DETAIL_PRE
SELECT
  TAX_DETAIL_ID AS TAX_DETAIL_ID,
  COMPANY_ID AS COMPANY_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_LINE_ID AS ENTITY_LINE_ID,
  TAX_NAME AS TAX_NAME,
  TAX_AMOUNT AS TAX_AMOUNT,
  TAX_RATE AS TAX_RATE,
  TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  TAX_DTTM AS TAX_DTTM,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TAX_TYPE AS TAX_TYPE,
  TAX_CATEGORY AS TAX_CATEGORY,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Tax_Detail_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Tax_Detail_Pre", mainWorkflowId, parentName)
