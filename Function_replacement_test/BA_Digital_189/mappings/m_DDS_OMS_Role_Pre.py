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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Role_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Role_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ROLE_0


query_0 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ROLE_PRIVATE AS IS_ROLE_PRIVATE,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_SOURCE_TYPE_ID AS CREATED_SOURCE_TYPE_ID,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE_ID AS LAST_UPDATED_SOURCE_TYPE_ID,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  APPLY_TO_BUSINESS_PARTNERS AS APPLY_TO_BUSINESS_PARTNERS,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION
FROM
  ROLE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ROLE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ROLE_1


query_1 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ROLE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ROLE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ROLE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_ROLE_PRE


spark.sql("""INSERT INTO
  OMS_ROLE_PRE
SELECT
  ROLE_ID AS ROLE_ID,
  COMPANY_ID AS COMPANY_ID,
  ROLE_NAME AS ROLE_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ROLE_TYPE_ID AS ROLE_TYPE_ID,
  DESCRIPTION AS DESCRIPTION,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Role_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Role_Pre", mainWorkflowId, parentName)
