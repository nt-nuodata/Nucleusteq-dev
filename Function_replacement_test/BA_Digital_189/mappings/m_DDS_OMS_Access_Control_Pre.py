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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Access_Control_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Access_Control_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ACCESS_CONTROL_0


query_0 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  GEO_REGION_ID AS GEO_REGION_ID,
  PARTNER_COMPANY_ID AS PARTNER_COMPANY_ID,
  BUSINESS_UNIT_ID AS BUSINESS_UNIT_ID,
  USER_GROUP_ID AS USER_GROUP_ID,
  IS_INTERNAL_CONTROL AS IS_INTERNAL_CONTROL,
  COMPANY_ID AS COMPANY_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM
FROM
  ACCESS_CONTROL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ACCESS_CONTROL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ACCESS_CONTROL_1


query_1 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ACCESS_CONTROL_0
WHERE
  (
    TRUNC(Shortcut_to_ACCESS_CONTROL_0.CREATED_DTTM) >= TRUNC(now()) -7
  )
  OR (
    TRUNC(Shortcut_to_ACCESS_CONTROL_0.LAST_UPDATED_DTTM) >= TRUNC(now()) -7
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ACCESS_CONTROL_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ACCESS_CONTROL_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_ACCESS_CONTROL_PRE


spark.sql("""INSERT INTO
  OMS_ACCESS_CONTROL_PRE
SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Access_Control_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Access_Control_Pre", mainWorkflowId, parentName)
