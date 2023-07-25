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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Object_Reason_Codes_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Object_Reason_Codes_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OBJECT_REASON_CODES_0


query_0 = f"""SELECT
  REASON_CODES_ID AS REASON_CODES_ID,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  OBJECT_TYPE AS OBJECT_TYPE,
  REASON_CODE AS REASON_CODE,
  REASON_TYPE AS REASON_TYPE,
  COMMENTS AS COMMENTS,
  REF_FIELD_1 AS REF_FIELD_1,
  REF_FIELD_2 AS REF_FIELD_2,
  ACTIVE AS ACTIVE,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM
FROM
  OBJECT_REASON_CODES"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OBJECT_REASON_CODES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OBJECT_REASON_CODES_1


query_1 = f"""SELECT
  REASON_CODES_ID AS REASON_CODES_ID,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  OBJECT_TYPE AS OBJECT_TYPE,
  REASON_CODE AS REASON_CODE,
  REASON_TYPE AS REASON_TYPE,
  COMMENTS AS COMMENTS,
  REF_FIELD_1 AS REF_FIELD_1,
  REF_FIELD_2 AS REF_FIELD_2,
  ACTIVE AS ACTIVE,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OBJECT_REASON_CODES_0
WHERE
  TRUNC(Shortcut_to_OBJECT_REASON_CODES_0.CREATED_DTTM) >= TRUNC(now()) -7
  OR TRUNC(
    Shortcut_to_OBJECT_REASON_CODES_0.LAST_UPDATED_DTTM
  ) >= TRUNC(now()) -7"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OBJECT_REASON_CODES_1")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_2


query_2 = f"""SELECT
  REASON_CODES_ID AS REASON_CODES_ID,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  REASON_CODE AS REASON_CODE,
  REASON_TYPE AS REASON_TYPE,
  COMMENTS AS COMMENTS,
  REF_FIELD_1 AS REF_FIELD_1,
  REF_FIELD_2 AS REF_FIELD_2,
  ACTIVE AS ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  now() AS LOAD_TSTMP_exp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OBJECT_REASON_CODES_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_TRANS_2")

# COMMAND ----------
# DBTITLE 1, OMS_OBJECT_REASON_CODES_PRE


spark.sql("""INSERT INTO
  OMS_OBJECT_REASON_CODES_PRE
SELECT
  REASON_CODES_ID AS REASON_CODES_ID,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  REASON_CODE AS REASON_CODE,
  REASON_TYPE AS REASON_TYPE,
  COMMENTS AS COMMENTS,
  REF_FIELD_1 AS REF_FIELD_1,
  REF_FIELD_2 AS REF_FIELD_2,
  ACTIVE AS ACTIVE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Object_Reason_Codes_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Object_Reason_Codes_Pre", mainWorkflowId, parentName)
