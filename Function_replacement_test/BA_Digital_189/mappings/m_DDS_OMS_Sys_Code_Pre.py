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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Sys_Code_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Sys_Code_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SYS_CODE_0


query_0 = f"""SELECT
  REC_TYPE AS REC_TYPE,
  CODE_TYPE AS CODE_TYPE,
  CODE_ID AS CODE_ID,
  CODE_DESC AS CODE_DESC,
  SHORT_DESC AS SHORT_DESC,
  MISC_FLAGS AS MISC_FLAGS,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WM_VERSION_ID AS WM_VERSION_ID,
  SYS_CODE_ID AS SYS_CODE_ID,
  SYS_CODE_TYPE_ID AS SYS_CODE_TYPE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM
FROM
  SYS_CODE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SYS_CODE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SYS_CODE_1


query_1 = f"""SELECT
  REC_TYPE AS REC_TYPE,
  CODE_TYPE AS CODE_TYPE,
  CODE_ID AS CODE_ID,
  CODE_DESC AS CODE_DESC,
  SHORT_DESC AS SHORT_DESC,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  SYS_CODE_ID AS SYS_CODE_ID,
  SYS_CODE_TYPE_ID AS SYS_CODE_TYPE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SYS_CODE_0
WHERE
  (
    TRUNC(Shortcut_to_SYS_CODE_0.CREATED_DTTM) >= TRUNC(now()) -7
  )
  OR (
    TRUNC(Shortcut_to_SYS_CODE_0.LAST_UPDATED_DTTM) >= TRUNC(now()) -7
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SYS_CODE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  REC_TYPE AS REC_TYPE,
  CODE_TYPE AS CODE_TYPE,
  CODE_ID AS CODE_ID,
  CODE_DESC AS CODE_DESC,
  SHORT_DESC AS SHORT_DESC,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  SYS_CODE_ID AS SYS_CODE_ID,
  SYS_CODE_TYPE_ID AS SYS_CODE_TYPE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SYS_CODE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_SYS_CODE_PRE


spark.sql("""INSERT INTO
  OMS_SYS_CODE_PRE
SELECT
  REC_TYPE AS REC_TYPE,
  CODE_TYPE AS CODE_TYPE,
  CODE_ID AS CODE_ID,
  CODE_DESC AS CODE_DESC,
  SHORT_DESC AS SHORT_DESC,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  SYS_CODE_ID AS SYS_CODE_ID,
  SYS_CODE_TYPE_ID AS SYS_CODE_TYPE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Sys_Code_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Sys_Code_Pre", mainWorkflowId, parentName)
