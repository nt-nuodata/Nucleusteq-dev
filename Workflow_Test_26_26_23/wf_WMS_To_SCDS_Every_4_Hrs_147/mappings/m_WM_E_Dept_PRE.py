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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_WM_E_Dept_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_WM_E_Dept_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_E_DEPT_0


query_0 = f"""SELECT
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM
FROM
  E_DEPT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_E_DEPT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_E_DEPT_1


query_1 = f"""SELECT
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_E_DEPT_0
WHERE
  {Initial_Load} (
    date_trunc('DAY', CREATE_DATE_TIME) >= date_trunc('DAY' ,to_date(DATE_FORMAT(CAST('{Prev_Run_Dt}' AS timestamp),'MM/dd/yyyy HH:mm:ss'), 'MM/dd/yyyy HH:mm:ss')) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', MOD_DATE_TIME) >= date_trunc('DAY', to_date(DATE_FORMAT(CAST('{Prev_Run_Dt}' AS timestamp),'MM/dd/yyyy HH:mm:ss'), 'MM/dd/yyyy HH:mm:ss')) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', CREATED_DTTM) >= date_trunc('DAY', to_date(DATE_FORMAT(CAST('{Prev_Run_Dt}' AS timestamp),'MM/dd/yyyy HH:mm:ss'), 'MM/dd/yyyy HH:mm:ss')) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', LAST_UPDATED_DTTM) >= date_trunc('DAY',to_date(DATE_FORMAT(CAST('{Prev_Run_Dt}' AS timestamp),'MM/dd/yyyy HH:mm:ss'), 'MM/dd/yyyy HH:mm:ss')) - INTERVAL '1' DAY
  )
  AND 1 = 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_E_DEPT_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  {DC_NBR} AS DC_NBR_EXP,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  current_timestamp()  AS LOAD_TSTMP_EXP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_E_DEPT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, WM_E_DEPT_PRE


spark.sql("""INSERT INTO
  WM_E_DEPT_PRE
SELECT
  DC_NBR_EXP AS DC_NBR,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP_EXP AS LOAD_TSTMP
FROM
  EXPTRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_WM_E_Dept_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_WM_E_Dept_PRE", mainWorkflowId, parentName)
