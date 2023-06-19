# Databricks notebook source
# MAGIC %run /Shared/WMS-Petsmart-POC/Workflow1/WorkflowUtility

# COMMAND ----------

# %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
variablesTableName = dbutils.widgets.get("variablesTableName")
canTruncateTargetTables = dbutils.widgets.get("canTruncateTargetTables")

# COMMAND ----------

#Truncate Target Tables
targetTables = ["DELTA_TRAINING.WM_E_DEPT_PRE"]
truncateTargetTables(targetTables, canTruncateTargetTables)

# COMMAND ----------

#Pre presession variable updation
variables = {"Prev_Run_Dt" : "wk_Last_Run_Dt"}
updateVariable(variables, variablesTableName, mainWorkflowId, parentName, "m_WM_E_Dept_PRE")

# COMMAND ----------

# Initial_Load = ""
# Prev_Run_Dt = "2023-05-28"
# DC_NBR = 14
fetchAndCreateVariables(parentName, "m_WM_Ucl_User_PRE" ,variablesTableName, mainWorkflowId)

# COMMAND ----------

# DBTITLE 1, Shortcut_to_E_DEPT_0

df_0 = spark.sql("""SELECT
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
  E_DEPT""")

df_0.createOrReplaceTempView("Shortcut_to_E_DEPT_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_E_DEPT_1

query = f"""SELECT
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

df_1 = spark.sql(query)

df_1.createOrReplaceTempView("SQ_Shortcut_to_E_DEPT_1")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_2

df_2 = spark.sql(f"""SELECT
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
  current_timestamp() AS LOAD_TSTMP_EXP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_E_DEPT_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------

df_2.display()

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

# MAGIC %sql
# MAGIC SELECT * FROM DELTA_TRAINING.WM_E_DEPT_PRE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM DELTA_TRAINING.WM_E_DEPT_PRE;

# COMMAND ----------

persistVariables(variablesTableName, "m_WM_E_Dept_PRE", mainWorkflowId, parentName)