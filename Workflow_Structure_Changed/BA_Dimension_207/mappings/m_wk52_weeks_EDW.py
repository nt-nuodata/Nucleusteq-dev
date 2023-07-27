# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wk52_weeks_EDW")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_wk52_weeks_EDW", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Wk52Weeks_0


query_0 = f"""SELECT
  WeekDt AS WeekDt,
  Wk52WeekDt AS Wk52WeekDt
FROM
  Wk52Weeks"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Wk52Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk52Weeks_1


query_1 = f"""SELECT
  wk52.WeekDt AS WeekDt,
  wk52.Wk52WeekDt AS Wk52WeekDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Wk52Weeks_0 wk52,
  Weeks w
WHERE
  wk52.WeekDt = w.WeekDt
  AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk52Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK52_WEEKS


spark.sql("""INSERT INTO
  WK52_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  Wk52WeekDt AS WK52_WEEK_DT
FROM
  SQ_Shortcut_to_Wk52Weeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wk52_weeks_EDW")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_wk52_weeks_EDW", mainWorkflowId, parentName)
