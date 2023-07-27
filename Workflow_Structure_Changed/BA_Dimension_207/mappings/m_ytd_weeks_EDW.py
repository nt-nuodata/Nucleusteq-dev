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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ytd_weeks_EDW")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ytd_weeks_EDW", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_YtdWeeks_0


query_0 = f"""SELECT
  WeekDt AS WeekDt,
  YtdWeekDt AS YtdWeekDt,
  LyrYtdWeekDt AS LyrYtdWeekDt
FROM
  YtdWeeks"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_YtdWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_YtdWeeks_1


query_1 = f"""SELECT
  ytd.WeekDt AS WeekDt,
  ytd.YtdWeekDt AS YtdWeekDt,
  ytd.LyrYtdWeekDt AS LyrYtdWeekDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_YtdWeeks_0 ytd,
  Weeks w
WHERE
  ytd.WeekDt = w.WeekDt
  AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_YtdWeeks_1")

# COMMAND ----------
# DBTITLE 1, YTD_WEEKS


spark.sql("""INSERT INTO
  YTD_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  YtdWeekDt AS YTD_WEEK_DT,
  LyrYtdWeekDt AS LYR_YTD_WEEK_DT
FROM
  SQ_Shortcut_to_YtdWeeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ytd_weeks_EDW")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ytd_weeks_EDW", mainWorkflowId, parentName)
