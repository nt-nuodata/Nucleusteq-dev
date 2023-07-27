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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wtd_days_EDW")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_wtd_days_EDW", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WtdDays_0


query_0 = f"""SELECT
  DayDt AS DayDt,
  WtdDayDt AS WtdDayDt,
  LyrWtdDayDt AS LyrWtdDayDt
FROM
  WtdDays"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_WtdDays_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WtdDays_1


query_1 = f"""SELECT
  wtd.DayDt AS DayDt,
  wtd.WtdDayDt AS WtdDayDt,
  wtd.LyrWtdDayDt AS LyrWtdDayDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WtdDays_0 wtd,
  Days d
WHERE
  wtd.DayDt = d.DayDt
  AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_WtdDays_1")

# COMMAND ----------
# DBTITLE 1, WTD_DAYS


spark.sql("""INSERT INTO
  WTD_DAYS
SELECT
  DayDt AS DAY_DT,
  WtdDayDt AS WTD_DAY_DT,
  LyrWtdDayDt AS LYR_WTD_DAY_DT
FROM
  SQ_Shortcut_to_WtdDays_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_wtd_days_EDW")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_wtd_days_EDW", mainWorkflowId, parentName)
